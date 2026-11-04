"""A small JSON-RPC client that classifies its failures.

Public RPCs fail in two different ways and each needs a different response:

* the request is too big (block range or result count over the provider's cap)
  -> raise RangeTooLarge so the caller can halve the range;
* the endpoint is unhealthy (timeouts, 429, 5xx, "overloaded")
  -> back off and retry here, then give up loudly.
"""

from __future__ import annotations

import random
import time
from typing import Any, Protocol

import requests

USER_AGENT = "chainproof/0.1 (+https://github.com/parthger/chainproof)"

_RANGE_HINTS = (
    "range",
    "too many",
    "limit exceeded",
    "exceed",
    "response size",
    "query returned more than",
    "max results",
    "block range",
    "10000 results",
)


class RpcError(RuntimeError):
    """The node answered with an error we do not know how to recover from."""


class RangeTooLarge(RpcError):
    """The request asked for more than the provider allows. Retry with a smaller range."""


class BatchTooLarge(RpcError):
    """The provider caps JSON-RPC batch size (drpc's free plan allows 3). Retry with smaller batches."""


class EndpointUnhealthy(RpcError):
    """Retries were exhausted on transient failures."""


class ChainReader(Protocol):
    """Everything the pipeline needs from a chain. RpcClient and the test FakeChain both implement it."""

    def chain_id(self) -> int: ...
    def finalized_block(self) -> tuple[int, str]: ...
    def block_hash(self, number: int) -> str: ...
    def get_logs(self, address: str, topic0: str, from_block: int, to_block: int) -> list[dict]: ...
    def call_many(self, calls: list[tuple[str, str]], block: int) -> list[str]: ...
    def block_timestamps(self, numbers: list[int]) -> dict[int, int]: ...


def _looks_like_range_error(message: str) -> bool:
    m = message.lower()
    return any(h in m for h in _RANGE_HINTS)


class RpcClient:
    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 6,
        backoff_base: float = 1.0,
        session: requests.Session | None = None,
    ) -> None:
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Content-Type": "application/json"})
        self.requests_made = 0
        self.batch_size = 20
        self._id = 0

    # ---- transport -------------------------------------------------------

    def _post(self, payload: Any) -> Any:
        last: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                self.requests_made += 1
                r = self.session.post(self.url, json=payload, timeout=self.timeout)
                if r.status_code == 429 or r.status_code >= 500:
                    body = r.text[:300]
                    if "batch" in body.lower() and ("not allowed" in body.lower() or "too large" in body.lower()):
                        raise BatchTooLarge(body)
                    if _looks_like_range_error(body):
                        raise RangeTooLarge(body)
                    raise requests.HTTPError(f"HTTP {r.status_code}: {body}")
                if r.status_code == 413:
                    raise RangeTooLarge("HTTP 413 payload too large")
                if 400 <= r.status_code < 500:
                    # Some providers send JSON-RPC errors with a 4xx status (drpc answers a
                    # 200-block getLogs with HTTP 400 "ranges over 10000 blocks are not supported").
                    # Retrying will never help: hand the error body to _unwrap for classification.
                    try:
                        body = r.json()
                    except ValueError:
                        raise RpcError(f"HTTP {r.status_code}: {r.text[:300]}") from None
                    if isinstance(body, dict) and body.get("error"):
                        return body
                    raise RpcError(f"HTTP {r.status_code}: {r.text[:300]}")
                r.raise_for_status()
                return r.json()
            except (RangeTooLarge, BatchTooLarge):
                raise
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError, ValueError) as e:
                last = e
                sleep = self.backoff_base * (2**attempt) * (1 + random.random() * 0.25)
                time.sleep(sleep)
        raise EndpointUnhealthy(f"{self.url}: gave up after {self.max_retries} attempts: {last}")

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    @staticmethod
    def _unwrap(resp: dict) -> Any:
        if "error" in resp and resp["error"] is not None:
            err = resp["error"]
            msg = f"{err.get('code')}: {err.get('message')} {err.get('data') or ''}".strip()
            if _looks_like_range_error(msg):
                raise RangeTooLarge(msg)
            raise RpcError(msg)
        return resp["result"]

    def request(self, method: str, params: list) -> Any:
        payload = {"jsonrpc": "2.0", "id": self._next_id(), "method": method, "params": params}
        return self._unwrap(self._post(payload))

    def batch(self, calls: list[tuple[str, list]]) -> list[Any]:
        """JSON-RPC batch. Results come back in request order; any error fails the batch.

        Batch size shrinks automatically if the provider rejects large batches.
        """
        out: list[Any] = []
        i = 0
        while i < len(calls):
            part = calls[i : i + self.batch_size]
            try:
                out.extend(self._batch_once(part))
            except BatchTooLarge:
                if self.batch_size == 1:
                    raise
                self.batch_size = max(1, self.batch_size // 2 if self.batch_size > 3 else 1)
                continue
            i += len(part)
        return out

    def _batch_once(self, part: list[tuple[str, list]]) -> list[Any]:
        out: list[Any] = []
        ids = [self._next_id() for _ in part]
        payload = [{"jsonrpc": "2.0", "id": rid, "method": m, "params": p} for rid, (m, p) in zip(ids, part, strict=True)]
        resp = self._post(payload)
        if not isinstance(resp, list):
            # some providers answer a batch with a single error object
            self._unwrap(resp)
            raise RpcError(f"expected a batch response, got {str(resp)[:200]}")
        by_id = {r.get("id"): r for r in resp}
        for rid in ids:
            if rid not in by_id:
                raise RpcError(f"batch response missing id {rid}")
            err = by_id[rid].get("error") or {}
            if "batch" in str(err.get("message", "")).lower():
                raise BatchTooLarge(str(err))
            out.append(self._unwrap(by_id[rid]))
        return out

    # ---- ChainReader -----------------------------------------------------

    def chain_id(self) -> int:
        return int(self.request("eth_chainId", []), 16)

    def finalized_block(self) -> tuple[int, str]:
        b = self.request("eth_getBlockByNumber", ["finalized", False])
        if b is None:
            raise RpcError("endpoint does not serve the 'finalized' tag")
        return int(b["number"], 16), b["hash"].lower()

    def block_hash(self, number: int) -> str:
        b = self.request("eth_getBlockByNumber", [hex(number), False])
        if b is None:
            raise RpcError(f"block {number} not found")
        return b["hash"].lower()

    def get_logs(self, address: str, topic0: str, from_block: int, to_block: int) -> list[dict]:
        return self.request(
            "eth_getLogs",
            [{"address": address, "topics": [topic0], "fromBlock": hex(from_block), "toBlock": hex(to_block)}],
        )

    def call_many(self, calls: list[tuple[str, str]], block: int) -> list[str]:
        tag = hex(block)
        return self.batch([("eth_call", [{"to": to, "data": data}, tag]) for to, data in calls])

    def block_timestamps(self, numbers: list[int]) -> dict[int, int]:
        blocks = self.batch([("eth_getBlockByNumber", [hex(n), False]) for n in numbers])
        return {int(b["number"], 16): int(b["timestamp"], 16) for b in blocks}
