"""The RPC client must classify failures the way real providers actually send them."""

import json

import pytest

from chainproof.rpc import EndpointUnhealthy, RangeTooLarge, RpcClient


class Resp:
    def __init__(self, status: int, body):
        self.status_code = status
        self._body = body
        self.text = body if isinstance(body, str) else json.dumps(body)

    def json(self):
        if isinstance(self._body, str):
            raise ValueError("not json")
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(str(self.status_code))


class Session:
    def __init__(self, responder):
        self.responder = responder
        self.headers = {}
        self.posts = []

    def post(self, url, json=None, timeout=None):
        self.posts.append(json)
        return self.responder(json)


def client(responder) -> RpcClient:
    return RpcClient("http://x", session=Session(responder), backoff_base=0.0)


def test_drpc_style_http_400_range_error_is_range_too_large_and_not_retried():
    body = {
        "id": 1,
        "jsonrpc": "2.0",
        "error": {"message": "ranges over 10000 blocks are not supported on free plan", "code": 35},
    }
    c = client(lambda _: Resp(400, body))
    with pytest.raises(RangeTooLarge):
        c.get_logs("0x" + "1" * 40, "0x" + "2" * 64, 1, 200)
    assert c.requests_made == 1


def test_jsonrpc_range_error_in_200_response():
    body = {"id": 1, "jsonrpc": "2.0", "error": {"code": -32005, "message": "query returned more than 10000 results"}}
    with pytest.raises(RangeTooLarge):
        client(lambda _: Resp(200, body)).get_logs("0x" + "1" * 40, "0x" + "2" * 64, 1, 2)


def test_429_is_retried_then_gives_up():
    c = client(lambda _: Resp(429, {"error": {"code": -32011, "message": "request limit reached"}}))
    with pytest.raises(EndpointUnhealthy):
        c.chain_id()
    assert c.requests_made == c.max_retries


def test_transient_500_then_success():
    calls = {"n": 0}

    def responder(payload):
        calls["n"] += 1
        if calls["n"] < 3:
            return Resp(502, "bad gateway")
        return Resp(200, {"jsonrpc": "2.0", "id": payload["id"], "result": "0x2105"})

    assert client(responder).chain_id() == 8453


def test_batch_shrinks_when_provider_caps_batch_size():
    def responder(payload):
        if isinstance(payload, list) and len(payload) > 3:
            return Resp(
                500,
                [
                    {
                        "id": p["id"],
                        "jsonrpc": "2.0",
                        "error": {"code": 31, "message": "Batch of more than 3 requests are not allowed on free plan"},
                    }
                    for p in payload
                ],
            )
        return Resp(200, [{"jsonrpc": "2.0", "id": p["id"], "result": hex(i)} for i, p in enumerate(payload)])

    c = client(responder)
    out = c.call_many([("0x" + "1" * 40, "0x18160ddd")] * 10, 100)
    assert len(out) == 10
    assert c.batch_size <= 3


def test_batch_results_follow_request_order_even_if_response_is_shuffled():
    def responder(payload):
        return Resp(200, [{"jsonrpc": "2.0", "id": p["id"], "result": hex(p["id"])} for p in reversed(payload)])

    c = client(responder)
    out = c.batch([("eth_chainId", [])] * 5)
    assert [int(x, 16) for x in out] == sorted(int(x, 16) for x in out)
