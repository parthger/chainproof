"""Extract Transfer logs for a block range with an adaptive window.

The window starts at `chunk` blocks, halves when the provider says the request is too big,
and grows back after a run of successes. At the floor it gives up instead of looping forever.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .decode import TRANSFER_TOPIC, DecodeError, Transfer, decode_transfer
from .rpc import ChainReader, RangeTooLarge

log = logging.getLogger(__name__)


class ExtractError(RuntimeError):
    pass


@dataclass
class ExtractStats:
    requests: int = 0
    splits: int = 0
    skipped_non_erc20: int = 0
    windows: list[tuple[int, int, int]] = field(default_factory=list)  # (from, to, logs)


def fetch_logs(
    reader: ChainReader,
    address: str,
    start: int,
    end: int,
    *,
    chunk: int = 200,
    floor: int = 1,
    max_chunk: int = 2_000,
    max_failures_at_floor: int = 3,
    stats: ExtractStats | None = None,
) -> list[dict]:
    if end < start:
        return []
    stats = stats if stats is not None else ExtractStats()
    out: list[dict] = []
    cur, size, ok_streak, floor_failures = start, max(chunk, floor), 0, 0
    while cur <= end:
        to = min(cur + size - 1, end)
        try:
            stats.requests += 1
            logs = reader.get_logs(address, TRANSFER_TOPIC, cur, to)
        except RangeTooLarge as e:
            if size <= floor:
                floor_failures += 1
                if floor_failures >= max_failures_at_floor:
                    raise ExtractError(f"range {cur}..{to} still too large at the floor ({floor}): {e}") from e
                continue
            size = max(floor, size // 2)
            stats.splits += 1
            ok_streak = 0
            log.debug("range too large at %d..%d, shrinking to %d", cur, to, size)
            continue
        stats.windows.append((cur, to, len(logs)))
        for lg in logs:
            bn = int(lg["blockNumber"], 16)
            if not cur <= bn <= to:
                raise ExtractError(f"provider returned block {bn} outside requested {cur}..{to}")
        out.extend(logs)
        cur = to + 1
        floor_failures = 0
        ok_streak += 1
        if ok_streak >= 4 and size < max_chunk:
            size = min(max_chunk, size * 2)
            ok_streak = 0
    return out


def extract_transfers(
    reader: ChainReader,
    chain_id: int,
    token: str,
    start: int,
    end: int,
    *,
    chunk: int = 200,
    stats: ExtractStats | None = None,
) -> list[Transfer]:
    stats = stats if stats is not None else ExtractStats()
    raw = fetch_logs(reader, token, start, end, chunk=chunk, stats=stats)
    transfers: list[Transfer] = []
    for lg in raw:
        if lg["address"].lower() != token.lower():
            raise ExtractError(f"log from {lg['address']} in a query filtered on {token}")
        t = decode_transfer(lg, chain_id)
        if t is None:
            stats.skipped_non_erc20 += 1
            continue
        transfers.append(t)

    missing = sorted({t.block_number for t in transfers if t.block_time is None})
    if missing:
        ts = {}
        for i in range(0, len(missing), 200):
            ts.update(reader.block_timestamps(missing[i : i + 200]))
        transfers = [
            t if t.block_time is not None else Transfer(**{**t.__dict__, "block_time": ts[t.block_number]}) for t in transfers
        ]

    seen: set[tuple[int, int, int]] = set()
    for t in transfers:
        if t.key in seen:
            raise ExtractError(f"duplicate log position {t.key}: provider returned the same log twice")
        seen.add(t.key)
    transfers.sort(key=lambda t: (t.block_number, t.log_index))
    return transfers


__all__ = ["DecodeError", "ExtractError", "ExtractStats", "extract_transfers", "fetch_logs"]
