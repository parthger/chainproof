"""The reconciliation gate: prove the staged rows are right before anyone sees them.

Each invariant catches a different class of bug, which is why there are several:

| check          | compares                                                    | catches                                   |
|----------------|-------------------------------------------------------------|-------------------------------------------|
| completeness   | every (block, log_index) and value vs an independent fetch  | dropped, extra, or altered rows           |
| supply         | totalSupply(end) - totalSupply(start-1) vs mints - burns    | missed/double-counted mints and burns     |
| balances       | balanceOf delta per busy holder vs inflow - outflow         | wrong amounts, swapped from/to, decimals  |
| canonical      | stored block hashes vs the chain's hashes                   | rows from a reorged (orphaned) block      |
| decimals       | decimals() on-chain vs config                               | a wrong token or a misconfigured unit     |

A holder-to-holder transfer with a wrong amount does NOT move totalSupply, so the supply check
alone is blind to it. The balances check is not. That gap is why one invariant is never enough.

Tolerance is zero everywhere. Amounts are exact integers; any difference fails the run.

Evidence (chain reads) is gathered once; `evaluate` is pure. That split lets us run a
*canary*: plant a one-unit fault in a copy of the rows and require the gate to turn red.
A guard that cannot fail is not a guard.
"""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field

from .config import Token
from .decode import (
    SEL_DECIMALS,
    SEL_TOTAL_SUPPLY,
    ZERO_ADDRESS,
    Transfer,
    decode_uint,
    encode_balance_of,
)
from .extract import extract_transfers
from .rpc import ChainReader


class ReconciliationError(RuntimeError):
    def __init__(self, failed: list[CheckResult], results: list[CheckResult] | None = None):
        self.failed = failed
        self.results = results or failed
        lines = [f"  - {c.name}: {c.summary}" for c in failed]
        super().__init__("reconciliation failed; nothing was published\n" + "\n".join(lines))


@dataclass
class CheckResult:
    name: str
    passed: bool
    summary: str
    detail: dict = field(default_factory=dict)


@dataclass
class Evidence:
    start: int
    end: int
    decimals_expected: int
    decimals_onchain: int
    supply_before: int
    supply_after: int
    balance_addresses: list[str]
    balances_before: dict[str, int]
    balances_after: dict[str, int]
    canonical_hashes: dict[int, str]
    verifier_rows: dict[tuple[int, int, int], tuple[str, str, str, int]]
    verifier_url: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["verifier_rows"] = len(self.verifier_rows)
        return d


def _row(t: Transfer) -> tuple[str, str, str, int]:
    return (t.tx_hash, t.from_address, t.to_address, t.amount_raw)


def pick_balance_addresses(transfers: list[Transfer], k: int, seed: int = 0) -> list[str]:
    """The k busiest holders plus k random ones: busy ones exercise many rows, random ones avoid bias."""
    counts: Counter[str] = Counter()
    for t in transfers:
        for a in (t.from_address, t.to_address):
            if a != ZERO_ADDRESS:
                counts[a] += 1
    busiest = [a for a, _ in counts.most_common(k)]
    rest = sorted(set(counts) - set(busiest))
    rng = random.Random(seed)
    sample = rng.sample(rest, min(k, len(rest)))
    return busiest + sample


def gather_evidence(
    reader: ChainReader,
    verifier: ChainReader,
    token: Token,
    chain_id: int,
    transfers: list[Transfer],
    start: int,
    end: int,
    *,
    balance_k: int = 10,
    verifier_chunk: int = 75,
    verifier_url: str = "",
) -> Evidence:
    addr = token.address
    before = start - 1

    (dec_hex,) = reader.call_many([(addr, SEL_DECIMALS)], end)
    s_before, s_after = (
        decode_uint(reader.call_many([(addr, SEL_TOTAL_SUPPLY)], before)[0]),
        decode_uint(reader.call_many([(addr, SEL_TOTAL_SUPPLY)], end)[0]),
    )

    holders = pick_balance_addresses(transfers, balance_k, seed=start)
    calls = [(addr, encode_balance_of(h)) for h in holders]
    b_before = dict(zip(holders, map(decode_uint, reader.call_many(calls, before)), strict=True)) if holders else {}
    b_after = dict(zip(holders, map(decode_uint, reader.call_many(calls, end)), strict=True)) if holders else {}

    blocks = sorted({t.block_number for t in transfers})
    rng = random.Random(end)
    sample = set(blocks[:1] + blocks[-1:] + rng.sample(blocks, min(8, len(blocks)))) if blocks else set()
    canonical = {n: verifier.block_hash(n) for n in sorted(sample)}

    # Independent re-read: a different endpoint and a different window size, so a bug in
    # paging or one provider's indexing cannot hide in both copies.
    vrows = {t.key: _row(t) for t in extract_transfers(verifier, chain_id, addr, start, end, chunk=verifier_chunk)}

    return Evidence(
        start=start,
        end=end,
        decimals_expected=token.decimals,
        decimals_onchain=decode_uint(dec_hex),
        supply_before=s_before,
        supply_after=s_after,
        balance_addresses=holders,
        balances_before=b_before,
        balances_after=b_after,
        canonical_hashes=canonical,
        verifier_rows=vrows,
        verifier_url=verifier_url,
    )


def evaluate(transfers: list[Transfer], ev: Evidence) -> list[CheckResult]:
    results: list[CheckResult] = []

    # 1. completeness: keys and exact values, not just counts
    mine = {t.key: _row(t) for t in transfers}
    missing = sorted(set(ev.verifier_rows) - set(mine))
    extra = sorted(set(mine) - set(ev.verifier_rows))
    mismatched = sorted(k for k in set(mine) & set(ev.verifier_rows) if mine[k] != ev.verifier_rows[k])
    ok = not (missing or extra or mismatched)
    results.append(
        CheckResult(
            "completeness",
            ok,
            f"{len(mine)} rows vs {len(ev.verifier_rows)} independent; "
            f"missing={len(missing)} extra={len(extra)} value_mismatches={len(mismatched)}",
            {"missing": missing[:5], "extra": extra[:5], "mismatched": mismatched[:5]},
        )
    )

    # 2. supply: mints and burns explain the change in totalSupply exactly
    mints = sum(t.amount_raw for t in transfers if t.is_mint)
    burns = sum(t.amount_raw for t in transfers if t.is_burn)
    expected = ev.supply_after - ev.supply_before
    got = mints - burns
    diff = got - expected
    direction = ""
    if diff > 0:
        direction = "events exceed state: double-counted or phantom mints"
    elif diff < 0:
        direction = "state exceeds events: missed mints, or supply changed without a Transfer"
    results.append(
        CheckResult(
            "supply",
            diff == 0,
            f"Δsupply={expected} mints-burns={got} diff={diff}" + (f" ({direction})" if direction else ""),
            {"mints": mints, "burns": burns, "supply_before": ev.supply_before, "supply_after": ev.supply_after},
        )
    )

    # 3. balances: every sampled holder's delta equals its net flow
    flow: dict[str, int] = defaultdict(int)
    for t in transfers:
        flow[t.to_address] += t.amount_raw
        flow[t.from_address] -= t.amount_raw
    bad = []
    for a in ev.balance_addresses:
        want = ev.balances_after[a] - ev.balances_before[a]
        if flow.get(a, 0) != want:
            bad.append({"address": a, "net_flow": flow.get(a, 0), "balance_delta": want, "diff": flow.get(a, 0) - want})
    results.append(
        CheckResult(
            "balances",
            not bad,
            f"{len(ev.balance_addresses) - len(bad)}/{len(ev.balance_addresses)} holders match balanceOf exactly",
            {"mismatches": bad[:5]},
        )
    )

    # 4. canonical: rows come from blocks that are on the chain, and one hash per block
    hashes_per_block: dict[int, set[str]] = defaultdict(set)
    for t in transfers:
        hashes_per_block[t.block_number].add(t.block_hash)
    multi = sorted(b for b, hs in hashes_per_block.items() if len(hs) > 1)
    orphaned = sorted(b for b, h in ev.canonical_hashes.items() if b in hashes_per_block and hashes_per_block[b] != {h})
    results.append(
        CheckResult(
            "canonical",
            not multi and not orphaned,
            f"{len(ev.canonical_hashes)} sampled blocks; orphaned={len(orphaned)} blocks_with_two_hashes={len(multi)}",
            {"orphaned": orphaned[:5], "multi_hash": multi[:5]},
        )
    )

    # 5. decimals
    results.append(
        CheckResult(
            "decimals",
            ev.decimals_onchain == ev.decimals_expected,
            f"on-chain={ev.decimals_onchain} config={ev.decimals_expected}",
        )
    )
    return results


def plant_fault(transfers: list[Transfer], ev: Evidence) -> tuple[list[Transfer], Transfer]:
    """Add one raw unit to the largest transfer touching a sampled holder (or any transfer)."""
    watched = set(ev.balance_addresses)
    candidates = [t for t in transfers if t.from_address in watched or t.to_address in watched] or transfers
    victim = max(candidates, key=lambda t: t.amount_raw)
    mutated = [t.with_amount(t.amount_raw + 1) if t.key == victim.key else t for t in transfers]
    return mutated, victim


def run_canary(transfers: list[Transfer], ev: Evidence) -> CheckResult:
    if not transfers:
        return CheckResult("canary", True, "no rows in range; nothing to mutate")
    mutated, victim = plant_fault(transfers, ev)
    caught = [r.name for r in evaluate(mutated, ev) if not r.passed]
    must = {"completeness", "balances"}
    ok = must.issubset(caught)
    return CheckResult(
        "canary",
        ok,
        f"planted +1 raw unit at block {victim.block_number} log {victim.log_index}; caught by {', '.join(caught) or 'NOTHING'}",
        {"caught_by": caught, "expected": sorted(must)},
    )


def gate(transfers: list[Transfer], ev: Evidence, *, canary: bool = True) -> list[CheckResult]:
    results = evaluate(transfers, ev)
    if canary:
        results.append(run_canary(transfers, ev))
    failed = [r for r in results if not r.passed]
    if failed:
        raise ReconciliationError(failed, results)
    return results
