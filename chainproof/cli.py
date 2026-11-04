"""Command line: `chainproof run | status | inspect | rewind | report`."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime

from . import __version__
from .config import get_chain, get_token
from .decode import SEL_DECIMALS, SEL_SYMBOL, SEL_TOTAL_SUPPLY, decode_string, decode_uint, format_units
from .pipeline import ReorgDetected, find_rewind_point, run
from .reconcile import ReconciliationError
from .rpc import RpcClient
from .store import Store

SLOTS = {
    "EIP-1967 implementation": "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc",
    "EIP-1967 admin": "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103",
    "zeppelinos implementation (legacy)": "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3",
    "zeppelinos admin (legacy)": "0x10d6a54a4754c8869d6886b5f5d7fbfa5b4522237ea5c60d11bc4e7a1ff9390b",
}


def _clients(args) -> tuple[RpcClient, RpcClient]:
    chain = get_chain(args.chain)
    primary = args.rpc or chain.rpc_urls[0]
    verifier = args.verify_rpc or (chain.rpc_urls[1] if len(chain.rpc_urls) > 1 else primary)
    return RpcClient(primary), RpcClient(verifier)


def _print_checks(checks: list[dict]) -> None:
    for c in checks:
        mark = "PASS" if c["passed"] else "FAIL"
        print(f"  {mark}  {c['name']:<12} {c['summary']}")


def cmd_run(args) -> int:
    chain, token = get_chain(args.chain), get_token(args.chain, args.token)
    reader, verifier = _clients(args)
    store = Store(args.db, decimals=token.decimals)
    print(f"chainproof {__version__}: {token.symbol} on {chain.name}")
    print(f"  primary  {reader.url}\n  verifier {verifier.url}")
    try:
        rep = run(
            reader,
            verifier,
            store,
            chain,
            token,
            lookback_blocks=args.blocks,
            from_block=args.from_block,
            max_blocks=args.max_blocks,
            chunk=args.chunk,
            balance_k=args.holders,
            canary=not args.no_canary,
            auto_rewind=args.auto_rewind,
        )
    except ReconciliationError as e:
        print(f"\n{e}", file=sys.stderr)
        _print_checks([c.__dict__ for c in e.results])
        return 2
    except ReorgDetected as e:
        print(f"\nREORG TRIPWIRE: {e}", file=sys.stderr)
        return 3
    if rep.status == "noop":
        print(f"  up to date: watermark is at the finalized head ({rep.finalized})")
        return 0
    if rep.rewound_to is not None:
        print(f"  rewound to block {rep.rewound_to} after a reorg")
    print(f"  blocks   {rep.start}..{rep.end} ({rep.end - rep.start + 1} blocks, finalized head {rep.finalized})")
    print(f"  rows     {rep.rows} transfers published   run {rep.run_id}")
    print(f"  rpc      {rep.extract['requests']} getLogs windows, {rep.extract['splits']} splits, {rep.seconds}s total")
    _print_checks(rep.checks)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(rep.__dict__, f, indent=2, default=str)
    return 0


def cmd_status(args) -> int:
    chain, token = get_chain(args.chain), get_token(args.chain, args.token)
    store = Store(args.db, decimals=token.decimals)
    wm = store.watermark(chain.chain_id, token.address)
    print(f"{token.symbol} on {chain.name}")
    print(f"  watermark  {wm[0] if wm else '-'}  {wm[1] if wm else ''}")
    print(f"  published  {store.published_count(chain.chain_id, token.address)} transfers")
    print("  recent runs:")
    for run_id, _cid, _tok, s, e, status, n, err, _checks in store.runs(10):
        line = f"    {run_id}  {s}..{e}  {status:<9} rows={n}"
        if err:
            line += f"  {err.splitlines()[0][:90]}"
        print(line)
    return 0


def cmd_report(args) -> int:
    chain, token = get_chain(args.chain), get_token(args.chain, args.token)
    store = Store(args.db, decimals=token.decimals)
    d = token.decimals
    print(f"| day (UTC) | transfers | volume ({token.symbol}) | minted | burned | senders | blocks | complete day? |")
    print("|---|---:|---:|---:|---:|---:|---|---|")
    for day, n, vol, mint, burn, snd, _rcv, b0, b1, t0, t1 in store.daily(chain.chain_id, token.address):
        full = t0.hour == 0 and t0.minute == 0 and t1.hour == 23 and t1.minute == 59
        print(
            f"| {day} | {n:,} | {format_units(int(vol), d)} | {format_units(int(mint), d)} | "
            f"{format_units(int(burn), d)} | {snd:,} | {b0}..{b1} | {'yes' if full else 'partial'} |"
        )
    return 0


def cmd_rewind(args) -> int:
    chain, token = get_chain(args.chain), get_token(args.chain, args.token)
    reader, _ = _clients(args)
    store = Store(args.db, decimals=token.decimals)
    if args.to is None:
        to_block, _ = find_rewind_point(store, reader, chain.chain_id, token.address)
    else:
        to_block = args.to
    n = store.rewind(chain.chain_id, token.address, to_block, reader.block_hash(to_block))
    print(f"rewound {token.symbol} on {chain.name} to block {to_block}; deleted {n} rows")
    return 0


def cmd_inspect(args) -> int:
    """Chapter-3-style research pass: is this the contract we think it is?"""
    chain, token = get_chain(args.chain), get_token(args.chain, args.token)
    rpc, _ = _clients(args)
    cid = rpc.chain_id()
    n, h = rpc.finalized_block()
    tag = hex(n)
    a = token.address
    symbol, decimals, supply = rpc.call_many([(a, SEL_SYMBOL), (a, SEL_DECIMALS), (a, SEL_TOTAL_SUPPLY)], n)
    print(f"{a} on {chain.name} (chain id {cid}) at finalized block {n} {h}")
    print(f"  symbol()       {decode_string(symbol)}")
    print(f"  decimals()     {decode_uint(decimals)}   config: {token.decimals}")
    print(f"  totalSupply()  {format_units(decode_uint(supply), token.decimals)}")
    for label, slot in SLOTS.items():
        v = rpc.request("eth_getStorageAt", [a, slot, tag])
        addr = "0x" + v[-40:]
        print(f"  {label:<36} {addr if int(v, 16) else '(empty)'}")
    print(f"  checked at {datetime.now(UTC):%Y-%m-%d %H:%M} UTC")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="chainproof", description=__doc__)
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp, rpc: bool = True):
        sp.add_argument("--chain", default="base")
        sp.add_argument("--token", default="USDC")
        sp.add_argument("--db", default="chainproof.duckdb")
        if rpc:
            sp.add_argument("--rpc", help="primary RPC URL (default: from config)")
            sp.add_argument("--verify-rpc", help="independent RPC used to re-read the range")

    r = sub.add_parser("run", help="index the next finalized range and publish it only if it reconciles")
    common(r)
    r.add_argument("--blocks", type=int, default=1_000, help="first run only: how many finalized blocks back to start")
    r.add_argument("--from-block", type=int, help="first run only: explicit start block")
    r.add_argument("--max-blocks", type=int, default=5_000, help="cap on blocks per run")
    r.add_argument("--chunk", type=int, default=200, help="initial eth_getLogs window")
    r.add_argument("--holders", type=int, default=10, help="busy holders to check with balanceOf (plus as many random)")
    r.add_argument("--no-canary", action="store_true", help="skip the planted-fault self-test")
    r.add_argument("--auto-rewind", action="store_true", help="on a reorg tripwire, rewind and continue")
    r.add_argument("--json", help="write the run report to this file")
    r.set_defaults(func=cmd_run)

    s = sub.add_parser("status", help="watermark and recent runs")
    common(s, rpc=False)
    s.set_defaults(func=cmd_status)

    rp = sub.add_parser("report", help="daily mart as a markdown table")
    common(rp, rpc=False)
    rp.set_defaults(func=cmd_report)

    rw = sub.add_parser("rewind", help="delete rows above a block and move the watermark back")
    common(rw)
    rw.add_argument("--to", type=int, help="target block (default: highest stored block that is still canonical)")
    rw.set_defaults(func=cmd_rewind)

    i = sub.add_parser("inspect", help="research the token contract: decimals, supply, proxy slots")
    common(i)
    i.set_defaults(func=cmd_inspect)

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    return args.func(args)
