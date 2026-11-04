# Indexing spec: USDC (Base, Ethereum, Arbitrum)

Written before any pipeline code, from reads against the chain. Re-run `chainproof inspect --chain <chain>` to refresh it.

## Contracts (checked 2026-11-04)

| Chain | Proxy (the address that emits events) | EIP-1967 impl slot | Legacy zeppelinos impl slot | decimals |
|---|---|---|---|---|
| Base | `0x833589fcd6edb6e08f4c7c32d4f71b54bda02913` | empty | `0x2ce6311ddae708829bc0784c967b7d77d19fd779` | 6 |
| Ethereum | `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` | empty | `0x43506849d7c04f9138d1a2050bbf3a0c054402dd` | 6 |
| Arbitrum | `0xaf88d065e77c8cc2239327c5edb3a432268e5831` | empty | `0x86e721b43d4ecfa71119dd38c0f938a75fdb57b3` | 6 |

**Finding:** USDC stores its implementation in the pre-EIP-1967 zeppelinos slot. A tool that reads only the standard EIP-1967 slot reports "not a proxy". Events are emitted from the proxy address, so that is the address we filter on.

## Events to index

- `Transfer(address indexed from, address indexed to, uint256 value)`, topic0 `0xddf252ad…b3ef`, exactly 3 topics.
  - Mints show up as `Transfer` from the zero address and burns as `Transfer` to the zero address. So `Transfer` alone covers all supply changes, and `Mint`/`Burn` would be metadata, not extra volume.
- Filter on `(address, topic0)` together. Any contract can emit a log with the same topic0, NFTs included.

## Traces needed?

No. USDC moves no native ETH, and every balance change emits a `Transfer`.

## Invariants (the reconciliation gate)

For a published range `[start, end]`, all read at pinned blocks `start-1` and `end`:

1. **completeness**: every `(block, log_index)` and every value equals an independent re-read from a second RPC, using a different window size.
2. **supply**: `totalSupply(end) − totalSupply(start−1) = Σ mints − Σ burns`.
3. **balances**: for the 10 busiest and 10 random holders, `balanceOf(end) − balanceOf(start−1) = inflow − outflow`.
4. **canonical**: the stored block hashes of sampled blocks equal the chain's hashes.
5. **decimals**: on-chain `decimals()` equals the config value.
6. **canary**: one raw unit added to one row must make (1) and (3) fail.

Tolerance: zero.

## Edge cases

- Blacklisting freezes balances but emits no `Transfer`, and doesn't break invariants (2) and (3).
- `pause()` stops transfers, so a run may see empty ranges. That's valid.
- A token with fee-on-transfer or rebasing would break invariant (3). USDC has neither, and the gate would catch it if that changed.
- Arbitrum's *finalized* block trails the head by thousands of blocks, so free RPCs treat a read at finality as an archive request. The config uses `arb1.arbitrum.io` for state reads.

## Validation set

Golden transaction (in `tests/test_decode.py`): Base block 52,955,608, tx `0xb4cff99a…f87ef9`, log 32 → 5.000000 USDC from `0x90f2…eb56` to `0x7707…5dce`.
