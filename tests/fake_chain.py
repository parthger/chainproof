"""An in-memory EVM-ish chain for tests: one ERC-20, real log shapes, state at any block, reorgs.

It implements the same ChainReader protocol as RpcClient, so the pipeline cannot tell them apart.
"""

from __future__ import annotations

import hashlib
import random
from collections import defaultdict
from dataclasses import dataclass, field

from chainproof.decode import SEL_BALANCE_OF, SEL_DECIMALS, SEL_TOTAL_SUPPLY, TRANSFER_TOPIC, ZERO_ADDRESS
from chainproof.rpc import RangeTooLarge

TOKEN = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
GENESIS_TS = 1_759_362_900  # 2026-10-01 23:55:00 UTC: the range crosses midnight


def _h(*parts) -> str:
    return "0x" + hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()


def _addr(i: int) -> str:
    return "0x" + f"{i:040x}"


def _topic(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:]


@dataclass
class Block:
    number: int
    hash: str
    timestamp: int
    logs: list[dict] = field(default_factory=list)


class FakeChain:
    def __init__(
        self,
        *,
        chain_id: int = 8453,
        first_block: int = 1_000,
        n_blocks: int = 300,
        finality_lag: int = 10,
        holders: int = 25,
        seed: int = 7,
        decimals: int = 6,
        range_limit: int | None = None,
    ) -> None:
        self._chain_id = chain_id
        self.first = first_block
        self.finality_lag = finality_lag
        self.holders = [_addr(i + 1) for i in range(holders)]
        self.decimals = decimals
        self.range_limit = range_limit
        self.rng = random.Random(seed)
        self.salt = 0
        self.blocks: dict[int, Block] = {}
        self.silent_supply: dict[int, int] = defaultdict(int)  # supply changes with no Transfer log
        self.get_logs_calls = 0
        # genesis allocation is a mint in the first block, so state is fully explained by logs
        self._make_block(first_block, genesis=True)
        for n in range(first_block + 1, first_block + n_blocks):
            self._make_block(n)

    # ---- generation ------------------------------------------------------

    def _balances_before(self, number: int) -> dict[str, int]:
        bal: dict[str, int] = defaultdict(int)
        for n in range(self.first, number):
            for lg in self.blocks[n].logs:
                if lg["address"] != TOKEN:
                    continue
                amt = int(lg["data"], 16)
                frm, to = "0x" + lg["topics"][1][-40:], "0x" + lg["topics"][2][-40:]
                if frm != ZERO_ADDRESS:
                    bal[frm] -= amt
                bal[to] += amt
        return bal

    def _make_block(self, n: int, genesis: bool = False) -> None:
        parent = self.blocks.get(n - 1)
        h = _h("block", n, self.salt, parent.hash if parent else "")
        blk = Block(n, h, GENESIS_TS + 2 * (n - self.first))
        self.blocks[n] = blk
        bal = self._balances_before(n)
        moves: list[tuple[str, str, int]] = []
        if genesis:
            moves = [(ZERO_ADDRESS, a, 1_000_000 * 10**self.decimals) for a in self.holders]
        else:
            for _ in range(self.rng.randint(0, 6)):
                r = self.rng.random()
                if r < 0.08:  # mint
                    moves.append((ZERO_ADDRESS, self.rng.choice(self.holders), self.rng.randint(1, 10**12)))
                    continue
                frm = self.rng.choice(self.holders)
                avail = bal[frm] - sum(a for f, _, a in moves if f == frm)
                if avail <= 0:
                    continue
                amt = self.rng.randint(1, max(1, avail // 3))
                to = ZERO_ADDRESS if r < 0.14 else self.rng.choice(self.holders)  # some burns
                moves.append((frm, to, amt))
        for i, (frm, to, amt) in enumerate(moves):
            blk.logs.append(
                {
                    "address": TOKEN,
                    "topics": [TRANSFER_TOPIC, _topic(frm), _topic(to)],
                    "data": "0x" + f"{amt:064x}",
                    "blockNumber": hex(n),
                    "blockHash": h,
                    "blockTimestamp": hex(blk.timestamp),
                    "transactionHash": _h("tx", n, i, self.salt),
                    "transactionIndex": hex(i),
                    "logIndex": hex(i),
                    "removed": False,
                }
            )
        # sprinkle an NFT-style Transfer from another contract: must be filtered by address
        if not genesis and self.rng.random() < 0.1:
            blk.logs.append(
                {
                    "address": _addr(0xBEEF),
                    "topics": [TRANSFER_TOPIC, _topic(self.holders[0]), _topic(self.holders[1]), "0x" + "0" * 63 + "1"],
                    "data": "0x",
                    "blockNumber": hex(n),
                    "blockHash": h,
                    "transactionHash": _h("nft", n, self.salt),
                    "transactionIndex": hex(99),
                    "logIndex": hex(len(blk.logs)),
                    "removed": False,
                }
            )

    # ---- scenario controls ----------------------------------------------

    @property
    def head(self) -> int:
        return max(self.blocks)

    def advance(self, n: int) -> None:
        for b in range(self.head + 1, self.head + 1 + n):
            self._make_block(b)

    def reorg(self, from_block: int) -> None:
        """Replace every block from `from_block` up with a different history (new hashes, new transfers)."""
        self.salt += 1
        top = self.head
        for n in range(from_block, top + 1):
            del self.blocks[n]
        for n in range(from_block, top + 1):
            self._make_block(n)

    def silent_mint(self, block: int, amount: int) -> None:
        """Increase totalSupply at `block` without emitting a Transfer (a broken or exotic token)."""
        self.silent_supply[block] += amount

    # ---- ChainReader -----------------------------------------------------

    def chain_id(self) -> int:
        return self._chain_id

    def finalized_block(self) -> tuple[int, str]:
        n = self.head - self.finality_lag
        return n, self.blocks[n].hash

    def block_hash(self, number: int) -> str:
        return self.blocks[number].hash

    def get_logs(self, address: str, topic0: str, from_block: int, to_block: int) -> list[dict]:
        self.get_logs_calls += 1
        if self.range_limit is not None and to_block - from_block + 1 > self.range_limit:
            raise RangeTooLarge(f"ranges over {self.range_limit} blocks are not supported")
        out = []
        for n in range(from_block, to_block + 1):
            for lg in self.blocks[n].logs:
                if lg["address"] == address.lower() and lg["topics"][0] == topic0:
                    out.append(dict(lg))
        return out

    def _state(self, block: int) -> tuple[int, dict[str, int]]:
        bal = self._balances_before(block + 1)
        supply = sum(v for a, v in bal.items() if a != ZERO_ADDRESS)
        supply += sum(v for b, v in self.silent_supply.items() if b <= block)
        return supply, bal

    def call_many(self, calls: list[tuple[str, str]], block: int) -> list[str]:
        supply, bal = self._state(block)
        out = []
        for to, data in calls:
            if to.lower() != TOKEN:
                out.append("0x")
            elif data == SEL_TOTAL_SUPPLY:
                out.append("0x" + f"{supply:064x}")
            elif data == SEL_DECIMALS:
                out.append("0x" + f"{self.decimals:064x}")
            elif data.startswith(SEL_BALANCE_OF):
                out.append("0x" + f"{bal.get('0x' + data[-40:], 0):064x}")
            else:
                raise ValueError(f"unsupported call {data[:10]}")
        return out

    def block_timestamps(self, numbers: list[int]) -> dict[int, int]:
        return {n: self.blocks[n].timestamp for n in numbers}


class FaultyReader:
    """Wraps a reader and corrupts what get_logs returns, to prove the gate notices."""

    def __init__(
        self,
        inner: FakeChain,
        *,
        drop_every: int = 0,
        bump_amount_at: int | None = None,
        strip_timestamps: bool = False,
    ) -> None:
        self.inner = inner
        self.drop_every = drop_every
        self.bump_amount_at = bump_amount_at
        self.strip_timestamps = strip_timestamps

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def get_logs(self, address, topic0, from_block, to_block):
        logs = self.inner.get_logs(address, topic0, from_block, to_block)
        out = []
        for i, lg in enumerate(logs):
            if self.drop_every and i % self.drop_every == self.drop_every - 1:
                continue
            lg = dict(lg)
            if self.bump_amount_at is not None and int(lg["blockNumber"], 16) == self.bump_amount_at:
                lg["data"] = "0x" + f"{int(lg['data'], 16) + 1:064x}"
            if self.strip_timestamps:
                lg.pop("blockTimestamp", None)
            out.append(lg)
        return out
