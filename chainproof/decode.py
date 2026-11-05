"""Decode ERC-20 Transfer logs by hand, and fail loudly on anything malformed.

A Transfer log has:
  topics[0] = keccak256("Transfer(address,address,uint256)")
  topics[1] = from, left-padded to 32 bytes
  topics[2] = to,   left-padded to 32 bytes
  data      = value, one 32-byte word

An ERC-721 Transfer has the same topic0 but four topics, so topic count is checked first.
Amounts stay Python ints (arbitrary precision) end to end: never a float.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDRESS = "0x" + "0" * 40
UINT256_MAX = 2**256 - 1


class DecodeError(ValueError):
    """A log that claims to be a Transfer but does not have the shape of one."""


@dataclass(frozen=True)
class Transfer:
    chain_id: int
    block_number: int
    block_hash: str
    log_index: int
    tx_hash: str
    token: str
    from_address: str
    to_address: str
    amount_raw: int
    block_time: int | None = None  # unix seconds

    @property
    def key(self) -> tuple[int, int, int]:
        """Natural key: position in the chain. Survives replays, identical across tools."""
        return (self.chain_id, self.block_number, self.log_index)

    @property
    def is_mint(self) -> bool:
        return self.from_address == ZERO_ADDRESS

    @property
    def is_burn(self) -> bool:
        return self.to_address == ZERO_ADDRESS

    def with_amount(self, amount_raw: int) -> Transfer:
        return replace(self, amount_raw=amount_raw)


def _hex32(value: str, what: str) -> str:
    if not isinstance(value, str) or not value.startswith("0x") or len(value) != 66:
        raise DecodeError(f"{what} is not a 32-byte hex word: {value!r}")
    try:
        int(value, 16)
    except ValueError:
        raise DecodeError(f"{what} is not hex: {value!r}") from None
    return value.lower()


def topic_to_address(topic: str, what: str = "topic") -> str:
    word = _hex32(topic, what)
    pad, addr = word[2:26], word[26:]
    if pad != "0" * 24:
        # An address topic with non-zero padding is not an address. Silently taking the last
        # 20 bytes would turn garbage into a plausible-looking address.
        raise DecodeError(f"{what} has non-zero padding, not an address: {topic}")
    return "0x" + addr


def decode_transfer(log: dict, chain_id: int) -> Transfer | None:
    """Return a Transfer, None for a non-ERC-20 Transfer (e.g. ERC-721), or raise DecodeError."""
    topics = log.get("topics") or []
    if not topics or topics[0].lower() != TRANSFER_TOPIC:
        return None
    if len(topics) == 4:
        return None  # ERC-721: token id is indexed, there is no amount
    if len(topics) != 3:
        raise DecodeError(f"Transfer with {len(topics)} topics in tx {log.get('transactionHash')}")
    if log.get("removed"):
        raise DecodeError(f"log marked removed=true in tx {log.get('transactionHash')}; refusing a reorged log")

    data = log.get("data", "")
    amount = int(_hex32(data, "data"), 16)

    ts = log.get("blockTimestamp")
    if ts is not None and int(ts, 16) == 0:
        ts = None  # arb1 sends "0x0" instead of omitting the field: backfill the real block time
    return Transfer(
        chain_id=chain_id,
        block_number=int(log["blockNumber"], 16),
        block_hash=_hex32(log["blockHash"], "blockHash"),
        log_index=int(log["logIndex"], 16),
        tx_hash=_hex32(log["transactionHash"], "transactionHash"),
        token=log["address"].lower(),
        from_address=topic_to_address(topics[1], "topics[1]"),
        to_address=topic_to_address(topics[2], "topics[2]"),
        amount_raw=amount,
        block_time=int(ts, 16) if ts else None,
    )


def format_units(raw: int, decimals: int) -> str:
    """Exact decimal string. Never raw / 10**decimals: a float rounds above 2**53."""
    sign = "-" if raw < 0 else ""
    raw = abs(raw)
    if decimals == 0:
        return f"{sign}{raw}"
    q, r = divmod(raw, 10**decimals)
    return f"{sign}{q}.{r:0{decimals}d}"


# ---- minimal ABI helpers for the reads reconciliation needs -------------------

SEL_TOTAL_SUPPLY = "0x18160ddd"
SEL_BALANCE_OF = "0x70a08231"
SEL_DECIMALS = "0x313ce567"
SEL_SYMBOL = "0x95d89b41"


def encode_balance_of(address: str) -> str:
    a = address.lower().removeprefix("0x")
    if len(a) != 40:
        raise ValueError(f"bad address {address}")
    return SEL_BALANCE_OF + "0" * 24 + a


def decode_uint(result: str) -> int:
    if not result or result == "0x":
        raise DecodeError("empty eth_call result (no code at that address, or a reverted call)")
    return int(result, 16)


def decode_string(result: str) -> str:
    """ABI-decode a single dynamic `string` return value."""
    b = bytes.fromhex(result.removeprefix("0x"))
    if len(b) < 64:
        return b.rstrip(b"\x00").decode(errors="replace")  # some old tokens return bytes32
    offset = int.from_bytes(b[0:32], "big")
    length = int.from_bytes(b[offset : offset + 32], "big")
    return b[offset + 32 : offset + 32 + length].decode(errors="replace")
