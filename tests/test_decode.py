import pytest

from chainproof.decode import (
    TRANSFER_TOPIC,
    DecodeError,
    decode_string,
    decode_transfer,
    encode_balance_of,
    format_units,
    topic_to_address,
)

# A real Base USDC transfer (block 52,955,608, tx 0xb4cff99a…f87ef9): 5 USDC.
REAL_LOG = {
    "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
    "topics": [
        TRANSFER_TOPIC,
        "0x00000000000000000000000090f2fc66ffe81ed5f028bb46b6089ac5c5b8eb56",
        "0x0000000000000000000000007707d707269958e823ca8462d24dbb812a045dce",
    ],
    "data": "0x00000000000000000000000000000000000000000000000000000000004c4b40",
    "blockNumber": hex(52_955_608),
    "blockHash": "0x" + "ab" * 32,
    "transactionHash": "0xb4cff99a4a1392d41294d4b5f34966b488db40825bad84d469bb753adef87ef9",
    "logIndex": hex(32),
    "removed": False,
}


def test_decodes_real_usdc_transfer():
    t = decode_transfer(REAL_LOG, 8453)
    assert t is not None
    assert t.from_address == "0x90f2fc66ffe81ed5f028bb46b6089ac5c5b8eb56"
    assert t.to_address == "0x7707d707269958e823ca8462d24dbb812a045dce"
    assert t.amount_raw == 5_000_000
    assert format_units(t.amount_raw, 6) == "5.000000"
    assert t.key == (8453, 52_955_608, 32)


def test_erc721_transfer_is_skipped_not_misread():
    log = {**REAL_LOG, "topics": [*REAL_LOG["topics"], "0x" + "0" * 63 + "7"], "data": "0x"}
    assert decode_transfer(log, 8453) is None


def test_other_events_are_ignored():
    assert decode_transfer({**REAL_LOG, "topics": ["0x" + "11" * 32]}, 8453) is None


def test_nonzero_padding_is_not_an_address():
    with pytest.raises(DecodeError, match="padding"):
        topic_to_address("0x" + "1" + "0" * 23 + "90f2fc66ffe81ed5f028bb46b6089ac5c5b8eb56")


def test_truncated_data_fails_loudly():
    # the off-by-one substring bug: a pipeline that silently accepts a short word
    with pytest.raises(DecodeError):
        decode_transfer({**REAL_LOG, "data": REAL_LOG["data"][:-1]}, 8453)


def test_removed_log_is_refused():
    with pytest.raises(DecodeError, match="removed"):
        decode_transfer({**REAL_LOG, "removed": True}, 8453)


def test_format_units_is_exact_beyond_float_precision():
    raw = 2**256 - 1
    s = format_units(raw, 18)
    assert s.replace(".", "") == str(raw)
    assert float(raw) / 1e18 != int(s.split(".")[0])  # what a float would have done


def test_abi_helpers():
    assert encode_balance_of("0x7707d707269958e823ca8462d24dbb812a045dce").endswith("7707d707269958e823ca8462d24dbb812a045dce")
    # ABI-encoded string "USDC"
    enc = "0x" + f"{32:064x}" + f"{4:064x}" + b"USDC".hex().ljust(64, "0")
    assert decode_string(enc) == "USDC"
