"""Chains and tokens chainproof knows about.

Every address here was checked on-chain before it was added (see `chainproof inspect`).
Decimals are re-read from the contract on every run and the run fails if they differ.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Chain:
    name: str
    chain_id: int
    rpc_urls: tuple[str, ...]  # first is primary, second (if any) is the independent verifier
    explorer: str


@dataclass(frozen=True)
class Token:
    symbol: str
    chain: str
    address: str  # lowercase
    decimals: int


CHAINS: dict[str, Chain] = {
    "base": Chain(
        "base",
        8453,
        ("https://base.drpc.org", "https://base-rpc.publicnode.com", "https://mainnet.base.org"),
        "https://basescan.org",
    ),
    "ethereum": Chain(
        "ethereum",
        1,
        ("https://eth.drpc.org", "https://ethereum-rpc.publicnode.com"),
        "https://etherscan.io",
    ),
    "arbitrum": Chain(
        "arbitrum",
        42161,
        ("https://arb1.arbitrum.io/rpc", "https://arbitrum.drpc.org"),
        "https://arbiscan.io",
    ),
}

TOKENS: dict[tuple[str, str], Token] = {
    ("base", "USDC"): Token("USDC", "base", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", 6),
    ("ethereum", "USDC"): Token("USDC", "ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6),
    ("arbitrum", "USDC"): Token("USDC", "arbitrum", "0xaf88d065e77c8cc2239327c5edb3a432268e5831", 6),
}


def get_chain(name: str) -> Chain:
    try:
        return CHAINS[name]
    except KeyError:
        raise SystemExit(f"unknown chain {name!r}; known: {', '.join(CHAINS)}") from None


def get_token(chain: str, symbol: str) -> Token:
    try:
        return TOKENS[(chain, symbol.upper())]
    except KeyError:
        known = ", ".join(f"{c}:{s}" for c, s in TOKENS)
        raise SystemExit(f"unknown token {chain}:{symbol}; known: {known}") from None
