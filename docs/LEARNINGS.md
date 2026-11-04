# What I learned building chainproof

Study notes from the *Blockchain Data Engineering Guide* (Unchain Data, 2026.10), in my own words, each tied to the place in this repo where I used it. At the end of each section is a question to answer out loud before an interview.

---

## 1. The job is the mainstream data stack, applied to chain data

Crypto data postings ask for Python, SQL, Airflow, dbt, Spark/Kafka and a warehouse far more than for any onchain tool. Onchain skills are the differentiator on top of that stack, not a replacement for it. The roles are mostly at exchanges. The deepest onchain work is at protocols, infra and analytics companies.

**In this repo:** Python + SQL (DuckDB), a CI-scheduled pipeline, and warehouse-style modeling (staging → published → mart).

> **Q:** Why build an indexer in Python when most indexers are TypeScript or Rust?
> Python is what you're hired for. Indexer frameworks are what you integrate with. Knowing how one works underneath lets you judge any framework.

---

## 2. What the chain actually records (the chapter everything else depends on)

- **Five EVM raw datasets:** blocks, transactions, receipts/logs, traces, state. Logs do most of the work.
- **A log** has an emitting `address`, up to 4 topics, and `data`. `topics[0]` = keccak of the event signature. Indexed params go in topics 1–3, and everything else is ABI-encoded in `data`.
- **ERC-20 vs ERC-721 `Transfer`** share topic0. ERC-721 has 4 topics, so count topics before decoding.
- **Always filter on `(address, topic0)`.** Spam tokens emit perfectly valid `Transfer` events.
- **Proxies:** events come from the proxy address, and the ABI lives on the implementation. Implementations change over time.
- **Account abstraction:** with ERC-4337 `tx.from` is the bundler, and with EIP-7702 "is this a contract?" no longer has a stable answer.
- **Finality differs by chain.** Ethereum's `finalized` tag is ~13 minutes behind head. OP-stack L2s are final once L1 finalizes. Solana is finalized in ~150 ms since Alpenglow (Oct 2026).
- **Solana is different:** programs vs accounts, inner instructions carry the economics, pre/post token balances give net deltas, slots ≠ blocks, 200 ms slots since Oct 2026.

**In this repo:** [decode.py](../chainproof/decode.py) decodes by hand, rejects 4-topic logs, rejects address topics with non-zero padding, and rejects `removed: true` logs. [extract.py](../chainproof/extract.py) rejects logs from an address we didn't ask for.

> **Q:** A `Transfer` log has non-zero bytes in the first 12 bytes of `topics[1]`. What do you do?
> Fail. Taking the last 20 bytes turns garbage into a plausible address, and that bug never shows up as an error.

---

## 3. Research before you build

The most expensive bug is indexing the wrong thing: the implementation instead of the proxy, the wrong factory, a fee-on-transfer token. Answer five questions first: which contracts, which entry points, which edge cases, who can change what, and how you'll validate. Write it down as a spec with a set of "golden transactions".

**In this repo:** `chainproof inspect` reads decimals, supply and both proxy slots. [docs/spec-usdc.md](spec-usdc.md) is the spec. The golden transaction is in [tests/test_decode.py](../tests/test_decode.py).

**What I found myself:** USDC's EIP-1967 slot is empty on all three chains. The implementation sits in the older zeppelinos slot.

> **Q:** How do you confirm a contract is the one you think it is?
> Read it on-chain at a pinned block (storage slots, role getters, `decimals()`), cross-check with an explorer, and decode one real transaction by hand.

---

## 4. Raw data access

- JSON-RPC is universal, but `eth_getLogs` is capped per provider, and every request costs money or rate limit.
- For history, bulk sources beat RPC: cryo → Parquet, HyperSync, SQD Portal, CDP SQL API, BigQuery/AWS public datasets.
- **Never let a float touch a raw amount.** Above 2^53 (~9 billion USDC) a float can't represent every integer.
- Cost has four drivers: requests, history depth, traces, and storage/scans.

**In this repo:** amounts are Python `int` from decode to DuckDB `BIGNUM`. `format_units` prints exactly. The test `test_format_units_is_exact_beyond_float_precision` shows the float failure.

> **Q:** Why store `amount_raw` as a string?
> No common warehouse type holds every uint256 (BigQuery's `BIGNUMERIC` tops out near 5.8×10^38). Store it exactly, and sum in an engine with a wide integer.

---

## 5. Indexing frameworks

Every indexer is a **data source** (RPC, data node, Firehose, in-node), a **handler runtime**, and a **sink**, with reorg handling across all three. Write the 20-line version first. It shows what frameworks sell: paging, retries, persistence, reorgs, following the head. Compare indexers on **canonical rows and exact values**, not on row counts.

**In this repo:** chainproof is the "build it yourself" version of those four gaps. Its `completeness` check is exactly the canonical-row comparison, run against an independent source.

> **Q:** Ponder, Envio, SQD, rindexer: how do you choose?
> First by output (app API vs analytics warehouse), then by team language, then by data source for backfill speed. Check licenses: Envio isn't OSI-approved, and SQD's package licenses disagree with its repo.

---

## 6–7. Solana and streaming

- Index Solana at the instruction level, including inner instructions, keyed by `(signature, outer_index, inner_index)`. Token-2022 extensions (transfer fees, hooks, confidential transfers) break "sent = received".
- Streaming rules: **key by chain position**, **carry finality status in every message**, **emit explicit retractions** (tombstones) on reorgs, and **assume at-least-once delivery**, so make writes idempotent.
- Version numbers come from one persisted sequence. Not block number (a reorged block keeps its number), not ingest time (a delayed replay resurrects deleted rows).

**Next step for this repo:** a Redpanda → ClickHouse sink with `ReplacingMergeTree(version, is_deleted)`, fed from the published table.

---

## 8–9. Storage

- Choose the engine by query shape: Postgres for serving and exact math, ClickHouse for chain-scale scans, DuckDB for files and CI.
- **Pick the uint256 policy before the first row.**
- In ClickHouse the table engine is your reorg policy. The ORDER BY key must be chain position only, and a materialized view is an insert trigger that never sees replacements.
- A finalized-only store with a watermark table updated *in the same transaction* as the data is simple, exact, and enough for one protocol.
- Warehouses: partition by date, cluster by contract, and give address lookups their own layout.

**In this repo:** [store.py](../chainproof/store.py) has the primary key on chain position, CHECK constraints on hex lengths and digit-only amounts, the watermark with hash, and publish + mart + watermark in one transaction.

> **Q:** Net flows for an address over a month: is that its balance?
> No. A balance needs an opening balance (`balanceOf` at `start−1`). That's exactly what the `balances` check does.

---

## 10. Buy vs build

Buy coverage, build specifics. Dune, Allium and SonarX cover common questions, while contract-specific logic and low latency need your own pipeline. Always reconcile a vendor number against an independent computation. Order of suspects when counts differ: day boundary/timezone, mints and burns, then the table's definition of "transfer". Never reconcile on a `DOUBLE` sum, because its last digits change with row order.

**In this repo:** the mart uses UTC days explicitly (`SET TimeZone = 'UTC'`) and labels partial days.

---

## 11. Transformation and reconciliation

- Layers: raw → decoded → staging → curated → marts, with one definition per metric.
- **Incremental models must replace whole ranges.** A key-based merge leaves rows for groups that a reorg emptied.
- **Reconciliation is the strongest test:** sum of events = contract state at block N, pinned, finalized, zero tolerance. Print the direction of any difference.

**In this repo:** `_rebuild_days` deletes and recomputes whole days. `test_rewind_removes_days_that_lost_all_rows` proves the ghost-row bug can't happen. The supply check prints "state exceeds events" vs "events exceed state".

> **Q:** Why zero tolerance?
> A tolerance hides real bugs. When the math is integers, any non-zero difference is either a bug or a real-world effect you haven't modeled, like a donation or a silent mint. Both are worth knowing.

---

## 12. Operations

- A pipeline isn't done until it **fails loudly**: run daily, recover, backfill, and alert on *data* (freshness, volume, gaps, reconciliation), not only on task failure.
- Backfill and live tailing are different jobs.
- Watermarks live in durable state with the block hash, and move only in the publish transaction.
- Build to staging, publish after the checks.
- Classify retries: halve the range vs back off.
- **A guard needs a planted fault that must turn it red.** A check that can't fail proves nothing.

**In this repo:** all of it. `run_canary` plants a +1 fault on every live run and fails the run if the gate doesn't notice.

---

## 13. Analytics craft and agents

Write every metric definition down, since most public disagreements are definition differences. "Active addresses" should mean signers, not recipients. Volume through aggregators gets double counted. Fees aren't revenue. With AI agents in the loop: research, estimate cost, run small, check three transactions by hand, recount independently, publish with a footnote.

---

## 14. Getting hired

Build projects that **end in a verification**, and say so. Resume bullet format: *verb, tool, measured result, check.*

> Built a Python + DuckDB ERC-20 indexer for USDC on Base, Ethereum and Arbitrum; reconciled 129k transfers per hour of Base to `totalSupply` and `balanceOf` with zero divergence; planted-fault canary on every run; scheduled daily in GitHub Actions.

---

## Interview drill: explain these without notes

1. Why `(chain_id, block_number, log_index)` and not `tx_hash` as the key?
2. What happens to your daily mart if a reorg removes the only transfer of a day?
3. Why can't you use `block_number` as a version for reorg tombstones?
4. A provider says your 200-block query is "over 10,000 blocks". What do you do?
5. Your volume is 3% above the protocol's dashboard. Walk through the diagnosis.
6. Why does the supply check miss a wrong holder-to-holder amount, and what catches it?
