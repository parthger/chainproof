"""Render a self-contained HTML dashboard from one or more chainproof databases.

Everything shown is read from the databases: published rows, the run log, stored check
results and recorded gaps. No RPC calls, no external scripts or fonts, so the page renders
the same in CI, on GitHub Pages and in a screenshot.
"""

from __future__ import annotations

import html
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from .config import Chain, Token
from .decode import format_units

REPO = "https://github.com/parthger/chainproof"
CHECK_ORDER = ["completeness", "supply", "balances", "canonical", "timestamps", "decimals", "canary"]


@dataclass
class ChainPanel:
    chain: Chain
    token: Token
    ok: bool = False
    has_data: bool = False
    verdict: str = "no data"
    last_run_at: datetime | None = None
    last_published_at: datetime | None = None
    watermark: int | None = None
    lag_blocks: int | None = None
    freshness_min: float | None = None
    runs_published: int = 0
    runs_failed: int = 0
    blocks_verified: int = 0
    transfers: int = 0
    volume_raw: int = 0
    net_supply_raw: int = 0
    total_supply_raw: int | None = None
    total_supply_block: int | None = None
    checks: list[dict] = field(default_factory=list)
    last_error: str | None = None
    hourly: list[tuple[datetime, int]] = field(default_factory=list)
    gaps: list[tuple] = field(default_factory=list)
    recent: list[tuple] = field(default_factory=list)


def _cols(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {r[0] for r in con.execute(f"SELECT column_name FROM duckdb_columns() WHERE table_name = '{table}'").fetchall()}


def _tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()}


def load_panel(chain: Chain, token: Token, path: str, now: datetime, hours: int) -> ChainPanel:
    p = ChainPanel(chain, token)
    if not Path(path).exists():
        p.verdict = "database not found"
        return p
    con = duckdb.connect(path, read_only=True)
    try:
        con.execute("SET TimeZone = 'UTC'")
        cid, addr = chain.chain_id, token.address
        since = (now - timedelta(hours=hours)).replace(tzinfo=None)  # the database stores naive UTC
        tables = _tables(con)

        wm = con.execute("SELECT block_number FROM watermarks WHERE chain_id = ? AND token = ?", [cid, addr]).fetchone()
        p.watermark = int(wm[0]) if wm else None

        fin_col = "finalized_block" if "finalized_block" in _cols(con, "runs") else "NULL"
        runs = con.execute(
            f"""SELECT run_id, start_block, end_block, status, row_count, checks, error, started_at, finished_at, {fin_col}
                FROM runs WHERE chain_id = ? AND token = ? ORDER BY started_at DESC""",
            [cid, addr],
        ).fetchall()
        p.recent = runs[:8]
        if runs:
            p.has_data = True
            latest = runs[0]
            p.last_run_at = latest[8] or latest[7]
            finals = [r[9] for r in runs if r[9] is not None]
            if finals and p.watermark is not None:
                p.lag_blocks = max(0, int(finals[0]) - p.watermark)
            window = [r for r in runs if (r[8] or r[7]) >= since]
            p.runs_published = sum(1 for r in window if r[3] == "published")
            p.runs_failed = sum(1 for r in window if r[3] == "failed")
            p.blocks_verified = sum(int(r[2]) - int(r[1]) + 1 for r in window if r[3] == "published")

            last_pub = next((r for r in runs if r[3] == "published"), None)
            if last_pub:
                p.last_published_at = last_pub[8]
                p.checks = json.loads(last_pub[5] or "[]")
                sup = next((c for c in p.checks if c["name"] == "supply"), None)
                if sup and "supply_after" in sup.get("detail", {}):
                    p.total_supply_raw = int(sup["detail"]["supply_after"])
                    p.total_supply_block = int(last_pub[2])

            if latest[3] == "failed":
                p.ok = False
                p.verdict = "latest run FAILED"
                p.last_error = (latest[6] or "").splitlines()[0][:220] if latest[6] else None
                failed_checks = json.loads(latest[5] or "[]")
                if failed_checks:
                    p.checks = failed_checks
            elif last_pub:
                p.ok = True
                p.verdict = "all checks passing"
            else:
                p.verdict = f"latest run {latest[3]}"

        row = con.execute(
            """SELECT count(*),
                      coalesce(sum(amount_raw::BIGNUM), 0::BIGNUM)::VARCHAR,
                      coalesce(sum(CASE WHEN from_address = '0x0000000000000000000000000000000000000000'
                                        THEN amount_raw::BIGNUM END), 0::BIGNUM)::VARCHAR,
                      coalesce(sum(CASE WHEN to_address = '0x0000000000000000000000000000000000000000'
                                        THEN amount_raw::BIGNUM END), 0::BIGNUM)::VARCHAR,
                      max(block_time)
               FROM transfers WHERE chain_id = ? AND token = ? AND block_time >= ?""",
            [cid, addr, since],
        ).fetchone() or (0, "0", "0", "0", None)
        p.transfers, p.volume_raw = int(row[0]), int(row[1])
        p.net_supply_raw = int(row[2]) - int(row[3])
        newest_row = con.execute("SELECT max(block_time) FROM transfers WHERE chain_id = ? AND token = ?", [cid, addr]).fetchone()
        newest = newest_row[0] if newest_row else None
        if newest is not None:
            p.freshness_min = (now.replace(tzinfo=None) - newest).total_seconds() / 60

        start_hour = (now - timedelta(hours=hours - 1)).replace(minute=0, second=0, microsecond=0, tzinfo=None)
        counts = dict(
            con.execute(
                """SELECT date_trunc('hour', block_time), count(*) FROM transfers
                   WHERE chain_id = ? AND token = ? AND block_time >= ? GROUP BY 1""",
                [cid, addr, start_hour],
            ).fetchall()
        )
        p.hourly = [(start_hour + timedelta(hours=i), int(counts.get(start_hour + timedelta(hours=i), 0))) for i in range(hours)]

        if "gaps" in tables:
            p.gaps = con.execute(
                "SELECT from_block, to_block, reason, created_at FROM gaps WHERE chain_id = ? AND token = ? ORDER BY from_block DESC",
                [cid, addr],
            ).fetchall()
    finally:
        con.close()
    return p


# ---- formatting --------------------------------------------------------------


def _compact(raw: int, decimals: int, signed: bool = False) -> str:
    v = raw / 10**decimals  # display only: the exact value is always shown next to it
    sign = "-" if v < 0 else ("+" if signed and v > 0 else "")
    v = abs(v)
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if v >= div:
            return f"{sign}{v / div:,.2f}{unit}"
    return f"{sign}{v:,.2f}"


def _exact(raw: int, decimals: int, signed: bool = False) -> str:
    s = format_units(raw, decimals)
    whole, _, frac = s.lstrip("-").partition(".")
    whole = f"{int(whole):,}"
    sign = "-" if raw < 0 else ("+" if signed and raw > 0 else "")
    return f"{sign}{whole}.{frac}" if frac else f"{sign}{whole}"


def _ago(dt: datetime | None, now: datetime) -> str:
    if dt is None:
        return "never"
    m = (now.replace(tzinfo=None) - dt).total_seconds() / 60
    if m < 1:
        return "just now"
    if m < 90:
        return f"{m:.0f} min ago"
    if m < 48 * 60:
        return f"{m / 60:.1f} h ago"
    return f"{m / 1440:.1f} days ago"


def _e(s) -> str:
    return html.escape(str(s), quote=True)


# ---- rendering -----------------------------------------------------------------

ICON_OK = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.5 8.5l3 3 6-7" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
ICON_FAIL = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4.5 4.5l7 7m0-7l-7 7" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>'
ICON_WARN = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 3v6m0 3v.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>'


def _badge(kind: str, label: str) -> str:
    icon = {"good": ICON_OK, "critical": ICON_FAIL, "warning": ICON_WARN}[kind]
    return f'<span class="badge {kind}">{icon}<span>{_e(label)}</span></span>'


def _bars(p: ChainPanel, hours: int) -> str:
    w, h, pad_l, pad_b, pad_t = 560, 120, 34, 18, 8
    n = len(p.hourly)
    peak = max((c for _, c in p.hourly), default=0)
    if peak == 0:
        return '<div class="empty">No transfers indexed in this window yet.</div>'
    top = _nice(peak)
    plot_w, plot_h = w - pad_l - 4, h - pad_b - pad_t
    slot = plot_w / n
    bw = max(2.0, slot - 2)  # 2px surface gap between bars
    parts = [
        f'<svg class="bars" viewBox="0 0 {w} {h}" role="img" aria-label="Transfers per hour, last {hours} hours, peak {peak:,}">'
    ]
    for frac in (0.5, 1.0):
        y = pad_t + plot_h * (1 - frac)
        parts.append(f'<line class="grid" x1="{pad_l}" x2="{w - 4}" y1="{y:.1f}" y2="{y:.1f}"/>')
        parts.append(f'<text class="axis" x="{pad_l - 6}" y="{y + 3:.1f}" text-anchor="end">{_short(int(top * frac))}</text>')
    base = pad_t + plot_h
    parts.append(f'<line class="baseline" x1="{pad_l}" x2="{w - 4}" y1="{base}" y2="{base}"/>')
    for i, (hour, c) in enumerate(p.hourly):
        x = pad_l + i * slot + (slot - bw) / 2
        bh = plot_h * c / top
        tip = f"{hour:%b %d %H:00} UTC · {c:,} transfers"
        if c > 0:
            r = min(4.0, bw / 2, bh)
            parts.append(
                f'<path class="bar" d="M{x:.1f},{base} V{base - bh + r:.1f} Q{x:.1f},{base - bh:.1f} {x + r:.1f},{base - bh:.1f} '
                f'H{x + bw - r:.1f} Q{x + bw:.1f},{base - bh:.1f} {x + bw:.1f},{base - bh + r:.1f} V{base} Z"/>'
            )
        parts.append(
            f'<rect class="hit" x="{pad_l + i * slot:.1f}" y="{pad_t}" width="{slot:.1f}" height="{plot_h + pad_b}" data-tip="{_e(tip)}"/>'
        )
        if i % 6 == 0:
            parts.append(f'<text class="axis" x="{x + bw / 2:.1f}" y="{h - 4}" text-anchor="middle">{hour:%H:00}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _nice(v: int) -> int:
    if v <= 5:
        return 5
    mag = 10 ** (len(str(v)) - 1)
    for m in (1, 2, 2.5, 5, 10):
        if v <= m * mag:
            return int(m * mag)
    return v


def _short(v: int) -> str:
    return f"{v / 1000:g}k" if v >= 1000 else str(v)


def _panel(p: ChainPanel, now: datetime, hours: int) -> str:
    d = p.token.decimals
    sym = p.token.symbol
    status = _badge("good", "All checks passing") if p.ok else _badge("critical", p.verdict.capitalize())
    if not p.has_data:
        status = _badge("warning", p.verdict.capitalize())

    fresh = (
        "–"
        if p.freshness_min is None
        else (f"{p.freshness_min:.0f} min" if p.freshness_min < 120 else f"{p.freshness_min / 60:.1f} h")
    )
    lag = "–" if p.lag_blocks is None else f"{p.lag_blocks:,} blocks"
    supply = (
        f'<div class="supply"><span class="label">totalSupply at block {p.total_supply_block:,}</span>'
        f'<span class="mono">{_exact(p.total_supply_raw, d)} {sym}</span></div>'
        if p.total_supply_raw is not None
        else ""
    )

    tiles = f"""
      <div class="tiles">
        <div class="tile"><div class="label">Transfers verified</div><div class="value">{p.transfers:,}</div>
          <div class="sub">{p.blocks_verified:,} blocks</div></div>
        <div class="tile"><div class="label">Volume</div><div class="value">{_compact(p.volume_raw, d)}</div>
          <div class="sub mono" title="exact">{_exact(p.volume_raw, d)}</div></div>
        <div class="tile"><div class="label">Net supply change</div><div class="value">{_compact(p.net_supply_raw, d, signed=True)}</div>
          <div class="sub mono" title="exact, = Δ totalSupply">{_exact(p.net_supply_raw, d, signed=True)}</div></div>
        <div class="tile"><div class="label">Data freshness</div><div class="value">{fresh}</div>
          <div class="sub">lag {lag}</div></div>
      </div>"""

    by_name = {c["name"]: c for c in p.checks}
    rows = []
    for name in CHECK_ORDER + [n for n in by_name if n not in CHECK_ORDER]:
        c = by_name.get(name)
        if c is None:
            continue
        mark = _badge("good", "PASS") if c["passed"] else _badge("critical", "FAIL")
        rows.append(
            f'<tr><td>{mark}</td><td><div class="name">{_e(name)}</div><div class="mono summary">{_e(c["summary"])}</div></td></tr>'
        )
    checks = (
        f'<table class="checks"><tbody>{"".join(rows)}</tbody></table>'
        if rows
        else '<div class="empty">No check results recorded yet.</div>'
    )
    err = f'<div class="error mono">{_e(p.last_error)}</div>' if p.last_error else ""

    gap_html = ""
    if p.gaps:
        g0, g1, why = p.gaps[0][:3]
        more = f" and {len(p.gaps) - 1} earlier" if len(p.gaps) > 1 else ""
        gap_html = (
            f'<div class="gap">{_badge("warning", "Gap recorded")}<span>blocks {g0:,}–{g1:,} were skipped, not verified '
            f"({_e(why[:80])}){more}. Shown, never hidden.</span></div>"
        )

    run_rows = "".join(
        f'<tr><td class="mono">{(r[8] or r[7]):%m-%d %H:%M}</td><td class="mono">{int(r[1]):,}–{int(r[2]):,}</td>'
        f'<td>{_e(r[3])}</td><td class="num">{"" if r[4] is None else f"{int(r[4]):,}"}</td></tr>'
        for r in p.recent[:5]
    )
    runs = (
        f'<details><summary>Recent runs</summary><table class="runs"><thead><tr><th>UTC</th><th>blocks</th><th>status</th>'
        f'<th class="num">rows</th></tr></thead><tbody>{run_rows}</tbody></table></details>'
        if run_rows
        else ""
    )

    return f"""
    <section class="panel {"ok" if p.ok else "bad"}">
      <div class="head">
        <header><h2>{_e(sym)} on {_e(p.chain.name.capitalize())}</h2>{status}</header>
        <div class="meta">last run {_ago(p.last_run_at, now)} · {p.runs_published} published, {p.runs_failed} failed in {hours}h
          · watermark {"–" if p.watermark is None else f"{p.watermark:,}"}</div>
      </div>
      {err}{gap_html}{tiles}
      <div class="chart"><div class="label">Transfers per hour (UTC)</div>{_bars(p, hours)}</div>
      {checks}{supply}{runs}
    </section>"""


CSS = """
:root{color-scheme:light;--bg:#f4f3f0;--surface:#fcfcfb;--border:#e3e2dd;--text:#0b0b0b;--text2:#52514e;--muted:#7a7974;
--series:#2a78d6;--series-hover:#1c5cab;--grid:#ecebe7;--good:#0ca30c;--good-ink:#006300;--critical:#d03b3b;--critical-ink:#b02a2a;
--warning:#fab219;--warning-ink:#8a5a00;--good-bg:#e7f6e7;--critical-bg:#fbeaea;--warning-bg:#fdf3dc}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--bg:#111110;--surface:#1a1a19;--border:#2e2e2b;
--text:#f0efec;--text2:#c3c2b7;--muted:#8f8e86;--series:#3987e5;--series-hover:#6da7ec;--grid:#262624;--good-ink:#5fd35f;
--critical-ink:#f08a8a;--warning-ink:#fab219;--good-bg:#132a13;--critical-bg:#331717;--warning-bg:#33280f}}
:root[data-theme="dark"]{color-scheme:dark;--bg:#111110;--surface:#1a1a19;--border:#2e2e2b;--text:#f0efec;--text2:#c3c2b7;--muted:#8f8e86;
--series:#3987e5;--series-hover:#6da7ec;--grid:#262624;--good-ink:#5fd35f;--critical-ink:#f08a8a;--warning-ink:#fab219;
--good-bg:#132a13;--critical-bg:#331717;--warning-bg:#33280f}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.wrap{max-width:1240px;margin:0 auto;padding:28px 16px 40px}
.top{display:flex;flex-wrap:wrap;gap:12px 24px;align-items:flex-end;justify-content:space-between;margin-bottom:20px}
h1{margin:0;font-size:26px;letter-spacing:-.01em}
.tagline{color:var(--text2);margin-top:4px}
.overall{display:flex;flex-direction:column;align-items:flex-end;gap:4px}
.overall .meta{text-align:right}
.meta{color:var(--muted);font-size:12.5px}
.grid3{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,380px),1fr));gap:16px}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:18px;display:flex;flex-direction:column;gap:14px;min-width:0}
.panel header{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
h2{margin:0;font-size:17px}
.badge{display:inline-flex;align-items:center;gap:5px;padding:3px 9px 3px 7px;border-radius:999px;font-size:12px;font-weight:600;white-space:nowrap}
.badge svg{width:14px;height:14px;flex:none}
.badge.good{background:var(--good-bg);color:var(--good-ink)}
.badge.critical{background:var(--critical-bg);color:var(--critical-ink)}
.badge.warning{background:var(--warning-bg);color:var(--warning-ink)}
.overall .badge{font-size:14px;padding:6px 12px 6px 10px}
.overall .badge svg{width:16px;height:16px}
.tiles{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.tile{border:1px solid var(--border);border-radius:10px;padding:10px 12px;min-width:0}
.label{color:var(--muted);font-size:11.5px;text-transform:uppercase;letter-spacing:.04em}
.value{font-size:22px;font-weight:650;margin-top:2px;font-variant-numeric:tabular-nums}
.sub{color:var(--text2);font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-variant-numeric:tabular-nums}
.chart .label{margin-bottom:6px}
svg.bars{width:100%;height:auto;display:block}
.bars .bar{fill:var(--series)}
.bars .grid{stroke:var(--grid);stroke-width:1}
.bars .baseline{stroke:var(--border);stroke-width:1}
.bars .axis{fill:var(--muted);font-size:10px}
.bars .hit{fill:transparent;cursor:default}
.bars .hit:hover{fill:var(--grid);fill-opacity:.6}
table{width:100%;border-collapse:collapse}
.checks td{padding:5px 6px;border-top:1px solid var(--border);vertical-align:top}
.checks tr:first-child td{border-top:0}
.checks td:first-child{width:1%;padding-left:0}
.checks .name{font-weight:600}
.checks .summary{color:var(--text2);font-size:11.5px;word-break:break-word;margin-top:1px}
.head{display:flex;flex-direction:column;gap:4px}
.supply{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;border-top:1px dashed var(--border);padding-top:10px;font-size:12.5px}
.gap,.error{display:flex;gap:8px;align-items:flex-start;font-size:12.5px;color:var(--text2)}
.error{background:var(--critical-bg);color:var(--critical-ink);padding:8px 10px;border-radius:8px;word-break:break-word}
.empty{color:var(--muted);font-size:12.5px;padding:8px 0}
details summary{cursor:pointer;color:var(--text2);font-size:12.5px}
.runs th,.runs td{text-align:left;padding:4px 6px;font-size:12px;border-top:1px solid var(--border)}
.runs th{color:var(--muted);font-weight:500}
.num{text-align:right}
.foot{margin-top:22px;color:var(--muted);font-size:12.5px;display:flex;flex-wrap:wrap;gap:6px 18px;justify-content:space-between}
.foot a{color:var(--text2)}
#tip{position:fixed;pointer-events:none;background:var(--text);color:var(--surface);padding:5px 8px;border-radius:6px;font-size:12px;
opacity:0;transition:opacity .08s;white-space:nowrap;z-index:9}
"""

JS = """
const th=location.hash.slice(1);if(th==='light'||th==='dark')document.documentElement.dataset.theme=th;
const tip=document.getElementById('tip');
document.addEventListener('mousemove',e=>{const t=e.target.closest&&e.target.closest('[data-tip]');
if(!t){tip.style.opacity=0;return}tip.textContent=t.dataset.tip;tip.style.opacity=1;
const x=Math.min(e.clientX+12,innerWidth-tip.offsetWidth-8);tip.style.left=x+'px';tip.style.top=(e.clientY-34)+'px'});
"""


def render(sources: list[tuple[Chain, Token, str]], out: str | Path, *, hours: int = 24, now: datetime | None = None) -> Path:
    now = now or datetime.now(UTC)
    panels = [load_panel(c, t, path, now, hours) for c, t, path in sources]
    failing = [p for p in panels if p.has_data and not p.ok]
    empty = [p for p in panels if not p.has_data]
    if failing:
        overall = _badge("critical", f"{len(failing)} of {len(panels)} chains failing")
    elif empty:
        overall = _badge("warning", f"{len(panels) - len(empty)} of {len(panels)} chains reporting")
    else:
        overall = _badge("good", f"All checks passing on {len(panels)} chains")
    total_tx = sum(p.transfers for p in panels)
    total_runs = sum(p.runs_published for p in panels)

    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>chainproof live</title><style>{CSS}</style></head>
<body><div class="wrap">
  <div class="top">
    <div><h1>chainproof · live reconciliation</h1>
      <div class="tagline">USDC transfers, published only after zero-tolerance checks against on-chain state. Last {hours} hours.</div></div>
    <div class="overall">{overall}
      <div class="meta">{total_tx:,} transfers verified across {total_runs} gated runs<br>generated {now:%Y-%m-%d %H:%M} UTC</div></div>
  </div>
  <div class="grid3">{"".join(_panel(p, now, hours) for p in panels)}</div>
  <div class="foot"><span>Finalized blocks only · every run plants a +1 raw-unit fault that the checks must catch ·
    amounts exact to the raw unit</span><a href="{REPO}">{REPO.removeprefix("https://")}</a></div>
</div><div id="tip" role="tooltip"></div><script>{JS}</script></body></html>
"""
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(page)
    return out
