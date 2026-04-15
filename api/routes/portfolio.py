"""Portfolio / asset-level API endpoints with retroactive date support."""

from __future__ import annotations

from fastapi import APIRouter, Query

from api.database import get_db

router = APIRouter(tags=["portfolio"])


@router.get("/portfolio/{ticker}/dates")
def get_available_dates(ticker: str):
    """Return all months with report data for a fund."""
    fund_id = ticker.lower()
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT DISTINCT reference_date FROM monthly_snapshots WHERE fund_id = ? ORDER BY reference_date",
            (fund_id,),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


@router.get("/portfolio/{ticker}/snapshot")
def get_snapshot(ticker: str, date: str = Query(None, description="YYYY-MM. If omitted, latest.")):
    """Full fund snapshot for a specific month: KPIs, assets, commentary, metrics."""
    fund_id = ticker.lower()
    conn = get_db()
    try:
        # Resolve date
        if not date:
            row = conn.execute(
                "SELECT MAX(reference_date) FROM monthly_snapshots WHERE fund_id = ?", (fund_id,)
            ).fetchone()
            date = row[0] if row and row[0] else None
        if not date:
            return {"error": "No data available"}

        # --- Monthly snapshot ---
        snap_row = conn.execute(
            "SELECT * FROM monthly_snapshots WHERE fund_id = ? AND reference_date = ?",
            (fund_id, date),
        ).fetchone()
        snapshot = dict(snap_row) if snap_row else {}

        # --- Fund info ---
        fund_row = conn.execute("SELECT * FROM funds WHERE fund_id = ?", (fund_id,)).fetchone()
        fund_info = dict(fund_row) if fund_row else {}

        # --- Transmission assets ---
        transmission = [dict(r) for r in conn.execute(
            "SELECT * FROM ops_transmission WHERE fund_id = ? AND reference_date = ? ORDER BY asset_name",
            (fund_id, date),
        ).fetchall()]

        # --- Generation assets ---
        generation = [dict(r) for r in conn.execute(
            "SELECT * FROM ops_generation WHERE fund_id = ? AND reference_date = ? ORDER BY asset_name",
            (fund_id, date),
        ).fetchall()]

        # --- Port assets ---
        port = [dict(r) for r in conn.execute(
            "SELECT * FROM ops_port WHERE fund_id = ? AND reference_date = ? ORDER BY asset_name",
            (fund_id, date),
        ).fetchall()]

        # --- Portfolio holdings (credit) ---
        holdings = [dict(r) for r in conn.execute(
            "SELECT * FROM portfolio_holdings WHERE fund_id = ? AND reference_date = ? ORDER BY pct_pl DESC",
            (fund_id, date),
        ).fetchall()]

        # --- Extra metrics ---
        metrics = {}
        for r in conn.execute(
            "SELECT metric_key, metric_value, metric_unit FROM fund_metrics_kv WHERE fund_id = ? AND reference_date = ?",
            (fund_id, date),
        ).fetchall():
            metrics[r["metric_key"]] = {"value": r["metric_value"], "unit": r["metric_unit"]}

        # --- Manager commentary ---
        commentaries = {}
        for r in conn.execute(
            "SELECT section, content FROM manager_commentary WHERE fund_id = ? AND reference_date = ?",
            (fund_id, date),
        ).fetchall():
            commentaries[r["section"]] = r["content"]

        # --- Distributions around this date ---
        distributions = [dict(r) for r in conn.execute(
            "SELECT * FROM distributions WHERE fund_id = ? ORDER BY reference_month",
            (fund_id,),
        ).fetchall()]

        return {
            "fund_id": fund_id,
            "ticker": ticker.upper(),
            "date": date,
            "fund_info": fund_info,
            "snapshot": snapshot,
            "transmission": transmission,
            "generation": generation,
            "port": port,
            "holdings": holdings,
            "metrics": metrics,
            "commentaries": commentaries,
            "distributions": distributions,
        }
    finally:
        conn.close()


@router.get("/portfolio/{ticker}/evolution")
def get_evolution(ticker: str):
    """Full time series for fund-level and asset-level metrics."""
    fund_id = ticker.lower()
    conn = get_db()
    try:
        # Fund-level
        fund_series = [dict(r) for r in conn.execute("""
            SELECT reference_date, nav_total, nav_per_unit, market_price,
                   discount_premium_pct, num_investors, avg_daily_volume,
                   return_month_pct, distribution_per_unit, irr_real,
                   dividend_yield_ltm, distribution_total_accum
            FROM monthly_snapshots WHERE fund_id = ?
            ORDER BY reference_date
        """, (fund_id,)).fetchall()]

        # Transmission evolution
        transmission = {}
        for r in conn.execute("""
            SELECT reference_date, asset_name, availability_pct, rap_annual_brl,
                   extension_km, ebitda_brl, revenue_brl
            FROM ops_transmission WHERE fund_id = ? ORDER BY reference_date
        """, (fund_id,)).fetchall():
            name = r["asset_name"]
            if name not in transmission:
                transmission[name] = []
            transmission[name].append(dict(r))

        # Generation evolution
        generation = {}
        for r in conn.execute("""
            SELECT reference_date, asset_name, gen_type, generation_mwm,
                   availability_pct, curtailment_mwm, capacity_mw
            FROM ops_generation WHERE fund_id = ? ORDER BY reference_date
        """, (fund_id,)).fetchall():
            name = r["asset_name"]
            if name not in generation:
                generation[name] = {"gen_type": r["gen_type"], "data": []}
            generation[name]["data"].append(dict(r))

        # Port evolution
        port = {}
        for r in conn.execute("""
            SELECT reference_date, asset_name, teus_month, ebitda_brl,
                   ebitda_margin_pct, net_debt_ebitda, revenue_brl, net_income_brl
            FROM ops_port WHERE fund_id = ? ORDER BY reference_date
        """, (fund_id,)).fetchall():
            name = r["asset_name"]
            if name not in port:
                port[name] = []
            port[name].append(dict(r))

        # Extra metrics evolution
        metrics_series = {}
        for r in conn.execute("""
            SELECT reference_date, metric_key, metric_value, metric_unit
            FROM fund_metrics_kv WHERE fund_id = ? ORDER BY reference_date
        """, (fund_id,)).fetchall():
            key = r["metric_key"]
            if key not in metrics_series:
                metrics_series[key] = {"unit": r["metric_unit"], "data": []}
            metrics_series[key]["data"].append({
                "date": r["reference_date"], "value": r["metric_value"],
            })

        return {
            "fund_id": fund_id,
            "ticker": ticker.upper(),
            "fund_series": fund_series,
            "transmission": transmission,
            "generation": generation,
            "port": port,
            "metrics_series": metrics_series,
        }
    finally:
        conn.close()
