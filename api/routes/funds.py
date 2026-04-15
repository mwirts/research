"""Fund data API endpoints - unified from SQLite (reports) + DuckDB (market)."""

from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, HTTPException

from api.database import get_db, get_market_db

router = APIRouter(tags=["funds"])

FUND_REGISTRY = {
    "PFIN11": {"name": "Perfin Apollo Energia FIP-IE", "manager": "Perfin", "segment": "transmissao"},
    "AZIN11": {"name": "AZ Quest Infra-Yield II FIP-IE", "manager": "AZ Quest", "segment": "credito_infra"},
    "PPEI11": {"name": "Prisma Proton Energia FIP-IE", "manager": "Prisma", "segment": "geracao_solar"},
    "VIGT11": {"name": "Vinci Energia FIP-IE", "manager": "Vinci", "segment": "energia_mista"},
    "PICE11": {"name": "Patria Infra Energia Core FIP-IE", "manager": "Patria", "segment": "geracao_eolica"},
    "BRZP11": {"name": "BRZ Infra Portos FIP-IE", "manager": "BRZ", "segment": "portuario"},
}


def _enrich_from_duckdb(results: dict) -> None:
    """Enrich fund data with market prices, dividends, DY, and TIR from DuckDB."""
    try:
        mkt = get_market_db()
    except Exception:
        return

    today = date.today()
    one_year_ago = today - timedelta(days=365)

    # Latest prices
    for ticker in list(results.keys()):
        row = mkt.execute("""
            SELECT trade_date, close, volume
            FROM daily_prices WHERE ticker = ?
            ORDER BY trade_date DESC LIMIT 1
        """, [ticker]).fetchone()
        if row:
            results[ticker]["market_price"] = round(row[1], 2)
            results[ticker]["market_date"] = str(row[0])
            results[ticker]["volume"] = row[2]

    # Dividends: totals + LTM for DY
    for ticker in list(results.keys()):
        # Total accumulated
        total = mkt.execute(
            "SELECT COUNT(*), SUM(amount) FROM dividends WHERE ticker = ?", [ticker]
        ).fetchone()
        if total and total[0] > 0:
            results[ticker]["div_events"] = total[0]
            results[ticker]["div_total"] = round(total[1], 2)

        # LTM dividends (last 12 months) for DY calculation
        ltm = mkt.execute("""
            SELECT SUM(amount) FROM dividends
            WHERE ticker = ? AND ex_date >= ?
        """, [ticker, one_year_ago]).fetchone()
        ltm_amount = ltm[0] if ltm and ltm[0] else 0

        price = results[ticker].get("market_price")
        if price and price > 0 and ltm_amount > 0:
            results[ticker]["dividend_yield_ltm"] = round(ltm_amount / price * 100, 2)
            results[ticker]["div_ltm_amount"] = round(ltm_amount, 2)

        # Last dividend
        last = mkt.execute("""
            SELECT amount, ex_date FROM dividends
            WHERE ticker = ? ORDER BY ex_date DESC LIMIT 1
        """, [ticker]).fetchone()
        if last:
            results[ticker]["div_last_amount"] = round(last[0], 2)
            results[ticker]["div_last_date"] = str(last[1])

    # TIR from breakpoints
    for ticker in list(results.keys()):
        price = results[ticker].get("market_price")
        if not price:
            continue
        bps = mkt.execute("""
            SELECT cota_min, cota_max, ipca_plus_start, ipca_plus_end
            FROM tir_breakpoints
            WHERE ticker = ? AND scraped_at = (
                SELECT MAX(scraped_at) FROM tir_breakpoints WHERE ticker = ?
            )
            ORDER BY cota_min
        """, [ticker, ticker]).fetchall()
        for bp in bps:
            if bp[0] <= price <= bp[1]:
                slope = (bp[3] - bp[2]) / (bp[1] - bp[0])
                ipca = bp[2] + slope * (price - bp[0])
                results[ticker]["irr_real"] = round(ipca * 100, 2)
                break

    mkt.close()


def _enrich_from_sqlite(results: dict) -> None:
    """Enrich fund data with report-derived data (NAV, investors, etc) from SQLite."""
    try:
        sql = get_db()
    except Exception:
        return

    for row in sql.execute("""
        SELECT f.fund_id, f.ticker,
               s.reference_date, s.nav_total, s.nav_per_unit,
               s.market_cap, s.avg_daily_volume, s.num_investors, s.pct_retail,
               s.return_month_pct, s.distribution_total_accum
        FROM funds f
        LEFT JOIN monthly_snapshots s ON f.fund_id = s.fund_id
            AND s.reference_date = (
                SELECT MAX(s2.reference_date) FROM monthly_snapshots s2
                WHERE s2.fund_id = f.fund_id
            )
    """).fetchall():
        ticker = row["ticker"]
        if ticker not in results:
            continue
        r = results[ticker]
        r["report_date"] = row["reference_date"]
        r["nav_total"] = row["nav_total"]
        r["nav_per_unit"] = row["nav_per_unit"]
        r["market_cap"] = row["market_cap"]
        r["avg_daily_volume"] = row["avg_daily_volume"]
        r["num_investors"] = row["num_investors"]
        r["pct_retail"] = row["pct_retail"]
        r["return_month_pct"] = row["return_month_pct"]
        if not r.get("div_total") and row["distribution_total_accum"]:
            r["div_total"] = row["distribution_total_accum"]

    sql.close()


@router.get("/funds")
def list_funds():
    """List all funds with latest data from market + reports."""
    results = {}
    for ticker, info in FUND_REGISTRY.items():
        results[ticker] = {
            "fund_id": ticker.lower(),
            "ticker": ticker,
            "fund_name": info["name"],
            "manager": info["manager"],
            "segment": info["segment"],
            "market_price": None,
            "market_date": None,
            "volume": None,
            "nav_total": None,
            "nav_per_unit": None,
            "market_cap": None,
            "discount_premium_pct": None,
            "price_to_book": None,
            "irr_real": None,
            "dividend_yield_ltm": None,
            "div_ltm_amount": None,
            "div_total": None,
            "div_events": None,
            "div_last_amount": None,
            "div_last_date": None,
            "avg_daily_volume": None,
            "num_investors": None,
            "pct_retail": None,
            "return_month_pct": None,
            "report_date": None,
        }

    _enrich_from_duckdb(results)
    _enrich_from_sqlite(results)

    # Compute discount where we have both market price and NAV
    for r in results.values():
        if r["market_price"] and r["nav_per_unit"] and r["nav_per_unit"] > 0:
            r["discount_premium_pct"] = round(
                (r["market_price"] - r["nav_per_unit"]) / r["nav_per_unit"] * 100, 2
            )
            r["price_to_book"] = round(r["market_price"] / r["nav_per_unit"], 2)

    return sorted(results.values(), key=lambda f: f["ticker"])


@router.get("/funds/{ticker}")
def get_fund(ticker: str):
    """Get full fund detail with time series from both sources."""
    ticker = ticker.upper()
    fund_id = ticker.lower()

    if ticker not in FUND_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")

    info = FUND_REGISTRY[ticker]
    result = {
        "fund": {
            "ticker": ticker,
            "fund_id": fund_id,
            "fund_name": info["name"],
            "manager": info["manager"],
            "segment": info["segment"],
        },
        "price_history": [],
        "dividends": [],
        "snapshots": [],
        "commentaries": [],
        "tir": None,
    }

    # Market data from DuckDB
    try:
        mkt = get_market_db()

        prices = mkt.execute("""
            SELECT trade_date, open, high, low, close, volume
            FROM daily_prices WHERE ticker = ?
            ORDER BY trade_date
        """, [ticker]).fetchall()
        result["price_history"] = [
            {"date": str(r[0]), "open": round(r[1], 2) if r[1] else None,
             "high": round(r[2], 2) if r[2] else None,
             "low": round(r[3], 2) if r[3] else None,
             "close": round(r[4], 2), "volume": r[5]}
            for r in prices
        ]

        divs = mkt.execute("""
            SELECT ex_date, record_date, payment_date, amount, type
            FROM dividends WHERE ticker = ?
            ORDER BY ex_date
        """, [ticker]).fetchall()
        result["dividends"] = [
            {"ex_date": str(r[0]),
             "record_date": str(r[1]) if r[1] else None,
             "payment_date": str(r[2]) if r[2] else None,
             "amount": round(r[3], 4), "type": r[4]}
            for r in divs
        ]

        # TIR at latest price
        if prices:
            latest_price = prices[-1][4]
            bps = mkt.execute("""
                SELECT cota_min, cota_max, ipca_plus_start, ipca_plus_end
                FROM tir_breakpoints
                WHERE ticker = ? AND scraped_at = (
                    SELECT MAX(scraped_at) FROM tir_breakpoints WHERE ticker = ?
                )
                ORDER BY cota_min
            """, [ticker, ticker]).fetchall()
            for bp in bps:
                if bp[0] <= latest_price <= bp[1]:
                    slope = (bp[3] - bp[2]) / (bp[1] - bp[0])
                    ipca = bp[2] + slope * (latest_price - bp[0])
                    gross = ((1 + ipca) * 1.035 - 1) / 0.85
                    result["tir"] = {
                        "cota": round(latest_price, 2),
                        "ipca_plus_pct": round(ipca * 100, 2),
                        "gross_up_pct": round(gross * 100, 2),
                    }
                    break

        mkt.close()
    except Exception:
        pass

    # Report data from SQLite
    try:
        sql = get_db()
        snapshots = sql.execute("""
            SELECT * FROM monthly_snapshots WHERE fund_id = ? ORDER BY reference_date
        """, (fund_id,)).fetchall()
        result["snapshots"] = [dict(s) for s in snapshots]

        commentaries = sql.execute("""
            SELECT * FROM manager_commentary WHERE fund_id = ?
            ORDER BY reference_date DESC LIMIT 5
        """, (fund_id,)).fetchall()
        result["commentaries"] = [dict(c) for c in commentaries]
        sql.close()
    except Exception:
        pass

    return result
