"""Market data API endpoints (prices, dividends, TIR)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.database import get_market_db

router = APIRouter(tags=["market"])


# --- Aggregate endpoints (MUST come before parameterized routes) ---

@router.get("/market/prices/latest")
def get_latest_prices():
    """Get the latest closing price for all funds."""
    conn = get_market_db()
    try:
        tickers = [r[0] for r in conn.execute(
            "SELECT DISTINCT ticker FROM daily_prices ORDER BY ticker"
        ).fetchall()]

        results = []
        for ticker in tickers:
            row = conn.execute("""
                SELECT trade_date, close, volume
                FROM daily_prices WHERE ticker = ?
                ORDER BY trade_date DESC LIMIT 1
            """, [ticker]).fetchone()
            if row:
                info = conn.execute(
                    "SELECT name, segment FROM fund_registry WHERE ticker = ?", [ticker]
                ).fetchone()
                results.append({
                    "ticker": ticker,
                    "date": str(row[0]),
                    "close": round(row[1], 2),
                    "volume": row[2],
                    "name": info[0] if info else "",
                    "segment": info[1] if info else "",
                })
        return results
    finally:
        conn.close()


@router.get("/market/dividends/summary")
def get_dividends_summary():
    """Get dividend summary for all funds."""
    conn = get_market_db()
    try:
        tickers = [r[0] for r in conn.execute(
            "SELECT DISTINCT ticker FROM dividends ORDER BY ticker"
        ).fetchall()]

        results = []
        for ticker in tickers:
            stats = conn.execute("""
                SELECT COUNT(*), SUM(amount), MAX(ex_date)
                FROM dividends WHERE ticker = ?
            """, [ticker]).fetchone()
            last = conn.execute("""
                SELECT amount FROM dividends WHERE ticker = ?
                ORDER BY ex_date DESC LIMIT 1
            """, [ticker]).fetchone()
            results.append({
                "ticker": ticker,
                "num_events": stats[0],
                "total_amount": round(stats[1], 2),
                "last_ex_date": str(stats[2]),
                "last_amount": round(last[0], 2) if last else None,
            })
        return results
    finally:
        conn.close()


# --- Per-fund parameterized endpoints ---

@router.get("/market/prices/{ticker}")
def get_prices(
    ticker: str,
    period: str = Query("1y", description="1m, 3m, 6m, 1y, 3y, max"),
):
    """Get daily price history for a fund."""
    ticker = ticker.upper()
    conn = get_market_db()
    try:
        period_map = {
            "1m": "INTERVAL 1 MONTH",
            "3m": "INTERVAL 3 MONTH",
            "6m": "INTERVAL 6 MONTH",
            "1y": "INTERVAL 1 YEAR",
            "3y": "INTERVAL 3 YEAR",
            "5y": "INTERVAL 5 YEAR",
        }
        where_clause = "ticker = ?"
        params = [ticker]

        if period != "max" and period in period_map:
            where_clause += f" AND trade_date >= CURRENT_DATE - {period_map[period]}"

        rows = conn.execute(f"""
            SELECT trade_date, open, high, low, close, volume
            FROM daily_prices
            WHERE {where_clause}
            ORDER BY trade_date
        """, params).fetchall()

        return [
            {
                "date": str(r[0]),
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


@router.get("/market/dividends/{ticker}")
def get_dividends(ticker: str):
    """Get dividend history for a fund from market data."""
    ticker = ticker.upper()
    conn = get_market_db()
    try:
        rows = conn.execute("""
            SELECT ex_date, record_date, payment_date, amount, type, source
            FROM dividends
            WHERE ticker = ?
            ORDER BY ex_date
        """, [ticker]).fetchall()

        return [
            {
                "ex_date": str(r[0]),
                "record_date": str(r[1]) if r[1] else None,
                "payment_date": str(r[2]) if r[2] else None,
                "amount": r[3],
                "type": r[4],
                "source": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


@router.get("/market/tir/{ticker}")
def get_tir(
    ticker: str,
    cota: float = Query(None, description="Market price. If omitted, uses latest close."),
):
    """Calculate implied TIR for a fund at a given price."""
    ticker = ticker.upper()
    conn = get_market_db()
    try:
        if cota is None:
            result = conn.execute("""
                SELECT close FROM daily_prices
                WHERE ticker = ? ORDER BY trade_date DESC LIMIT 1
            """, [ticker]).fetchone()
            if not result:
                raise HTTPException(404, f"No price data for {ticker}")
            cota = result[0]

        breakpoints = conn.execute("""
            SELECT cota_min, cota_max, ipca_plus_start, ipca_plus_end
            FROM tir_breakpoints
            WHERE ticker = ? AND scraped_at = (
                SELECT MAX(scraped_at) FROM tir_breakpoints WHERE ticker = ?
            )
            ORDER BY cota_min
        """, [ticker, ticker]).fetchall()

        if not breakpoints:
            raise HTTPException(404, f"No TIR breakpoints for {ticker}. Run tir_calculator --update first.")

        params = {}
        for pr in conn.execute("""
            SELECT param_key, param_value FROM tir_parameters
            WHERE ticker = ? AND scraped_at = (
                SELECT MAX(scraped_at) FROM tir_parameters WHERE ticker = ?
            )
        """, [ticker, ticker]).fetchall():
            params[pr[0]] = pr[1]

        inflation = params.get("inflation", 0.035)
        tax_rate = params.get("tax_rate", 0.15)

        ipca_plus = None
        for bp in breakpoints:
            if bp[0] <= cota <= bp[1]:
                slope = (bp[3] - bp[2]) / (bp[1] - bp[0])
                ipca_plus = bp[2] + slope * (cota - bp[0])
                break

        if ipca_plus is None:
            return {
                "ticker": ticker,
                "cota": cota,
                "error": f"Cota R$ {cota:.2f} fora do range (R$ {breakpoints[0][0]:.2f} - R$ {breakpoints[-1][1]:.2f})",
            }

        gross_up = ((1 + ipca_plus) * (1 + inflation) - 1) / (1 - tax_rate)

        return {
            "ticker": ticker,
            "cota": round(cota, 2),
            "ipca_plus": round(ipca_plus, 5),
            "ipca_plus_pct": round(ipca_plus * 100, 2),
            "gross_up": round(gross_up, 5),
            "gross_up_pct": round(gross_up * 100, 2),
            "inflation": inflation,
            "tax_rate": tax_rate,
            "breakpoints_count": len(breakpoints),
        }
    finally:
        conn.close()
