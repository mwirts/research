"""Download and store daily market data (prices + dividends) for FIP-IE funds.

Sources:
  - Prices (OHLCV): Yahoo Finance via yfinance (primary)
  - Prices fallback: B3 COTAHIST annual files (for tickers missing on Yahoo)
  - Dividends: Yahoo Finance (ex-date + amount)

Storage: DuckDB at data/processed/market_data.ddb

Usage:
    python -m etl.downloader.market_data --all
    python -m etl.downloader.market_data --fund PFIN11
    python -m etl.downloader.market_data --update  # incremental update
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import yfinance as yf

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "processed" / "market_data.ddb"

# Fund registry
FUNDS = {
    "PFIN11": {"name": "Perfin Apollo Energia FIP-IE", "segment": "transmissao"},
    "AZIN11": {"name": "AZ Quest Infra-Yield II FIP-IE", "segment": "credito_infra"},
    "PPEI11": {"name": "Prisma Proton Energia FIP-IE", "segment": "geracao_solar"},
    "VIGT11": {"name": "Vinci Energia FIP-IE", "segment": "energia_mista"},
    "PICE11": {"name": "Patria Infra Energia Core FIP-IE", "segment": "geracao_eolica"},
    "BRZP11": {"name": "BRZ Infra Portos FIP-IE", "segment": "portuario"},
}

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_prices (
    ticker      VARCHAR NOT NULL,
    trade_date  DATE NOT NULL,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    source      VARCHAR DEFAULT 'yfinance',
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS dividends (
    ticker        VARCHAR NOT NULL,
    ex_date       DATE NOT NULL,
    record_date   DATE,
    payment_date  DATE,
    amount        DOUBLE NOT NULL,
    type          VARCHAR,  -- 'dividendo', 'amortizacao', 'jcp', 'rendimento'
    source        VARCHAR DEFAULT 'yfinance',
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, ex_date, amount)
);

CREATE TABLE IF NOT EXISTS fund_registry (
    ticker      VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    segment     VARCHAR,
    yf_ticker   VARCHAR,
    first_trade DATE,
    last_update TIMESTAMP
);
"""


def init_db() -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB database with schema."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))
    conn.execute(DB_SCHEMA)

    # Seed fund registry
    for ticker, info in FUNDS.items():
        conn.execute("""
            INSERT OR IGNORE INTO fund_registry (ticker, name, segment, yf_ticker)
            VALUES (?, ?, ?, ?)
        """, [ticker, info["name"], info["segment"], f"{ticker}.SA"])

    return conn


def _download_prices_yfinance(ticker: str, start: str = None, full: bool = False):
    """Try to download prices from Yahoo Finance.

    Returns:
        pandas DataFrame with OHLCV columns, or None if no data.
    """
    yf_ticker = f"{ticker}.SA"
    yf_obj = yf.Ticker(yf_ticker)

    if full:
        logger.info("%s: Downloading full price history from Yahoo Finance", ticker)
        hist = None
        for period in ["max", "5y", "2y", "1y", "6mo", "3mo", "1mo", "5d"]:
            try:
                hist = yf_obj.history(period=period, auto_adjust=False)
                if not hist.empty:
                    break
            except Exception:
                continue
        if hist is None or hist.empty:
            return None
        return hist
    else:
        end = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            hist = yf_obj.history(start=start, end=end, auto_adjust=False)
        except Exception:
            hist = yf_obj.history(period="5d", auto_adjust=False)
        return hist if not hist.empty else None


def _download_prices_b3_cotahist(ticker: str, start: str = None, full: bool = False) -> int:
    """Download prices from B3 COTAHIST annual files (fallback source).

    Returns:
        List of dicts with keys: trade_date, open, high, low, close, volume.
    """
    from etl.downloader.b3_cotahist import download_ticker_history

    if full or not start:
        records = download_ticker_history(ticker, find_all=True, cache=True)
    else:
        # Only download the year(s) we need
        start_year = int(start[:4])
        current_year = date.today().year
        years = list(range(start_year, current_year + 1))
        records = download_ticker_history(ticker, years=years, cache=True)

    if start and records:
        start_date = date.fromisoformat(start)
        records = [r for r in records if r.trade_date >= start_date]

    return [
        {
            "trade_date": r.trade_date,
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.total_qty,
        }
        for r in records
    ]


def download_prices(conn: duckdb.DuckDBPyConnection, ticker: str, start: str = None, full: bool = False) -> int:
    """Download daily prices and store in DuckDB.

    Tries Yahoo Finance first. If yfinance returns no data, falls back to
    B3 COTAHIST annual files.

    Args:
        conn: DuckDB connection
        ticker: B3 ticker (e.g., 'PFIN11')
        start: Start date (YYYY-MM-DD). If None, downloads from last available date.
        full: If True, download full history regardless of existing data.

    Returns:
        Number of rows inserted/updated.
    """
    if not full and not start:
        result = conn.execute(
            "SELECT MAX(trade_date) FROM daily_prices WHERE ticker = ?", [ticker]
        ).fetchone()
        last_date = result[0] if result[0] else None

        if last_date:
            start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info("%s: Updating from %s", ticker, start)
        else:
            full = True

    # --- Try Yahoo Finance first ---
    source = "yfinance"
    hist = _download_prices_yfinance(ticker, start=start, full=full)

    if hist is not None and not hist.empty:
        rows_inserted = 0
        for idx, row in hist.iterrows():
            trade_date = idx.date() if hasattr(idx, 'date') else idx
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO daily_prices
                        (ticker, trade_date, open, high, low, close, volume, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    ticker,
                    trade_date,
                    float(row.get("Open", 0)) if row.get("Open") else None,
                    float(row.get("High", 0)) if row.get("High") else None,
                    float(row.get("Low", 0)) if row.get("Low") else None,
                    float(row.get("Close", 0)) if row.get("Close") else None,
                    int(row.get("Volume", 0)) if row.get("Volume") else None,
                    source,
                ])
                rows_inserted += 1
            except Exception as e:
                logger.warning("%s: Failed to insert price for %s: %s", ticker, trade_date, e)
    else:
        # --- Fallback: B3 COTAHIST ---
        logger.warning("%s: Yahoo Finance returned no data, falling back to B3 COTAHIST", ticker)
        source = "b3_cotahist"
        b3_records = _download_prices_b3_cotahist(ticker, start=start, full=full)

        if not b3_records:
            logger.warning("%s: No price data from B3 COTAHIST either", ticker)
            return 0

        rows_inserted = 0
        for rec in b3_records:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO daily_prices
                        (ticker, trade_date, open, high, low, close, volume, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    ticker,
                    rec["trade_date"],
                    rec["open"],
                    rec["high"],
                    rec["low"],
                    rec["close"],
                    rec["volume"],
                    source,
                ])
                rows_inserted += 1
            except Exception as e:
                logger.warning("%s: Failed to insert B3 price for %s: %s",
                               ticker, rec["trade_date"], e)

    # Update registry
    conn.execute("""
        UPDATE fund_registry SET last_update = CURRENT_TIMESTAMP,
        first_trade = COALESCE(first_trade, (SELECT MIN(trade_date) FROM daily_prices WHERE ticker = ?))
        WHERE ticker = ?
    """, [ticker, ticker])

    logger.info("%s: Inserted/updated %d price records (source: %s)", ticker, rows_inserted, source)
    return rows_inserted


def download_dividends(conn: duckdb.DuckDBPyConnection, ticker: str) -> int:
    """Download dividend history and store in DuckDB.

    Tries Yahoo Finance first. If empty, falls back to PDF-parsed data in SQLite.

    Returns:
        Number of rows inserted/updated.
    """
    rows_inserted = 0

    # --- Try Yahoo Finance ---
    yf_ticker = f"{ticker}.SA"
    try:
        divs = yf.Ticker(yf_ticker).dividends
        if not divs.empty:
            for ex_date, amount in divs.items():
                ex_date_val = ex_date.date() if hasattr(ex_date, 'date') else ex_date
                if float(amount) <= 0:
                    continue
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO dividends (ticker, ex_date, amount, source)
                        VALUES (?, ?, ?, 'yfinance')
                    """, [ticker, ex_date_val, float(amount)])
                    rows_inserted += 1
                except Exception as e:
                    logger.warning("%s: Failed to insert yf dividend: %s", ticker, e)
    except Exception:
        pass

    # --- Fallback: PDF-parsed data from SQLite ---
    if rows_inserted == 0:
        rows_inserted = _sync_dividends_from_sqlite(conn, ticker)

    logger.info("%s: Inserted %d dividend records", ticker, rows_inserted)
    return rows_inserted


def _sync_dividends_from_sqlite(conn: duckdb.DuckDBPyConnection, ticker: str) -> int:
    """Sync dividends from SQLite (PDF-parsed) to DuckDB as fallback."""
    import sqlite3 as sqlite3_mod

    sqlite_path = PROJECT_ROOT / "data" / "processed" / "fund_data.db"
    if not sqlite_path.exists():
        return 0

    sql = sqlite3_mod.connect(str(sqlite_path))
    sql.row_factory = sqlite3_mod.Row
    rows = sql.execute(
        "SELECT * FROM distributions WHERE fund_id = ? ORDER BY reference_month",
        (ticker.lower(),),
    ).fetchall()
    sql.close()

    if not rows:
        return 0

    logger.info("%s: Syncing %d dividends from PDF reports (yfinance had none)", ticker, len(rows))
    today = date.today()
    inserted = 0

    for r in rows:
        ex = r["ex_date"]
        if not ex:
            continue
        # Only sync past distributions, not future projections
        try:
            ex_date = date.fromisoformat(ex)
        except ValueError:
            continue
        if ex_date > today:
            continue

        try:
            conn.execute("""
                INSERT OR IGNORE INTO dividends
                    (ticker, ex_date, payment_date, amount, type, source)
                VALUES (?, ?, ?, ?, 'rendimento', 'pdf_report')
            """, [ticker, ex, r["payment_date"], r["amount_per_unit"]])
            inserted += 1
        except Exception:
            pass

    return inserted


def download_fund(conn: duckdb.DuckDBPyConnection, ticker: str, full: bool = False) -> dict:
    """Download all data for a fund."""
    stats = {"ticker": ticker, "prices": 0, "dividends": 0, "errors": []}

    try:
        stats["prices"] = download_prices(conn, ticker, full=full)
    except Exception as e:
        logger.error("%s: Price download failed: %s", ticker, e)
        stats["errors"].append(f"prices: {e}")

    try:
        stats["dividends"] = download_dividends(conn, ticker)
    except Exception as e:
        logger.error("%s: Dividend download failed: %s", ticker, e)
        stats["errors"].append(f"dividends: {e}")

    return stats


def print_summary(conn: duckdb.DuckDBPyConnection):
    """Print a summary of data in the database."""
    print("\n=== Market Data Summary ===")

    prices = conn.execute("""
        SELECT ticker,
               COUNT(*) as rows,
               MIN(trade_date) as first_date,
               MAX(trade_date) as last_date,
               LAST(close ORDER BY trade_date) as last_close
        FROM daily_prices
        GROUP BY ticker
        ORDER BY ticker
    """).fetchall()

    print(f"\n{'Ticker':>8s} {'Rows':>6s} {'First':>12s} {'Last':>12s} {'Close':>8s}")
    print("-" * 50)
    for row in prices:
        print(f"{row[0]:>8s} {row[1]:>6d} {str(row[2]):>12s} {str(row[3]):>12s} R${row[4]:>6.2f}")

    divs = conn.execute("""
        SELECT ticker, COUNT(*) as rows, SUM(amount) as total
        FROM dividends
        GROUP BY ticker
        ORDER BY ticker
    """).fetchall()

    print(f"\n{'Ticker':>8s} {'Events':>6s} {'Total R$/cota':>14s}")
    print("-" * 32)
    for row in divs:
        print(f"{row[0]:>8s} {row[1]:>6d} R${row[2]:>12.2f}")


def main():
    parser = argparse.ArgumentParser(description="FIP-IE Market Data Downloader")
    parser.add_argument("--fund", type=str, help="Download a specific fund (e.g., PFIN11)")
    parser.add_argument("--all", action="store_true", help="Download all funds")
    parser.add_argument("--update", action="store_true", help="Incremental update (only new data)")
    parser.add_argument("--full", action="store_true", help="Full history download")
    parser.add_argument("--summary", action="store_true", help="Print database summary")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = init_db()

    if args.summary:
        print_summary(conn)
        conn.close()
        return

    full = args.full and not args.update

    if args.fund:
        ticker = args.fund.upper()
        if ticker not in FUNDS:
            logger.error("Unknown fund: %s. Available: %s", ticker, ", ".join(FUNDS))
            sys.exit(1)
        stats = download_fund(conn, ticker, full=full)
        print(f"\n{ticker}: {stats['prices']} prices, {stats['dividends']} dividends")

    elif args.all or args.update:
        all_stats = []
        for ticker in FUNDS:
            stats = download_fund(conn, ticker, full=full)
            all_stats.append(stats)

        print("\n=== Download Summary ===")
        for s in all_stats:
            status = "OK" if not s["errors"] else f"ERRORS: {', '.join(s['errors'])}"
            print(f"  {s['ticker']:>8s}: {s['prices']:>5d} prices, {s['dividends']:>3d} dividends  [{status}]")

    else:
        parser.print_help()
        sys.exit(1)

    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
