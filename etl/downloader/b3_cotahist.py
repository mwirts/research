"""Download and parse B3 COTAHIST historical price data.

Source: B3 annual historical files (COTAHIST)
URL: https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{YYYY}.ZIP

Each ZIP contains a fixed-width TXT file with daily OHLCV data for all tickers
traded on B3. This is the definitive source for historical prices when Yahoo
Finance lacks data for a ticker.

Layout reference:
  https://www.b3.com.br/data/files/33/67/B9/50/D84057102C784E47AC094EA8/SeriesHistoricas_Layout.pdf

Fixed-width columns (0-indexed):
  00-01  TIPREG    Record type (00=header, 01=data, 99=trailer)
  02-09  DATA      Trade date YYYYMMDD
  10-11  CODBDI    BDI code (instrument type table)
  12-23  CODNEG    Ticker symbol (12 chars, left-aligned, space-padded)
  24-26  TPMERC    Market type code
  27-38  NOMRES    Short company/fund name
  39-48  ESPECI    Specification
  49-51  PRAZOT    Forward market deadline (blank for spot)
  52-55  MODREF    Reference currency ("R$  ")
  56-68  PREABE    Opening price (13 digits, last 2 decimal)
  69-81  PREMAX    High price
  82-94  PREMIN    Low price
  95-107 PREMED    Average price
  108-120 PREULT   Closing price
  121-133 PREOFC   Best buy offer
  134-146 PREOFV   Best sell offer
  147-151 TOTNEG   Number of trades (5 digits)
  152-169 QUATOT   Total quantity traded (18 digits)
  170-187 VOLTOT   Total volume BRL (18 digits, last 2 decimal)

Usage:
    python -m etl.downloader.b3_cotahist --ticker VIGT11
    python -m etl.downloader.b3_cotahist --ticker VIGT11 --years 2024 2025 2026
    python -m etl.downloader.b3_cotahist --ticker VIGT11 --all-years
    python -m etl.downloader.b3_cotahist --ticker VIGT11 --store  # save to DuckDB
"""

from __future__ import annotations

import argparse
import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

COTAHIST_BASE_URL = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "b3_cotahist"


@dataclass
class CotahistRecord:
    """One daily OHLCV record from COTAHIST."""

    ticker: str
    trade_date: date
    open: float
    high: float
    low: float
    avg: float
    close: float
    best_bid: float
    best_ask: float
    num_trades: int
    total_qty: int
    volume: float  # total volume in BRL
    bdi_code: str
    market_type: str
    short_name: str
    isin: str


def parse_cotahist_line(line: str) -> Optional[CotahistRecord]:
    """Parse a single COTAHIST fixed-width line into a CotahistRecord.

    Returns None for header/trailer lines or lines that are too short.
    """
    if len(line) < 188:
        return None
    if line[0:2] != "01":
        return None

    try:
        dt_str = line[2:10]
        trade_date = date(int(dt_str[:4]), int(dt_str[4:6]), int(dt_str[6:8]))

        return CotahistRecord(
            ticker=line[12:24].strip(),
            trade_date=trade_date,
            open=int(line[56:69]) / 100,
            high=int(line[69:82]) / 100,
            low=int(line[82:95]) / 100,
            avg=int(line[95:108]) / 100,
            close=int(line[108:121]) / 100,
            best_bid=int(line[121:134]) / 100,
            best_ask=int(line[134:147]) / 100,
            num_trades=int(line[147:152]),
            total_qty=int(line[152:170]),
            volume=int(line[170:188]) / 100,
            bdi_code=line[10:12].strip(),
            market_type=line[24:27].strip(),
            short_name=line[27:39].strip(),
            isin=line[188:200].strip() if len(line) >= 200 else "",
        )
    except (ValueError, IndexError) as e:
        logger.warning("Failed to parse COTAHIST line: %s", e)
        return None


def download_cotahist_zip(year: int, cache: bool = True) -> bytes:
    """Download a COTAHIST annual ZIP file.

    Args:
        year: The year to download (e.g. 2024).
        cache: If True, cache the ZIP locally under data/raw/b3_cotahist/.

    Returns:
        Raw ZIP bytes.

    Raises:
        HTTPError: If the file is not found (e.g. future year).
    """
    if cache:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path = CACHE_DIR / f"COTAHIST_A{year}.ZIP"
        if cache_path.exists():
            logger.info("Using cached file: %s", cache_path)
            return cache_path.read_bytes()

    url = f"{COTAHIST_BASE_URL}/COTAHIST_A{year}.ZIP"
    logger.info("Downloading %s ...", url)

    req = Request(url, headers={"User-Agent": USER_AGENT})
    resp = urlopen(req, timeout=180)
    data = resp.read()
    logger.info("Downloaded %s (%.1f MB)", url, len(data) / 1e6)

    if cache:
        cache_path = CACHE_DIR / f"COTAHIST_A{year}.ZIP"
        cache_path.write_bytes(data)
        logger.info("Cached to %s", cache_path)

    return data


def extract_ticker_from_zip(zip_data: bytes, ticker: str) -> list[CotahistRecord]:
    """Extract all records for a given ticker from a COTAHIST ZIP.

    Args:
        zip_data: Raw ZIP file bytes.
        ticker: B3 ticker to filter (e.g. 'VIGT11').

    Returns:
        List of CotahistRecord sorted by trade_date.
    """
    ticker_upper = ticker.upper()
    zf = zipfile.ZipFile(io.BytesIO(zip_data))
    txt_names = [n for n in zf.namelist() if n.upper().endswith(".TXT")]
    if not txt_names:
        logger.warning("No TXT file found in ZIP")
        return []

    content = zf.read(txt_names[0]).decode("latin-1")
    records = []

    for line in content.split("\n"):
        if len(line) < 24:
            continue
        if line[12:24].strip() != ticker_upper:
            continue
        rec = parse_cotahist_line(line)
        if rec:
            records.append(rec)

    records.sort(key=lambda r: r.trade_date)
    return records


def download_ticker_history(
    ticker: str,
    years: list[int] | None = None,
    find_all: bool = False,
    cache: bool = True,
) -> list[CotahistRecord]:
    """Download complete price history for a ticker from B3 COTAHIST files.

    Args:
        ticker: B3 ticker (e.g. 'VIGT11').
        years: Specific years to download. If None and find_all is False,
               downloads current year only.
        find_all: If True, scan backwards from current year until no data
                  is found, to get the complete history.
        cache: Cache ZIP files locally.

    Returns:
        List of CotahistRecord sorted by trade_date.
    """
    all_records: list[CotahistRecord] = []
    current_year = date.today().year

    if years:
        target_years = sorted(years)
    elif find_all:
        target_years = list(range(current_year, 2017, -1))  # Scan back to 2018
    else:
        target_years = [current_year]

    for year in target_years:
        try:
            zip_data = download_cotahist_zip(year, cache=cache)
            records = extract_ticker_from_zip(zip_data, ticker)
            logger.info("%s %d: %d trading days found", ticker, year, len(records))

            if records:
                all_records.extend(records)
            elif find_all and year < current_year:
                # No data for this year and we're scanning backwards - stop
                logger.info("%s: No data in %d, stopping backwards scan", ticker, year)
                break
        except HTTPError as e:
            if e.code == 404:
                logger.warning("COTAHIST file not available for %d", year)
            else:
                raise

    all_records.sort(key=lambda r: r.trade_date)
    return all_records


def store_to_duckdb(records: list[CotahistRecord], db_path: Path | None = None) -> int:
    """Store COTAHIST records into the market_data DuckDB database.

    Uses the same schema as market_data.py (daily_prices table), with
    source='b3_cotahist'.

    Args:
        records: List of CotahistRecord to store.
        db_path: Path to DuckDB file. Defaults to data/processed/market_data.ddb.

    Returns:
        Number of rows inserted/updated.
    """
    import duckdb

    if db_path is None:
        db_path = PROJECT_ROOT / "data" / "processed" / "market_data.ddb"

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    # Ensure table exists (same schema as market_data.py)
    conn.execute("""
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
        )
    """)

    rows = 0
    for rec in records:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO daily_prices
                    (ticker, trade_date, open, high, low, close, volume, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'b3_cotahist', CURRENT_TIMESTAMP)
            """, [
                rec.ticker,
                rec.trade_date,
                rec.open,
                rec.high,
                rec.low,
                rec.close,
                rec.total_qty,
            ])
            rows += 1
        except Exception as e:
            logger.warning("Failed to insert %s %s: %s", rec.ticker, rec.trade_date, e)

    conn.close()
    logger.info("Stored %d records to %s", rows, db_path)
    return rows


def print_records(records: list[CotahistRecord], max_rows: int = 0):
    """Pretty-print COTAHIST records."""
    if not records:
        print("No records found.")
        return

    print(f"\n{'Date':<12} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} "
          f"{'Volume (R$)':>15} {'Trades':>7} {'Qty':>10}")
    print("-" * 85)

    display = records if max_rows == 0 else records[:max_rows]
    for r in display:
        print(f"{r.trade_date!s:<12} {r.open:>8.2f} {r.high:>8.2f} {r.low:>8.2f} "
              f"{r.close:>8.2f} {r.volume:>15,.2f} {r.num_trades:>7} {r.total_qty:>10}")

    if max_rows and len(records) > max_rows:
        print(f"  ... ({len(records) - max_rows} more rows)")

    print(f"\nTotal: {len(records)} trading days")
    print(f"Date range: {records[0].trade_date} to {records[-1].trade_date}")
    print(f"Price range: R${min(r.low for r in records):.2f} - R${max(r.high for r in records):.2f}")
    print(f"Last close: R${records[-1].close:.2f}")


def main():
    parser = argparse.ArgumentParser(
        description="Download B3 COTAHIST historical price data"
    )
    parser.add_argument("--ticker", required=True, help="B3 ticker (e.g. VIGT11)")
    parser.add_argument("--years", nargs="+", type=int, help="Specific years to download")
    parser.add_argument("--all-years", action="store_true",
                        help="Scan backwards to find all available history")
    parser.add_argument("--store", action="store_true",
                        help="Store results in DuckDB (data/processed/market_data.ddb)")
    parser.add_argument("--no-cache", action="store_true",
                        help="Do not cache downloaded ZIP files")
    parser.add_argument("--max-rows", type=int, default=30,
                        help="Max rows to display (0=all)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    records = download_ticker_history(
        ticker=args.ticker,
        years=args.years,
        find_all=args.all_years,
        cache=not args.no_cache,
    )

    print_records(records, max_rows=args.max_rows)

    if args.store:
        count = store_to_duckdb(records)
        print(f"\nStored {count} records to DuckDB")


if __name__ == "__main__":
    main()
