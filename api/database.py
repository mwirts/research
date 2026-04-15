"""Database connection helpers."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQLITE_PATH = PROJECT_ROOT / "data" / "processed" / "fund_data.db"
DUCKDB_PATH = PROJECT_ROOT / "data" / "processed" / "market_data.ddb"


def get_db() -> sqlite3.Connection:
    """Get a SQLite connection (fund report data) with row factory."""
    conn = sqlite3.connect(str(SQLITE_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_market_db() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection (market prices, dividends, TIR)."""
    return duckdb.connect(str(DUCKDB_PATH))
