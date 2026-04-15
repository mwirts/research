"""SQLite database schema creation and data loading for FIP-IE fund data."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from etl.transformer.models import (
    Distribution,
    ExtractionLogEntry,
    Fund,
    FundMetricKV,
    FundReport,
    GenerationAsset,
    ManagerCommentary,
    MonthlySnapshot,
    PortAsset,
    PortfolioHolding,
    TransmissionAsset,
)

logger = logging.getLogger(__name__)

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS funds (
    fund_id         TEXT PRIMARY KEY,
    ticker          TEXT NOT NULL UNIQUE,
    fund_name       TEXT NOT NULL,
    cnpj            TEXT,
    manager         TEXT NOT NULL,
    administrator   TEXT,
    segment         TEXT NOT NULL,
    inception_date  TEXT,
    fund_term       TEXT,
    target_audience TEXT,
    admin_fee_pct   REAL,
    mgmt_fee_pct    REAL,
    perf_fee_pct    REAL
);

CREATE TABLE IF NOT EXISTS monthly_snapshots (
    fund_id                  TEXT NOT NULL REFERENCES funds(fund_id),
    reference_date           TEXT NOT NULL,
    report_date              TEXT,
    nav_total                REAL,
    market_cap               REAL,
    nav_per_unit             REAL,
    market_price             REAL,
    units_outstanding        INTEGER,
    discount_premium_pct     REAL,
    price_to_book            REAL,
    irr_real                 REAL,
    irr_nominal              REAL,
    return_month_pct         REAL,
    return_ytd_pct           REAL,
    return_12m_pct           REAL,
    return_since_inception_pct REAL,
    distribution_per_unit    REAL,
    distribution_total_accum REAL,
    dividend_yield_ltm       REAL,
    avg_daily_volume         REAL,
    num_investors            INTEGER,
    pct_retail               REAL,
    source_pdf               TEXT NOT NULL,
    page_count               INTEGER,
    PRIMARY KEY (fund_id, reference_date)
);

CREATE TABLE IF NOT EXISTS distributions (
    fund_id          TEXT NOT NULL REFERENCES funds(fund_id),
    reference_month  TEXT NOT NULL,
    ex_date          TEXT,
    payment_date     TEXT,
    amount_per_unit  REAL NOT NULL,
    PRIMARY KEY (fund_id, reference_month)
);

CREATE TABLE IF NOT EXISTS portfolio_holdings (
    fund_id         TEXT NOT NULL REFERENCES funds(fund_id),
    reference_date  TEXT NOT NULL,
    issuer          TEXT NOT NULL,
    segment         TEXT,
    instrument_type TEXT,
    ticker          TEXT,
    pct_pl          REAL,
    amount_brl      REAL,
    duration_years  REAL,
    indexer         TEXT,
    spread_pct      REAL,
    ltv_pct         REAL,
    icsd_min        REAL,
    source_pdf      TEXT,
    PRIMARY KEY (fund_id, reference_date, issuer)
);

CREATE TABLE IF NOT EXISTS ops_transmission (
    fund_id          TEXT NOT NULL REFERENCES funds(fund_id),
    reference_date   TEXT NOT NULL,
    asset_name       TEXT NOT NULL,
    extension_km     REAL,
    rap_annual_brl   REAL,
    concession_end   TEXT,
    availability_pct REAL,
    ebitda_brl       REAL,
    ebitda_margin_pct REAL,
    revenue_brl      REAL,
    source_pdf       TEXT,
    PRIMARY KEY (fund_id, reference_date, asset_name)
);

CREATE TABLE IF NOT EXISTS ops_generation (
    fund_id             TEXT NOT NULL REFERENCES funds(fund_id),
    reference_date      TEXT NOT NULL,
    asset_name          TEXT NOT NULL,
    gen_type            TEXT NOT NULL,
    capacity_mw         REAL,
    generation_mwm      REAL,
    p50_mwm             REAL,
    p90_mwm             REAL,
    contracted_mwm      REAL,
    capacity_factor_pct REAL,
    availability_pct    REAL,
    curtailment_mwm     REAL,
    ppa_price_brl_mwh   REAL,
    ppa_end_date        TEXT,
    ebitda_brl          REAL,
    ebitda_margin_pct   REAL,
    revenue_brl         REAL,
    source_pdf          TEXT,
    PRIMARY KEY (fund_id, reference_date, asset_name)
);

CREATE TABLE IF NOT EXISTS ops_port (
    fund_id           TEXT NOT NULL REFERENCES funds(fund_id),
    reference_date    TEXT NOT NULL,
    asset_name        TEXT NOT NULL,
    teus_month        REAL,
    teus_ytd          REAL,
    containers_month  REAL,
    revenue_brl       REAL,
    ebitda_brl        REAL,
    ebitda_margin_pct REAL,
    net_income_brl    REAL,
    net_debt_brl      REAL,
    net_debt_ebitda   REAL,
    source_pdf        TEXT,
    PRIMARY KEY (fund_id, reference_date, asset_name)
);

CREATE TABLE IF NOT EXISTS manager_commentary (
    fund_id        TEXT NOT NULL REFERENCES funds(fund_id),
    reference_date TEXT NOT NULL,
    section        TEXT NOT NULL,
    content        TEXT NOT NULL,
    PRIMARY KEY (fund_id, reference_date, section)
);

CREATE TABLE IF NOT EXISTS fund_metrics_kv (
    fund_id        TEXT NOT NULL REFERENCES funds(fund_id),
    reference_date TEXT NOT NULL,
    metric_key     TEXT NOT NULL,
    metric_value   REAL,
    metric_unit    TEXT,
    source_pdf     TEXT,
    PRIMARY KEY (fund_id, reference_date, metric_key)
);

CREATE TABLE IF NOT EXISTS extraction_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    fund_id        TEXT NOT NULL,
    source_pdf     TEXT NOT NULL,
    reference_date TEXT,
    started_at     TEXT NOT NULL,
    completed_at   TEXT,
    status         TEXT NOT NULL,
    parser_version TEXT,
    warnings       TEXT,
    errors         TEXT
);
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create the database and all tables if they don't exist."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(DB_SCHEMA)
    conn.commit()

    logger.info("Database initialized at %s", db_path)
    return conn


def upsert_fund(conn: sqlite3.Connection, fund: Fund) -> None:
    """Insert or update a fund record."""
    conn.execute(
        """INSERT INTO funds (fund_id, ticker, fund_name, cnpj, manager,
           administrator, segment, inception_date, fund_term, target_audience,
           admin_fee_pct, mgmt_fee_pct, perf_fee_pct)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(fund_id) DO UPDATE SET
             fund_name=excluded.fund_name,
             cnpj=COALESCE(excluded.cnpj, funds.cnpj),
             administrator=COALESCE(excluded.administrator, funds.administrator),
             inception_date=COALESCE(excluded.inception_date, funds.inception_date),
             fund_term=COALESCE(excluded.fund_term, funds.fund_term),
             target_audience=COALESCE(excluded.target_audience, funds.target_audience),
             admin_fee_pct=COALESCE(excluded.admin_fee_pct, funds.admin_fee_pct),
             mgmt_fee_pct=COALESCE(excluded.mgmt_fee_pct, funds.mgmt_fee_pct),
             perf_fee_pct=COALESCE(excluded.perf_fee_pct, funds.perf_fee_pct)
        """,
        (
            fund.fund_id, fund.ticker, fund.fund_name, fund.cnpj, fund.manager,
            fund.administrator, fund.segment,
            str(fund.inception_date) if fund.inception_date else None,
            fund.fund_term, fund.target_audience,
            fund.admin_fee_pct, fund.mgmt_fee_pct, fund.perf_fee_pct,
        ),
    )


def upsert_snapshot(conn: sqlite3.Connection, snap: MonthlySnapshot) -> None:
    """Insert or update a monthly snapshot."""
    conn.execute(
        """INSERT INTO monthly_snapshots (
             fund_id, reference_date, report_date,
             nav_total, market_cap, nav_per_unit, market_price,
             units_outstanding, discount_premium_pct, price_to_book,
             irr_real, irr_nominal,
             return_month_pct, return_ytd_pct, return_12m_pct,
             return_since_inception_pct,
             distribution_per_unit, distribution_total_accum, dividend_yield_ltm,
             avg_daily_volume, num_investors, pct_retail,
             source_pdf, page_count
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(fund_id, reference_date) DO UPDATE SET
             report_date=COALESCE(excluded.report_date, monthly_snapshots.report_date),
             nav_total=COALESCE(excluded.nav_total, monthly_snapshots.nav_total),
             market_cap=COALESCE(excluded.market_cap, monthly_snapshots.market_cap),
             nav_per_unit=COALESCE(excluded.nav_per_unit, monthly_snapshots.nav_per_unit),
             market_price=COALESCE(excluded.market_price, monthly_snapshots.market_price),
             units_outstanding=COALESCE(excluded.units_outstanding, monthly_snapshots.units_outstanding),
             discount_premium_pct=COALESCE(excluded.discount_premium_pct, monthly_snapshots.discount_premium_pct),
             price_to_book=COALESCE(excluded.price_to_book, monthly_snapshots.price_to_book),
             irr_real=COALESCE(excluded.irr_real, monthly_snapshots.irr_real),
             irr_nominal=COALESCE(excluded.irr_nominal, monthly_snapshots.irr_nominal),
             return_month_pct=COALESCE(excluded.return_month_pct, monthly_snapshots.return_month_pct),
             return_ytd_pct=COALESCE(excluded.return_ytd_pct, monthly_snapshots.return_ytd_pct),
             return_12m_pct=COALESCE(excluded.return_12m_pct, monthly_snapshots.return_12m_pct),
             return_since_inception_pct=COALESCE(excluded.return_since_inception_pct, monthly_snapshots.return_since_inception_pct),
             distribution_per_unit=COALESCE(excluded.distribution_per_unit, monthly_snapshots.distribution_per_unit),
             distribution_total_accum=COALESCE(excluded.distribution_total_accum, monthly_snapshots.distribution_total_accum),
             dividend_yield_ltm=COALESCE(excluded.dividend_yield_ltm, monthly_snapshots.dividend_yield_ltm),
             avg_daily_volume=COALESCE(excluded.avg_daily_volume, monthly_snapshots.avg_daily_volume),
             num_investors=COALESCE(excluded.num_investors, monthly_snapshots.num_investors),
             pct_retail=COALESCE(excluded.pct_retail, monthly_snapshots.pct_retail),
             source_pdf=excluded.source_pdf,
             page_count=COALESCE(excluded.page_count, monthly_snapshots.page_count)
        """,
        (
            snap.fund_id, snap.reference_date, snap.report_date,
            snap.nav_total, snap.market_cap, snap.nav_per_unit, snap.market_price,
            snap.units_outstanding, snap.discount_premium_pct, snap.price_to_book,
            snap.irr_real, snap.irr_nominal,
            snap.return_month_pct, snap.return_ytd_pct, snap.return_12m_pct,
            snap.return_since_inception_pct,
            snap.distribution_per_unit, snap.distribution_total_accum, snap.dividend_yield_ltm,
            snap.avg_daily_volume, snap.num_investors, snap.pct_retail,
            snap.source_pdf, snap.page_count,
        ),
    )


def upsert_distribution(conn: sqlite3.Connection, dist: Distribution) -> None:
    """Insert or update a distribution record."""
    conn.execute(
        """INSERT INTO distributions (fund_id, reference_month, ex_date, payment_date, amount_per_unit)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(fund_id, reference_month) DO UPDATE SET
             ex_date=COALESCE(excluded.ex_date, distributions.ex_date),
             payment_date=COALESCE(excluded.payment_date, distributions.payment_date),
             amount_per_unit=excluded.amount_per_unit
        """,
        (dist.fund_id, dist.reference_month, dist.ex_date, dist.payment_date, dist.amount_per_unit),
    )


def upsert_commentary(conn: sqlite3.Connection, comm: ManagerCommentary) -> None:
    """Insert or update a manager commentary."""
    conn.execute(
        """INSERT INTO manager_commentary (fund_id, reference_date, section, content)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(fund_id, reference_date, section) DO UPDATE SET
             content=excluded.content
        """,
        (comm.fund_id, comm.reference_date, comm.section, comm.content),
    )


def upsert_metric_kv(conn: sqlite3.Connection, kv: FundMetricKV) -> None:
    """Insert or update a key-value metric."""
    conn.execute(
        """INSERT INTO fund_metrics_kv (fund_id, reference_date, metric_key, metric_value, metric_unit, source_pdf)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(fund_id, reference_date, metric_key) DO UPDATE SET
             metric_value=excluded.metric_value,
             metric_unit=COALESCE(excluded.metric_unit, fund_metrics_kv.metric_unit),
             source_pdf=excluded.source_pdf
        """,
        (kv.fund_id, kv.reference_date, kv.metric_key, kv.metric_value, kv.metric_unit, kv.source_pdf),
    )


def upsert_transmission(conn: sqlite3.Connection, asset: TransmissionAsset) -> None:
    """Insert or update a transmission asset record."""
    conn.execute(
        """INSERT INTO ops_transmission (fund_id, reference_date, asset_name,
             extension_km, rap_annual_brl, concession_end, availability_pct,
             ebitda_brl, ebitda_margin_pct, revenue_brl, source_pdf)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(fund_id, reference_date, asset_name) DO UPDATE SET
             extension_km=COALESCE(excluded.extension_km, ops_transmission.extension_km),
             rap_annual_brl=COALESCE(excluded.rap_annual_brl, ops_transmission.rap_annual_brl),
             concession_end=COALESCE(excluded.concession_end, ops_transmission.concession_end),
             availability_pct=COALESCE(excluded.availability_pct, ops_transmission.availability_pct),
             ebitda_brl=COALESCE(excluded.ebitda_brl, ops_transmission.ebitda_brl),
             ebitda_margin_pct=COALESCE(excluded.ebitda_margin_pct, ops_transmission.ebitda_margin_pct),
             revenue_brl=COALESCE(excluded.revenue_brl, ops_transmission.revenue_brl),
             source_pdf=excluded.source_pdf
        """,
        (
            asset.fund_id, asset.reference_date, asset.asset_name,
            asset.extension_km, asset.rap_annual_brl, asset.concession_end,
            asset.availability_pct, asset.ebitda_brl, asset.ebitda_margin_pct,
            asset.revenue_brl, asset.source_pdf,
        ),
    )


def upsert_generation(conn: sqlite3.Connection, asset: GenerationAsset) -> None:
    """Insert or update a generation asset record."""
    conn.execute(
        """INSERT INTO ops_generation (fund_id, reference_date, asset_name, gen_type,
             capacity_mw, generation_mwm, p50_mwm, p90_mwm, contracted_mwm,
             capacity_factor_pct, availability_pct, curtailment_mwm,
             ppa_price_brl_mwh, ppa_end_date,
             ebitda_brl, ebitda_margin_pct, revenue_brl, source_pdf)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(fund_id, reference_date, asset_name) DO UPDATE SET
             gen_type=excluded.gen_type,
             capacity_mw=COALESCE(excluded.capacity_mw, ops_generation.capacity_mw),
             generation_mwm=COALESCE(excluded.generation_mwm, ops_generation.generation_mwm),
             p50_mwm=COALESCE(excluded.p50_mwm, ops_generation.p50_mwm),
             p90_mwm=COALESCE(excluded.p90_mwm, ops_generation.p90_mwm),
             contracted_mwm=COALESCE(excluded.contracted_mwm, ops_generation.contracted_mwm),
             capacity_factor_pct=COALESCE(excluded.capacity_factor_pct, ops_generation.capacity_factor_pct),
             availability_pct=COALESCE(excluded.availability_pct, ops_generation.availability_pct),
             curtailment_mwm=COALESCE(excluded.curtailment_mwm, ops_generation.curtailment_mwm),
             ppa_price_brl_mwh=COALESCE(excluded.ppa_price_brl_mwh, ops_generation.ppa_price_brl_mwh),
             ppa_end_date=COALESCE(excluded.ppa_end_date, ops_generation.ppa_end_date),
             ebitda_brl=COALESCE(excluded.ebitda_brl, ops_generation.ebitda_brl),
             ebitda_margin_pct=COALESCE(excluded.ebitda_margin_pct, ops_generation.ebitda_margin_pct),
             revenue_brl=COALESCE(excluded.revenue_brl, ops_generation.revenue_brl),
             source_pdf=excluded.source_pdf
        """,
        (
            asset.fund_id, asset.reference_date, asset.asset_name, asset.gen_type,
            asset.capacity_mw, asset.generation_mwm, asset.p50_mwm, asset.p90_mwm,
            asset.contracted_mwm, asset.capacity_factor_pct, asset.availability_pct,
            asset.curtailment_mwm, asset.ppa_price_brl_mwh, asset.ppa_end_date,
            asset.ebitda_brl, asset.ebitda_margin_pct, asset.revenue_brl, asset.source_pdf,
        ),
    )


def upsert_port(conn: sqlite3.Connection, asset: PortAsset) -> None:
    """Insert or update a port asset record."""
    conn.execute(
        """INSERT INTO ops_port (fund_id, reference_date, asset_name,
             teus_month, teus_ytd, containers_month,
             revenue_brl, ebitda_brl, ebitda_margin_pct,
             net_income_brl, net_debt_brl, net_debt_ebitda, source_pdf)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(fund_id, reference_date, asset_name) DO UPDATE SET
             teus_month=COALESCE(excluded.teus_month, ops_port.teus_month),
             teus_ytd=COALESCE(excluded.teus_ytd, ops_port.teus_ytd),
             containers_month=COALESCE(excluded.containers_month, ops_port.containers_month),
             revenue_brl=COALESCE(excluded.revenue_brl, ops_port.revenue_brl),
             ebitda_brl=COALESCE(excluded.ebitda_brl, ops_port.ebitda_brl),
             ebitda_margin_pct=COALESCE(excluded.ebitda_margin_pct, ops_port.ebitda_margin_pct),
             net_income_brl=COALESCE(excluded.net_income_brl, ops_port.net_income_brl),
             net_debt_brl=COALESCE(excluded.net_debt_brl, ops_port.net_debt_brl),
             net_debt_ebitda=COALESCE(excluded.net_debt_ebitda, ops_port.net_debt_ebitda),
             source_pdf=excluded.source_pdf
        """,
        (
            asset.fund_id, asset.reference_date, asset.asset_name,
            asset.teus_month, asset.teus_ytd, asset.containers_month,
            asset.revenue_brl, asset.ebitda_brl, asset.ebitda_margin_pct,
            asset.net_income_brl, asset.net_debt_brl, asset.net_debt_ebitda,
            asset.source_pdf,
        ),
    )


def upsert_holding(conn: sqlite3.Connection, h: PortfolioHolding) -> None:
    """Insert or update a portfolio holding."""
    conn.execute(
        """INSERT INTO portfolio_holdings (fund_id, reference_date, issuer,
             segment, instrument_type, ticker, pct_pl, amount_brl,
             duration_years, indexer, spread_pct, ltv_pct, icsd_min, source_pdf)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(fund_id, reference_date, issuer) DO UPDATE SET
             segment=COALESCE(excluded.segment, portfolio_holdings.segment),
             instrument_type=COALESCE(excluded.instrument_type, portfolio_holdings.instrument_type),
             pct_pl=COALESCE(excluded.pct_pl, portfolio_holdings.pct_pl),
             amount_brl=COALESCE(excluded.amount_brl, portfolio_holdings.amount_brl),
             duration_years=COALESCE(excluded.duration_years, portfolio_holdings.duration_years),
             indexer=COALESCE(excluded.indexer, portfolio_holdings.indexer),
             spread_pct=COALESCE(excluded.spread_pct, portfolio_holdings.spread_pct),
             ltv_pct=COALESCE(excluded.ltv_pct, portfolio_holdings.ltv_pct),
             icsd_min=COALESCE(excluded.icsd_min, portfolio_holdings.icsd_min),
             source_pdf=excluded.source_pdf
        """,
        (
            h.fund_id, h.reference_date, h.issuer,
            h.segment, h.instrument_type, h.ticker, h.pct_pl, h.amount_brl,
            h.duration_years, h.indexer, h.spread_pct, h.ltv_pct, h.icsd_min,
            h.source_pdf,
        ),
    )


def log_extraction(conn: sqlite3.Connection, entry: ExtractionLogEntry) -> None:
    """Insert an extraction log entry."""
    conn.execute(
        """INSERT INTO extraction_log (fund_id, source_pdf, reference_date,
             started_at, completed_at, status, parser_version, warnings, errors)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry.fund_id, entry.source_pdf, entry.reference_date,
            entry.started_at.isoformat(),
            entry.completed_at.isoformat() if entry.completed_at else None,
            entry.status, entry.parser_version, entry.warnings, entry.errors,
        ),
    )


def load_report(conn: sqlite3.Connection, report: FundReport) -> None:
    """Load a complete FundReport into the database."""
    if report.fund:
        upsert_fund(conn, report.fund)

    if report.snapshot:
        upsert_snapshot(conn, report.snapshot)

    for dist in report.distributions:
        upsert_distribution(conn, dist)

    for holding in report.portfolio_holdings:
        upsert_holding(conn, holding)

    for asset in report.transmission_assets:
        upsert_transmission(conn, asset)

    for asset in report.generation_assets:
        upsert_generation(conn, asset)

    for asset in report.port_assets:
        upsert_port(conn, asset)

    for comm in report.commentaries:
        upsert_commentary(conn, comm)

    for kv in report.extra_metrics:
        upsert_metric_kv(conn, kv)

    conn.commit()
