"""Pydantic models for FIP-IE fund data extraction and storage."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Fund registry (static / slowly-changing)
# ---------------------------------------------------------------------------

class Fund(BaseModel):
    fund_id: str = Field(..., description="Lowercase ticker, e.g. 'pfin11'")
    ticker: str = Field(..., description="B3 ticker, e.g. 'PFIN11'")
    fund_name: str
    cnpj: Optional[str] = None
    manager: str = Field(..., description="Gestora, e.g. 'Perfin'")
    administrator: Optional[str] = None
    segment: str = Field(..., description="transmissao|geracao_eolica|geracao_solar|geracao_hidrica|energia_mista|portuario|credito_infra")
    inception_date: Optional[date] = None
    fund_term: Optional[str] = None
    target_audience: Optional[str] = None
    admin_fee_pct: Optional[float] = None
    mgmt_fee_pct: Optional[float] = None
    perf_fee_pct: Optional[float] = None


# ---------------------------------------------------------------------------
# Monthly snapshot – core fact table
# ---------------------------------------------------------------------------

class MonthlySnapshot(BaseModel):
    fund_id: str
    reference_date: str = Field(..., description="YYYY-MM format")
    report_date: Optional[str] = None

    # NAV & Pricing
    nav_total: Optional[float] = Field(None, description="Patrimonio liquido (R$)")
    market_cap: Optional[float] = Field(None, description="Valor de mercado (R$)")
    nav_per_unit: Optional[float] = Field(None, description="Cota patrimonial (R$/cota)")
    market_price: Optional[float] = Field(None, description="Cota mercado (R$/cota)")
    units_outstanding: Optional[int] = None
    discount_premium_pct: Optional[float] = Field(None, description="(market - nav) / nav * 100")
    price_to_book: Optional[float] = None

    # Performance
    irr_real: Optional[float] = Field(None, description="TIR real implicita (% IPCA+)")
    irr_nominal: Optional[float] = None
    return_month_pct: Optional[float] = None
    return_ytd_pct: Optional[float] = None
    return_12m_pct: Optional[float] = None
    return_since_inception_pct: Optional[float] = None

    # Distribution
    distribution_per_unit: Optional[float] = Field(None, description="R$/cota no mes")
    distribution_total_accum: Optional[float] = Field(None, description="Acumulado desde inicio")
    dividend_yield_ltm: Optional[float] = Field(None, description="DY ultimos 12 meses (%)")

    # Trading
    avg_daily_volume: Optional[float] = Field(None, description="Volume medio diario (R$)")
    num_investors: Optional[int] = None
    pct_retail: Optional[float] = Field(None, description="% pessoas fisicas")

    # Metadata
    source_pdf: str = ""
    page_count: Optional[int] = None


# ---------------------------------------------------------------------------
# Distribution history
# ---------------------------------------------------------------------------

class Distribution(BaseModel):
    fund_id: str
    reference_month: str = Field(..., description="YYYY-MM")
    ex_date: Optional[str] = None
    payment_date: Optional[str] = None
    amount_per_unit: float


# ---------------------------------------------------------------------------
# Portfolio holdings (credit funds like AZIN11)
# ---------------------------------------------------------------------------

class PortfolioHolding(BaseModel):
    fund_id: str
    reference_date: str
    issuer: str
    segment: Optional[str] = None
    instrument_type: Optional[str] = None
    ticker: Optional[str] = None
    pct_pl: Optional[float] = None
    amount_brl: Optional[float] = None
    duration_years: Optional[float] = None
    indexer: Optional[str] = None
    spread_pct: Optional[float] = None
    ltv_pct: Optional[float] = None
    icsd_min: Optional[float] = None
    source_pdf: str = ""


# ---------------------------------------------------------------------------
# Operational metrics: transmission assets
# ---------------------------------------------------------------------------

class TransmissionAsset(BaseModel):
    fund_id: str
    reference_date: str
    asset_name: str
    extension_km: Optional[float] = None
    rap_annual_brl: Optional[float] = None
    concession_end: Optional[str] = None
    availability_pct: Optional[float] = None
    ebitda_brl: Optional[float] = None
    ebitda_margin_pct: Optional[float] = None
    revenue_brl: Optional[float] = None
    source_pdf: str = ""


# ---------------------------------------------------------------------------
# Operational metrics: generation assets (wind, solar, hydro)
# ---------------------------------------------------------------------------

class GenerationAsset(BaseModel):
    fund_id: str
    reference_date: str
    asset_name: str
    gen_type: str = Field(..., description="eolica|solar|hidrica")
    capacity_mw: Optional[float] = None
    generation_mwm: Optional[float] = None
    p50_mwm: Optional[float] = None
    p90_mwm: Optional[float] = None
    contracted_mwm: Optional[float] = None
    capacity_factor_pct: Optional[float] = None
    availability_pct: Optional[float] = None
    curtailment_mwm: Optional[float] = None
    ppa_price_brl_mwh: Optional[float] = None
    ppa_end_date: Optional[str] = None
    ebitda_brl: Optional[float] = None
    ebitda_margin_pct: Optional[float] = None
    revenue_brl: Optional[float] = None
    source_pdf: str = ""


# ---------------------------------------------------------------------------
# Operational metrics: port assets (BRZP11)
# ---------------------------------------------------------------------------

class PortAsset(BaseModel):
    fund_id: str
    reference_date: str
    asset_name: str
    teus_month: Optional[float] = None
    teus_ytd: Optional[float] = None
    containers_month: Optional[float] = None
    revenue_brl: Optional[float] = None
    ebitda_brl: Optional[float] = None
    ebitda_margin_pct: Optional[float] = None
    net_income_brl: Optional[float] = None
    net_debt_brl: Optional[float] = None
    net_debt_ebitda: Optional[float] = None
    source_pdf: str = ""


# ---------------------------------------------------------------------------
# Manager commentary
# ---------------------------------------------------------------------------

class ManagerCommentary(BaseModel):
    fund_id: str
    reference_date: str
    section: str = Field(..., description="macro|strategy|portfolio|outlook|highlights")
    content: str


# ---------------------------------------------------------------------------
# Key-value store for fund-specific metrics
# ---------------------------------------------------------------------------

class FundMetricKV(BaseModel):
    fund_id: str
    reference_date: str
    metric_key: str
    metric_value: Optional[float] = None
    metric_unit: Optional[str] = None
    source_pdf: str = ""


# ---------------------------------------------------------------------------
# Extraction log
# ---------------------------------------------------------------------------

class ExtractionLogEntry(BaseModel):
    fund_id: str
    source_pdf: str
    reference_date: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = Field(..., description="success|partial|failed")
    parser_version: Optional[str] = None
    warnings: Optional[str] = None
    errors: Optional[str] = None


# ---------------------------------------------------------------------------
# Top-level container for a full parsed report
# ---------------------------------------------------------------------------

class FundReport(BaseModel):
    """All data extracted from a single monthly report PDF."""

    fund: Optional[Fund] = None
    snapshot: Optional[MonthlySnapshot] = None
    distributions: list[Distribution] = Field(default_factory=list)
    portfolio_holdings: list[PortfolioHolding] = Field(default_factory=list)
    transmission_assets: list[TransmissionAsset] = Field(default_factory=list)
    generation_assets: list[GenerationAsset] = Field(default_factory=list)
    port_assets: list[PortAsset] = Field(default_factory=list)
    commentaries: list[ManagerCommentary] = Field(default_factory=list)
    extra_metrics: list[FundMetricKV] = Field(default_factory=list)
