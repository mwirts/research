"""Validation layer for extracted fund data."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from etl.transformer.models import FundReport

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    fund_id: str
    reference_date: str
    is_valid: bool = True
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)
        logger.warning("[%s %s] %s", self.fund_id, self.reference_date, msg)

    def error(self, msg: str) -> None:
        self.errors.append(msg)
        self.is_valid = False
        logger.error("[%s %s] %s", self.fund_id, self.reference_date, msg)


def validate_report(report: FundReport) -> ValidationResult:
    """Validate a FundReport for consistency and completeness."""
    snap = report.snapshot
    if not snap:
        result = ValidationResult(
            fund_id=report.fund.fund_id if report.fund else "unknown",
            reference_date="unknown",
        )
        result.error("No snapshot data found")
        return result

    result = ValidationResult(
        fund_id=snap.fund_id,
        reference_date=snap.reference_date,
    )

    # --- Required fields ---
    if snap.nav_total is None:
        result.warn("Missing nav_total (patrimonio liquido)")
    if snap.market_price is None:
        result.warn("Missing market_price (cota mercado)")
    if snap.source_pdf == "":
        result.warn("Missing source_pdf")

    # --- Cross-field consistency ---
    if snap.market_cap and snap.market_price and snap.units_outstanding:
        expected_cap = snap.market_price * snap.units_outstanding
        if abs(expected_cap - snap.market_cap) / snap.market_cap > 0.05:
            result.warn(
                f"market_cap ({snap.market_cap:,.0f}) != "
                f"market_price * units ({expected_cap:,.0f}), diff > 5%"
            )

    if snap.nav_total and snap.nav_per_unit and snap.units_outstanding:
        expected_nav = snap.nav_per_unit * snap.units_outstanding
        if abs(expected_nav - snap.nav_total) / snap.nav_total > 0.05:
            result.warn(
                f"nav_total ({snap.nav_total:,.0f}) != "
                f"nav_per_unit * units ({expected_nav:,.0f}), diff > 5%"
            )

    # --- Range checks ---
    if snap.nav_per_unit is not None and snap.nav_per_unit <= 0:
        result.error(f"nav_per_unit is non-positive: {snap.nav_per_unit}")

    if snap.market_price is not None and snap.market_price <= 0:
        result.error(f"market_price is non-positive: {snap.market_price}")

    if snap.discount_premium_pct is not None and abs(snap.discount_premium_pct) > 80:
        result.warn(f"Unusual discount/premium: {snap.discount_premium_pct}%")

    if snap.irr_real is not None and (snap.irr_real < 0 or snap.irr_real > 50):
        result.warn(f"Unusual TIR real: {snap.irr_real}%")

    if snap.dividend_yield_ltm is not None and snap.dividend_yield_ltm > 30:
        result.warn(f"Unusual DY LTM: {snap.dividend_yield_ltm}%")

    if snap.num_investors is not None and snap.num_investors <= 0:
        result.warn(f"Invalid num_investors: {snap.num_investors}")

    if snap.pct_retail is not None and (snap.pct_retail < 0 or snap.pct_retail > 100):
        result.warn(f"Invalid pct_retail: {snap.pct_retail}%")

    # --- Distribution consistency ---
    for dist in report.distributions:
        if dist.amount_per_unit <= 0:
            result.warn(f"Non-positive distribution: {dist.amount_per_unit} in {dist.reference_month}")

    # --- Log summary ---
    if result.is_valid and not result.warnings:
        logger.info("[%s %s] Validation passed", result.fund_id, result.reference_date)
    elif result.is_valid:
        logger.info(
            "[%s %s] Validation passed with %d warning(s)",
            result.fund_id, result.reference_date, len(result.warnings),
        )

    return result
