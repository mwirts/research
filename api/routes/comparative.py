"""Comparative analysis API endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from api.database import get_db

router = APIRouter(tags=["comparative"])


@router.get("/comparative")
def get_comparative():
    """Cross-fund comparison data with latest snapshots."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT f.fund_id, f.ticker, f.fund_name, f.manager, f.segment,
                   s.reference_date, s.nav_per_unit, s.market_price,
                   s.discount_premium_pct, s.price_to_book,
                   s.irr_real, s.dividend_yield_ltm,
                   s.nav_total, s.market_cap,
                   s.avg_daily_volume, s.num_investors,
                   s.distribution_total_accum,
                   s.return_month_pct
            FROM funds f
            JOIN monthly_snapshots s ON f.fund_id = s.fund_id
                AND s.reference_date = (
                    SELECT MAX(s2.reference_date)
                    FROM monthly_snapshots s2
                    WHERE s2.fund_id = f.fund_id
                )
            ORDER BY s.irr_real DESC NULLS LAST
        """).fetchall()

        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/comparative/ranking")
def get_ranking():
    """Current ranking of funds by key metrics."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT f.fund_id, f.ticker, f.fund_name,
                   s.reference_date, s.irr_real, s.dividend_yield_ltm,
                   s.discount_premium_pct, s.avg_daily_volume, s.nav_total
            FROM funds f
            JOIN monthly_snapshots s ON f.fund_id = s.fund_id
                AND s.reference_date = (
                    SELECT MAX(s2.reference_date)
                    FROM monthly_snapshots s2
                    WHERE s2.fund_id = f.fund_id
                )
            ORDER BY f.ticker
        """).fetchall()

        funds_data = [dict(r) for r in rows]

        # Build rankings
        by_irr = sorted(
            [f for f in funds_data if f.get("irr_real") is not None],
            key=lambda f: f["irr_real"], reverse=True,
        )
        by_dy = sorted(
            [f for f in funds_data if f.get("dividend_yield_ltm") is not None],
            key=lambda f: f["dividend_yield_ltm"], reverse=True,
        )
        by_discount = sorted(
            [f for f in funds_data if f.get("discount_premium_pct") is not None],
            key=lambda f: f["discount_premium_pct"],
        )

        return {
            "funds": funds_data,
            "rankings": {
                "by_irr": [f["ticker"] for f in by_irr],
                "by_dy": [f["ticker"] for f in by_dy],
                "by_discount": [f["ticker"] for f in by_discount],
            },
        }
    finally:
        conn.close()
