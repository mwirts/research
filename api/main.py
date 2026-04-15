"""FastAPI application for the FIP-IE Fund Analysis Dashboard."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from api.routes import comparative, export, funds, market, portfolio

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"

app = FastAPI(
    title="FIP-IE Fund Analysis",
    description="API para analise de fundos FIP-IE listados na B3",
    version="1.0.0",
)

# Register API routes
app.include_router(funds.router, prefix="/api")
app.include_router(comparative.router, prefix="/api")
app.include_router(export.router, prefix="/api")
app.include_router(market.router, prefix="/api")
app.include_router(portfolio.router, prefix="/api")

# Serve dashboard static files
if DASHBOARD_DIR.exists():
    app.mount("/", StaticFiles(directory=str(DASHBOARD_DIR), html=True), name="dashboard")
