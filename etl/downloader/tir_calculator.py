"""TIR (IRR) calculator - scrapes breakpoints from fund websites.

Supports two parser types:
  Type A (MZ IQ template): PFIN11, PPEI11, VIGT11 - hidden <table id="intervalos">
  Type B (static table):   PICE11 - plain HTML sensitivity table with discrete points

BRZP11 and AZIN11 have no public TIR calculator.

Usage:
    python -m etl.downloader.tir_calculator --update-all
    python -m etl.downloader.tir_calculator --update --fund PFIN11
    python -m etl.downloader.tir_calculator --calc 87.70 --fund PFIN11
    python -m etl.downloader.tir_calculator --show --fund PFIN11
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "processed" / "market_data.ddb"

# Fund TIR sources: url + parser type + gross-up parameters
TIR_SOURCES = {
    "PFIN11": {
        "url": "https://www.perfinapolloenergia.com/calculadora-tir-cota-2/",
        "parser": "mziq",
        "inflation": 0.035,
        "tax_rate": 0.15,
    },
    "PPEI11": {
        "url": "https://ri.protonenergy.com.br/taxa-interna-de-retornos-e-dividendos/",
        "parser": "mziq",
        "inflation": 0.0373,
        "tax_rate": 0.15,
    },
    "VIGT11": {
        "url": "https://www.vincifundoslistados.com/vigt11/informacoes-aos-investidores/calculadora-tir-x-cota-vigt11/",
        "parser": "mziq",
        "inflation": 0.04,
        "tax_rate": 0.15,
    },
    "PICE11": {
        "url": "https://www.pice11.com.br/infos-e-docs/analise-de-sensibilidade/",
        "parser": "static_table",
        "inflation": 0.035,
        "tax_rate": 0.15,
    },
}

TIR_SCHEMA = """
CREATE TABLE IF NOT EXISTS tir_breakpoints (
    ticker          VARCHAR NOT NULL,
    cota_min        DOUBLE NOT NULL,
    cota_max        DOUBLE NOT NULL,
    ipca_plus_start DOUBLE NOT NULL,
    ipca_plus_end   DOUBLE NOT NULL,
    scraped_at      TIMESTAMP NOT NULL,
    source_url      VARCHAR,
    PRIMARY KEY (ticker, cota_min, scraped_at)
);

CREATE TABLE IF NOT EXISTS tir_parameters (
    ticker              VARCHAR NOT NULL,
    param_key           VARCHAR NOT NULL,
    param_value         DOUBLE NOT NULL,
    scraped_at          TIMESTAMP NOT NULL,
    PRIMARY KEY (ticker, param_key, scraped_at)
);
"""


def init_tir_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute(TIR_SCHEMA)


def _fetch_html(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    resp = requests.get(url, timeout=30, verify=False, headers=headers)
    resp.raise_for_status()
    return resp.text


# ---------------------------------------------------------------------------
# Parser Type A: MZ IQ template (PFIN11, PPEI11, VIGT11)
# ---------------------------------------------------------------------------

def _parse_mziq(html: str) -> list[dict]:
    """Parse <table id="intervalos"> with CSS-classed <td> cells."""
    table_match = re.search(
        r'<table[^>]*id\s*=\s*["\']intervalos["\'][^>]*>(.*?)</table>',
        html, re.DOTALL | re.IGNORECASE,
    )
    if not table_match:
        raise ValueError("No <table id='intervalos'> found")

    table_html = table_match.group(1)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
    breakpoints = []

    for row_html in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
        if len(cells) < 4:
            continue

        vals = [_parse_num(c) for c in cells[:4]]
        if any(v is None or v == 0 for v in vals):
            continue  # skip empty placeholder rows

        # Column order: [ipca_start, ipca_end, cota_min, cota_max]
        breakpoints.append({
            "cota_min": vals[2],
            "cota_max": vals[3],
            "ipca_plus_start": vals[0],  # already decimal (0.10 = 10%)
            "ipca_plus_end": vals[1],
        })

    return breakpoints


# ---------------------------------------------------------------------------
# Parser Type B: Static sensitivity table (PICE11)
# ---------------------------------------------------------------------------

def _parse_static_table(html: str) -> list[dict]:
    """Parse a static sensitivity table with discrete cota/TIR points.

    Expected layout:
      Row 0: Valor da Cota (R$) | 26,00 | 28,50 | 31,00 | 33,50 | 36,00
      Row 1: TIR Implicita ...  | 36,8% | 33,4% | 30,7% | 28,5% | 26,5%

    Converts discrete points to piecewise intervals for interpolation.
    """
    # Find tbody rows
    tbody_match = re.search(r'<tbody>(.*?)</tbody>', html, re.DOTALL | re.IGNORECASE)
    if not tbody_match:
        raise ValueError("No <tbody> found for static TIR table")

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_match.group(1), re.DOTALL)
    if len(rows) < 2:
        raise ValueError(f"Expected 2 data rows, found {len(rows)}")

    def extract_cells(row_html):
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
        return [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

    cota_cells = extract_cells(rows[0])
    tir_cells = extract_cells(rows[1])

    # Parse cota values (skip label cell, handle "31,00*" format)
    cotas = []
    for c in cota_cells:
        c = c.replace("*", "").replace(".", "").replace(",", ".").strip()
        try:
            cotas.append(float(c))
        except ValueError:
            continue  # skip label cell

    # Parse TIR values (skip label cell, handle "36,8%" format)
    tirs = []
    for c in tir_cells:
        c = c.replace("%", "").replace(".", "").replace(",", ".").strip()
        try:
            tirs.append(float(c) / 100)  # convert 36.8% to 0.368
        except ValueError:
            continue

    if len(cotas) != len(tirs) or len(cotas) < 2:
        raise ValueError(f"Mismatch: {len(cotas)} cotas vs {len(tirs)} TIRs")

    # Convert discrete points to intervals
    breakpoints = []
    for i in range(len(cotas) - 1):
        breakpoints.append({
            "cota_min": cotas[i],
            "cota_max": cotas[i + 1],
            "ipca_plus_start": tirs[i],
            "ipca_plus_end": tirs[i + 1],
        })

    return breakpoints


# ---------------------------------------------------------------------------
# Scrape + Store
# ---------------------------------------------------------------------------

def scrape_fund(ticker: str) -> dict:
    """Scrape TIR breakpoints for a given fund."""
    if ticker not in TIR_SOURCES:
        raise ValueError(f"No TIR source for {ticker}")

    cfg = TIR_SOURCES[ticker]
    url = cfg["url"]
    parser_type = cfg["parser"]

    logger.info("%s: Scraping from %s (parser=%s)", ticker, url, parser_type)
    html = _fetch_html(url)

    if parser_type == "mziq":
        breakpoints = _parse_mziq(html)
    elif parser_type == "static_table":
        breakpoints = _parse_static_table(html)
    else:
        raise ValueError(f"Unknown parser type: {parser_type}")

    if not breakpoints:
        raise ValueError(f"No breakpoints extracted for {ticker}")

    logger.info(
        "%s: %d breakpoints, cota R$ %.2f - R$ %.2f, IPCA+ %.1f%% - %.1f%%",
        ticker, len(breakpoints),
        breakpoints[0]["cota_min"], breakpoints[-1]["cota_max"],
        breakpoints[0]["ipca_plus_start"] * 100,
        breakpoints[-1]["ipca_plus_end"] * 100,
    )

    return {
        "breakpoints": breakpoints,
        "metadata": {
            "inflation": cfg["inflation"],
            "tax_rate": cfg["tax_rate"],
            "source_url": url,
            "scraped_at": datetime.now(),
        },
    }


def save_breakpoints(conn: duckdb.DuckDBPyConnection, ticker: str, data: dict):
    """Save scraped breakpoints to DuckDB."""
    init_tir_tables(conn)
    scraped_at = data["metadata"]["scraped_at"]
    source_url = data["metadata"]["source_url"]

    for bp in data["breakpoints"]:
        conn.execute("""
            INSERT OR REPLACE INTO tir_breakpoints
            (ticker, cota_min, cota_max, ipca_plus_start, ipca_plus_end, scraped_at, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [ticker, bp["cota_min"], bp["cota_max"],
              bp["ipca_plus_start"], bp["ipca_plus_end"],
              scraped_at, source_url])

    for key, value in [("inflation", data["metadata"]["inflation"]),
                       ("tax_rate", data["metadata"]["tax_rate"])]:
        conn.execute("""
            INSERT OR REPLACE INTO tir_parameters (ticker, param_key, param_value, scraped_at)
            VALUES (?, ?, ?, ?)
        """, [ticker, key, value, scraped_at])

    logger.info("%s: Saved %d breakpoints", ticker, len(data["breakpoints"]))


# ---------------------------------------------------------------------------
# Calculation
# ---------------------------------------------------------------------------

def calc_ipca_plus(breakpoints: list[dict], cota: float) -> float | None:
    """Piecewise linear interpolation of IPCA+ for a given cota."""
    for bp in breakpoints:
        if bp["cota_min"] <= cota <= bp["cota_max"]:
            span = bp["cota_max"] - bp["cota_min"]
            if span == 0:
                return bp["ipca_plus_start"]
            slope = (bp["ipca_plus_end"] - bp["ipca_plus_start"]) / span
            return bp["ipca_plus_start"] + slope * (cota - bp["cota_min"])
    return None


def calc_gross_up(ipca_plus: float, inflation: float = 0.035, tax_rate: float = 0.15) -> float:
    """Nominal gross-up: [(1+IPCA+)*(1+infl)-1] / (1-tax)"""
    nominal = (1 + ipca_plus) * (1 + inflation) - 1
    return nominal / (1 - tax_rate)


def get_latest_breakpoints(conn: duckdb.DuckDBPyConnection, ticker: str) -> list[dict]:
    result = conn.execute("""
        SELECT cota_min, cota_max, ipca_plus_start, ipca_plus_end
        FROM tir_breakpoints
        WHERE ticker = ? AND scraped_at = (
            SELECT MAX(scraped_at) FROM tir_breakpoints WHERE ticker = ?
        )
        ORDER BY cota_min
    """, [ticker, ticker]).fetchall()
    return [{"cota_min": r[0], "cota_max": r[1],
             "ipca_plus_start": r[2], "ipca_plus_end": r[3]} for r in result]


def calculate_tir(conn: duckdb.DuckDBPyConnection, ticker: str, cota: float) -> dict | None:
    bps = get_latest_breakpoints(conn, ticker)
    if not bps:
        return None

    params = {}
    for r in conn.execute("""
        SELECT param_key, param_value FROM tir_parameters
        WHERE ticker = ? AND scraped_at = (
            SELECT MAX(scraped_at) FROM tir_parameters WHERE ticker = ?
        )
    """, [ticker, ticker]).fetchall():
        params[r[0]] = r[1]

    ipca = calc_ipca_plus(bps, cota)
    if ipca is None:
        return None

    inflation = params.get("inflation", 0.035)
    tax_rate = params.get("tax_rate", 0.15)

    return {
        "ticker": ticker,
        "cota": round(cota, 2),
        "ipca_plus": ipca,
        "ipca_plus_pct": round(ipca * 100, 2),
        "gross_up_pct": round(calc_gross_up(ipca, inflation, tax_rate) * 100, 2),
    }


def _parse_num(text: str) -> float | None:
    text = re.sub(r'<[^>]+>', '', text).strip().replace(" ", "")
    if "," in text and "." not in text:
        text = text.replace(",", ".")
    elif "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="FIP-IE TIR Calculator")
    parser.add_argument("--update", action="store_true", help="Scrape breakpoints for --fund")
    parser.add_argument("--update-all", action="store_true", help="Scrape all funds")
    parser.add_argument("--calc", type=float, help="Calculate TIR for a cota price")
    parser.add_argument("--fund", type=str, default="PFIN11")
    parser.add_argument("--show", action="store_true", help="Show breakpoints")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    conn = duckdb.connect(str(DB_PATH))
    init_tir_tables(conn)

    if args.update_all:
        for ticker in TIR_SOURCES:
            try:
                data = scrape_fund(ticker)
                save_breakpoints(conn, ticker, data)
                print(f"  {ticker}: {len(data['breakpoints'])} breakpoints OK")
            except Exception as e:
                print(f"  {ticker}: FAILED - {e}")

    elif args.update:
        ticker = args.fund.upper()
        data = scrape_fund(ticker)
        save_breakpoints(conn, ticker, data)
        print(f"{ticker}: {len(data['breakpoints'])} breakpoints saved")

    if args.show:
        ticker = args.fund.upper()
        bps = get_latest_breakpoints(conn, ticker)
        if bps:
            print(f"\n=== {ticker} TIR Breakpoints ({len(bps)}) ===")
            print(f"{'Cota Min':>10s} {'Cota Max':>10s} {'IPCA+ Start':>12s} {'IPCA+ End':>10s}")
            for bp in bps:
                print(f"R${bp['cota_min']:>8.2f} R${bp['cota_max']:>8.2f} {bp['ipca_plus_start']*100:>10.2f}% {bp['ipca_plus_end']*100:>8.2f}%")
        else:
            print(f"No breakpoints for {ticker}")

    if args.calc is not None:
        ticker = args.fund.upper()
        result = calculate_tir(conn, ticker, args.calc)
        if result:
            print(f"\n{ticker} @ R$ {args.calc:.2f}: IPCA+ {result['ipca_plus_pct']:.2f}%, Gross-up {result['gross_up_pct']:.2f}%")
        else:
            print(f"Cota R$ {args.calc:.2f} fora do range para {ticker}")

    conn.close()


if __name__ == "__main__":
    main()
