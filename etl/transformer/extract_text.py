"""Layer 1: Extract raw text and tables from PDF using pdfplumber.

Produces a JSON file per PDF with the structure:
{
    "metadata": {
        "fund_id": "pfin11",
        "ticker": "PFIN11",
        "year": 2026,
        "month": 2,
        "source_pdf": "pfin11_relatorio_mensal_2026_02.pdf",
        "page_count": 4,
        "file_size_bytes": 123456
    },
    "pages": [
        {
            "page_num": 1,
            "text": "...",
            "tables": [[[cell, ...], ...], ...]
        },
        ...
    ]
}
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)


def extract_pdf(pdf_path: Path, output_dir: Path | None = None) -> dict:
    """Extract text and tables from a PDF file.

    Args:
        pdf_path: Path to the PDF file.
        output_dir: If provided, saves the output JSON to this directory.

    Returns:
        dict with metadata and pages.
    """
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    # Parse filename: {ticker}_relatorio_mensal_{year}_{month}.pdf
    m = re.match(r"(\w+)_relatorio_mensal_(\d{4})_(\d{2})\.pdf", pdf_path.name)
    if not m:
        raise ValueError(f"Unexpected filename format: {pdf_path.name}")

    fund_id = m.group(1).lower()
    ticker = m.group(1).upper()
    year = int(m.group(2))
    month = int(m.group(3))

    logger.info("Extracting %s (%04d-%02d) ...", ticker, year, month)

    pages = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages):
            page_data = {
                "page_num": i + 1,
                "text": page.extract_text() or "",
                "tables": [],
            }

            # Extract tables
            try:
                tables = page.extract_tables()
                if tables:
                    page_data["tables"] = [
                        _clean_table(t) for t in tables
                    ]
            except Exception as e:
                logger.warning(
                    "Table extraction failed on page %d of %s: %s",
                    i + 1, pdf_path.name, e,
                )

            pages.append(page_data)

    result = {
        "metadata": {
            "fund_id": fund_id,
            "ticker": ticker,
            "year": year,
            "month": month,
            "source_pdf": pdf_path.name,
            "page_count": len(pages),
            "file_size_bytes": pdf_path.stat().st_size,
        },
        "pages": pages,
    }

    # Save to output directory if provided
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{fund_id}_{year:04d}_{month:02d}_raw.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info("Saved raw extraction to %s", output_file)

    return result


def _clean_table(table: list[list]) -> list[list[str]]:
    """Clean a table extracted by pdfplumber: normalize cells to strings."""
    cleaned = []
    for row in table:
        cleaned_row = []
        for cell in row:
            if cell is None:
                cleaned_row.append("")
            else:
                # Normalize whitespace
                cleaned_row.append(re.sub(r"\s+", " ", str(cell).strip()))
        cleaned.append(cleaned_row)
    return cleaned


def extract_fund(
    fund_id: str,
    raw_dir: Path,
    output_base_dir: Path,
    force: bool = False,
) -> list[Path]:
    """Extract all PDFs for a given fund.

    Args:
        fund_id: e.g. "pfin11"
        raw_dir: Base directory containing raw PDFs (data/raw/funds/)
        output_base_dir: Base directory for extracted JSONs (data/processed/extracted/)
        force: If True, re-extract even if output already exists.

    Returns:
        List of output JSON paths.
    """
    pdf_dir = raw_dir / fund_id / "monthly_report"
    if not pdf_dir.exists():
        logger.warning("PDF directory not found: %s", pdf_dir)
        return []

    output_dir = output_base_dir / fund_id
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = []
    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    logger.info("Found %d PDFs for %s", len(pdf_files), fund_id.upper())

    for pdf_path in pdf_files:
        m = re.match(r"\w+_relatorio_mensal_(\d{4})_(\d{2})\.pdf", pdf_path.name)
        if not m:
            logger.warning("Skipping non-standard filename: %s", pdf_path.name)
            continue

        year, month = int(m.group(1)), int(m.group(2))
        output_file = output_dir / f"{fund_id}_{year:04d}_{month:02d}_raw.json"

        if output_file.exists() and not force:
            logger.debug("Skipping (already extracted): %s", pdf_path.name)
            outputs.append(output_file)
            continue

        try:
            extract_pdf(pdf_path, output_dir)
            outputs.append(output_file)
        except Exception as e:
            logger.error("Failed to extract %s: %s", pdf_path.name, e)

    return outputs
