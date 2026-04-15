"""Base parser with common functions for Brazilian financial report parsing."""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from etl.transformer.models import FundReport


# ---------------------------------------------------------------------------
# Number / currency parsing
# ---------------------------------------------------------------------------

def parse_brl(text: str) -> Optional[float]:
    """Parse a Brazilian currency string to float.

    Examples:
        "R$ 1.738.675.310,77" -> 1738675310.77
        "R$ 1,5 bilhão"      -> 1500000000.0
        "R$ 928,21 milhões"   -> 928210000.0
        "1.335.259,26"        -> 1335259.26
    """
    if not text or not text.strip():
        return None

    text = text.strip()
    # Remove R$ prefix and whitespace
    text = re.sub(r"R\$\s*", "", text)

    # Handle "bilhão/bilhões"
    m = re.match(r"([\d.,]+)\s*bilh", text, re.IGNORECASE)
    if m:
        num = _parse_br_number(m.group(1))
        return num * 1_000_000_000 if num is not None else None

    # Handle "milhão/milhões/MM"
    m = re.match(r"([\d.,]+)\s*(?:milh|MM)", text, re.IGNORECASE)
    if m:
        num = _parse_br_number(m.group(1))
        return num * 1_000_000 if num is not None else None

    # Handle "mil"
    m = re.match(r"([\d.,]+)\s*mil", text, re.IGNORECASE)
    if m:
        num = _parse_br_number(m.group(1))
        return num * 1_000 if num is not None else None

    return _parse_br_number(text)


def _parse_br_number(text: str) -> Optional[float]:
    """Parse a Brazilian formatted number: 1.234.567,89 -> 1234567.89"""
    if not text:
        return None
    text = text.strip().replace(" ", "")
    # Remove trailing non-numeric chars except comma/dot/minus
    text = re.sub(r"[^\d.,-]", "", text)
    if not text:
        return None

    # Brazilian format: dots as thousands, comma as decimal
    # If there's a comma, treat it as decimal separator
    if "," in text:
        text = text.replace(".", "")   # remove thousand separators
        text = text.replace(",", ".")  # comma -> dot for decimal
    # If only dots and the last dot has <= 2 digits after it, it's ambiguous
    # but we assume Brazilian format (dots are thousands) unless it looks like US
    elif text.count(".") > 1:
        text = text.replace(".", "")  # all dots are thousands

    try:
        return float(text)
    except ValueError:
        return None


def parse_pct(text: str) -> Optional[float]:
    """Parse a percentage string to float.

    Examples:
        "14,8%"      -> 14.8
        "99,83%"     -> 99.83
        "-33,6%"     -> -33.6
        "IPCA + 13,69%" -> 13.69
        "+426bps"    -> None (not a simple percentage)
    """
    if not text or not text.strip():
        return None

    text = text.strip()

    # Handle "IPCA + X%" or "IPCA+X%" pattern
    m = re.search(r"IPCA\s*\+\s*([\d.,]+)\s*%", text, re.IGNORECASE)
    if m:
        return _parse_br_number(m.group(1))

    # Handle simple percentage: "-33,6%" or "14,8%"
    m = re.search(r"(-?[\d.,]+)\s*%", text)
    if m:
        return _parse_br_number(m.group(1))

    return None


def parse_ipca_plus(text: str) -> Optional[float]:
    """Parse a TIR/IRR string that is expressed as IPCA + X%.

    Examples:
        "IPCA + 13,69%"  -> 13.69
        "14,8"           -> 14.8
        "IPCA + 30,7%"   -> 30.7
    """
    if not text:
        return None

    text = text.strip()

    # Try IPCA + X% pattern first
    m = re.search(r"IPCA\s*\+\s*([\d.,]+)\s*%?", text, re.IGNORECASE)
    if m:
        return _parse_br_number(m.group(1))

    # Fallback: just a number
    m = re.search(r"([\d.,]+)", text)
    if m:
        return _parse_br_number(m.group(1))

    return None


def parse_integer(text: str) -> Optional[int]:
    """Parse an integer from text, handling Brazilian formatting.

    Examples:
        "16.938.939" -> 16938939
        "5.282"      -> 5282
        "4.360"      -> 4360
    """
    if not text:
        return None
    text = text.strip().replace(" ", "")
    text = re.sub(r"[^\d.]", "", text)
    if not text:
        return None
    # If only dots, they are thousand separators
    text = text.replace(".", "")
    try:
        return int(text)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

MONTHS_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6,
    "julho": 7, "agosto": 8, "setembro": 9,
    "outubro": 10, "novembro": 11, "dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4,
    "mai": 5, "jun": 6, "jul": 7, "ago": 8,
    "set": 9, "out": 10, "nov": 11, "dez": 12,
}


def parse_date_pt(text: str) -> Optional[str]:
    """Parse a Portuguese date string to YYYY-MM format.

    Examples:
        "Fevereiro/2026"      -> "2026-02"
        "fev/26"              -> "2026-02"
        "Fevereiro de 2026"   -> "2026-02"
        "FEVEREIRO 2026"      -> "2026-02"
    """
    if not text:
        return None

    text = text.strip().lower()

    # "Mes/YYYY" or "Mes YYYY" or "Mes de YYYY"
    for month_name, month_num in MONTHS_PT.items():
        pattern = rf"{re.escape(month_name)}\s*(?:/|de\s+|\s+)(\d{{2,4}})"
        m = re.search(pattern, text)
        if m:
            year = int(m.group(1))
            if year < 100:
                year += 2000
            return f"{year:04d}-{month_num:02d}"

    # "MM/YYYY" numeric
    m = re.search(r"(\d{1,2})\s*/\s*(\d{4})", text)
    if m:
        return f"{int(m.group(2)):04d}-{int(m.group(1)):02d}"

    return None


def parse_full_date(text: str) -> Optional[str]:
    """Parse a full date to ISO format YYYY-MM-DD.

    Examples:
        "27/02/2026" -> "2026-02-27"
        "28/02/2026" -> "2026-02-28"
    """
    if not text:
        return None

    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text.strip())
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{year:04d}-{month:02d}-{day:02d}"

    return None


# ---------------------------------------------------------------------------
# Text extraction helpers
# ---------------------------------------------------------------------------

def extract_between(text: str, start: str, end: str) -> Optional[str]:
    """Extract text between two markers (case-insensitive)."""
    pattern = re.escape(start) + r"(.*?)" + re.escape(end)
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else None


def extract_after(text: str, marker: str, max_chars: int = 200) -> Optional[str]:
    """Extract text after a marker, up to max_chars or next newline."""
    pattern = re.escape(marker) + r"\s*(.*)"
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        result = m.group(1).strip()[:max_chars]
        # Stop at double newline
        nl = result.find("\n\n")
        if nl > 0:
            result = result[:nl]
        return result.strip() if result else None
    return None


def find_value_near(text: str, label: str, max_distance: int = 100) -> Optional[str]:
    """Find a numeric value near a label in text."""
    idx = text.lower().find(label.lower())
    if idx < 0:
        return None

    # Search forward from the label
    snippet = text[idx:idx + max_distance]
    # Look for R$ value or percentage or plain number
    m = re.search(r"R\$\s*[\d.,]+(?:\s*(?:milh|bilh|mil)[\wõã]*)?|[\d.,]+\s*%|[\d.,]+", snippet[len(label):])
    return m.group(0).strip() if m else None


# ---------------------------------------------------------------------------
# Abstract base parser
# ---------------------------------------------------------------------------

class BaseFundParser(ABC):
    """Abstract base class for fund-specific parsers."""

    FUND_ID: str = ""
    TICKER: str = ""
    PARSER_VERSION: str = "1.0"

    def parse(self, raw_json_path: Path) -> FundReport:
        """Parse a raw extracted JSON into a FundReport."""
        with open(raw_json_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        return self.parse_raw(raw_data)

    @abstractmethod
    def parse_raw(self, raw_data: dict) -> FundReport:
        """Parse raw extracted data into a structured FundReport.

        Args:
            raw_data: dict with keys 'metadata' and 'pages', where each page
                      has 'page_num', 'text', and optionally 'tables'.
        """
        ...

    def _get_full_text(self, raw_data: dict) -> str:
        """Concatenate all page texts into a single string."""
        pages = raw_data.get("pages", [])
        return "\n\n".join(p.get("text", "") for p in pages)

    def _get_page_text(self, raw_data: dict, page_num: int) -> str:
        """Get text for a specific page (1-indexed)."""
        pages = raw_data.get("pages", [])
        for p in pages:
            if p.get("page_num") == page_num:
                return p.get("text", "")
        return ""

    def _get_page_tables(self, raw_data: dict, page_num: int) -> list[list[list[str]]]:
        """Get tables for a specific page (1-indexed)."""
        pages = raw_data.get("pages", [])
        for p in pages:
            if p.get("page_num") == page_num:
                return p.get("tables", [])
        return []

    def _get_metadata(self, raw_data: dict) -> dict:
        """Get metadata from raw data."""
        return raw_data.get("metadata", {})
