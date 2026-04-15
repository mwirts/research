"""CLI orchestrator for the FIP-IE ETL transformation pipeline.

Usage:
    python -m etl.transformer.run_pipeline --all
    python -m etl.transformer.run_pipeline --fund pfin11
    python -m etl.transformer.run_pipeline --fund pfin11 --date 2026-02
    python -m etl.transformer.run_pipeline --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from etl.transformer.extract_text import extract_fund, extract_pdf
from etl.transformer.load_db import init_db, load_report, log_extraction
from etl.transformer.models import ExtractionLogEntry
from etl.transformer.validate import validate_report

logger = logging.getLogger(__name__)

# Registry of available parsers
FUND_PARSERS = {}


def _register_parsers():
    """Lazily import and register available fund parsers."""
    global FUND_PARSERS
    if FUND_PARSERS:
        return

    parser_map = {
        "pfin11": "etl.transformer.parsers.pfin11:PFIN11Parser",
        "brzp11": "etl.transformer.parsers.brzp11:BRZP11Parser",
        "azin11": "etl.transformer.parsers.azin11:AZIN11Parser",
        "ppei11": "etl.transformer.parsers.ppei11:PPEI11Parser",
        "pice11": "etl.transformer.parsers.pice11:PICE11Parser",
        "vigt11": "etl.transformer.parsers.vigt11:VIGT11Parser",
    }
    for fund_id, class_path in parser_map.items():
        try:
            module_path, class_name = class_path.rsplit(":", 1)
            import importlib
            mod = importlib.import_module(module_path)
            FUND_PARSERS[fund_id] = getattr(mod, class_name)()
        except Exception as e:
            logger.warning("Could not load parser for %s: %s", fund_id, e)


# Default paths
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "funds"
EXTRACTED_DIR = PROJECT_ROOT / "data" / "processed" / "extracted"
DB_PATH = PROJECT_ROOT / "data" / "processed" / "fund_data.db"


def process_single_report(
    fund_id: str,
    year: int,
    month: int,
    force_extract: bool = False,
    dry_run: bool = False,
) -> bool:
    """Process a single monthly report through the full pipeline.

    Returns True if processing was successful.
    """
    _register_parsers()

    parser = FUND_PARSERS.get(fund_id)
    if not parser:
        logger.warning("No parser available for %s, skipping", fund_id)
        return False

    pdf_filename = f"{fund_id}_relatorio_mensal_{year:04d}_{month:02d}.pdf"
    pdf_path = RAW_DIR / fund_id / "monthly_report" / pdf_filename

    if not pdf_path.exists():
        logger.warning("PDF not found: %s", pdf_path)
        return False

    raw_json_path = EXTRACTED_DIR / fund_id / f"{fund_id}_{year:04d}_{month:02d}_raw.json"
    structured_json_path = EXTRACTED_DIR / fund_id / f"{fund_id}_{year:04d}_{month:02d}_structured.json"

    if dry_run:
        status = "exists" if raw_json_path.exists() else "pending"
        logger.info("[DRY RUN] %s %04d-%02d: %s", fund_id.upper(), year, month, status)
        return True

    started_at = datetime.now()
    ref_date = f"{year:04d}-{month:02d}"

    try:
        # Step 1: Extract text from PDF (cached)
        if not raw_json_path.exists() or force_extract:
            output_dir = EXTRACTED_DIR / fund_id
            extract_pdf(pdf_path, output_dir)
            logger.info("Extracted text: %s", raw_json_path.name)
        else:
            logger.debug("Using cached extraction: %s", raw_json_path.name)

        # Step 2: Parse structured data
        report = parser.parse(raw_json_path)

        # Step 3: Validate
        validation = validate_report(report)

        # Step 4: Save structured JSON
        structured_json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(structured_json_path, "w", encoding="utf-8") as f:
            json.dump(report.model_dump(mode="json"), f, ensure_ascii=False, indent=2, default=str)

        # Step 5: Load to database
        conn = init_db(DB_PATH)
        try:
            load_report(conn, report)

            # Log extraction
            log_extraction(conn, ExtractionLogEntry(
                fund_id=fund_id,
                source_pdf=pdf_filename,
                reference_date=ref_date,
                started_at=started_at,
                completed_at=datetime.now(),
                status="success" if validation.is_valid else "partial",
                parser_version=parser.PARSER_VERSION,
                warnings=json.dumps(validation.warnings) if validation.warnings else None,
                errors=json.dumps(validation.errors) if validation.errors else None,
            ))
            conn.commit()
        finally:
            conn.close()

        logger.info(
            "Processed %s %s: %s (%d warnings)",
            fund_id.upper(), ref_date,
            "OK" if validation.is_valid else "PARTIAL",
            len(validation.warnings),
        )
        return True

    except Exception as e:
        logger.error("Failed to process %s %04d-%02d: %s", fund_id, year, month, e)

        # Log failure
        try:
            conn = init_db(DB_PATH)
            log_extraction(conn, ExtractionLogEntry(
                fund_id=fund_id,
                source_pdf=pdf_filename,
                reference_date=ref_date,
                started_at=started_at,
                completed_at=datetime.now(),
                status="failed",
                parser_version=parser.PARSER_VERSION if parser else None,
                errors=str(e),
            ))
            conn.close()
        except Exception:
            pass

        return False


def process_fund(fund_id: str, force: bool = False, dry_run: bool = False) -> dict:
    """Process all reports for a fund. Returns summary stats."""
    pdf_dir = RAW_DIR / fund_id / "monthly_report"
    if not pdf_dir.exists():
        logger.warning("No PDF directory for %s", fund_id)
        return {"fund": fund_id, "total": 0, "success": 0, "failed": 0}

    import re
    stats = {"fund": fund_id, "total": 0, "success": 0, "failed": 0, "skipped": 0}

    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        m = re.match(r"\w+_relatorio_mensal_(\d{4})_(\d{2})\.pdf", pdf_path.name)
        if not m:
            continue

        year, month = int(m.group(1)), int(m.group(2))
        stats["total"] += 1

        ok = process_single_report(fund_id, year, month, force_extract=force, dry_run=dry_run)
        if ok:
            stats["success"] += 1
        else:
            stats["failed"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="FIP-IE ETL Transformation Pipeline",
    )
    parser.add_argument("--fund", type=str, help="Process a specific fund (e.g., pfin11)")
    parser.add_argument("--date", type=str, help="Process a specific month (YYYY-MM)")
    parser.add_argument("--all", action="store_true", help="Process all funds")
    parser.add_argument("--force", action="store_true", help="Force re-extraction")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    _register_parsers()
    available = list(FUND_PARSERS.keys())
    logger.info("Available parsers: %s", ", ".join(available) if available else "none")

    if args.fund and args.date:
        # Single report
        parts = args.date.split("-")
        year, month = int(parts[0]), int(parts[1])
        ok = process_single_report(
            args.fund.lower(), year, month,
            force_extract=args.force, dry_run=args.dry_run,
        )
        sys.exit(0 if ok else 1)

    elif args.fund:
        # All reports for a fund
        stats = process_fund(args.fund.lower(), force=args.force, dry_run=args.dry_run)
        logger.info(
            "Fund %s: %d total, %d success, %d failed",
            stats["fund"].upper(), stats["total"], stats["success"], stats["failed"],
        )

    elif args.all:
        # All funds with available parsers
        all_stats = []
        for fund_id in available:
            stats = process_fund(fund_id, force=args.force, dry_run=args.dry_run)
            all_stats.append(stats)

        # Print summary
        print("\n=== Pipeline Summary ===")
        total_pdfs = sum(s["total"] for s in all_stats)
        total_ok = sum(s["success"] for s in all_stats)
        total_fail = sum(s["failed"] for s in all_stats)
        for s in all_stats:
            print(f"  {s['fund'].upper():8s}: {s['success']}/{s['total']} OK, {s['failed']} failed")
        print(f"  {'TOTAL':8s}: {total_ok}/{total_pdfs} OK, {total_fail} failed")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
