"""Export endpoints (PowerPoint, PDF)."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["export"])


@router.post("/export/pptx")
def export_pptx():
    """Generate PowerPoint presentation. (Placeholder for Sprint 5)"""
    return {"status": "not_implemented", "message": "PowerPoint export coming in Sprint 5"}
