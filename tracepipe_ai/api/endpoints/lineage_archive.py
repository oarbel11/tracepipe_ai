from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List, Dict, Any
from tracepipe_ai.lineage_archive import LineageArchive

router = APIRouter(prefix="/api/lineage/archive", tags=["lineage_archive"])


class ArchiveRequest(BaseModel):
    event_id: Optional[str] = None
    event_type: str
    source_table: str
    target_table: str
    transformation: Optional[str] = None
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None


class HistoricalQueryRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    table_name: Optional[str] = None


class ComplianceReportRequest(BaseModel):
    start_date: datetime
    end_date: datetime


def get_archive():
    return LineageArchive()


@router.post("/store")
async def archive_lineage(request: ArchiveRequest, archive: LineageArchive = Depends(get_archive)):
    try:
        event_id = archive.archive_lineage(request.model_dump())
        return {"status": "success", "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def query_historical(
    request: HistoricalQueryRequest,
    archive: LineageArchive = Depends(get_archive)
):
    try:
        results = archive.query_historical_lineage(
            request.start_date, request.end_date, request.table_name
        )
        return {"status": "success", "results": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compliance-report")
async def get_compliance_report(
    request: ComplianceReportRequest,
    archive: LineageArchive = Depends(get_archive)
):
    try:
        report = archive.get_compliance_report(request.start_date, request.end_date)
        return {"status": "success", "report": report}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
