from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, List
from tracepipe_ai.lineage_archive import LineageArchive

router = APIRouter(prefix="/api/lineage-archive", tags=["lineage-archive"])


class ArchiveRequest(BaseModel):
    event_id: str
    timestamp: datetime
    source_table: str
    target_table: str
    operation_type: str
    user_name: str
    metadata: Optional[Dict] = None


class QueryResponse(BaseModel):
    events: List[Dict]
    count: int


class StatsResponse(BaseModel):
    total_events: int
    oldest_event: Optional[datetime]


@router.post("/archive")
async def archive_lineage(request: ArchiveRequest) -> Dict:
    archive = LineageArchive()
    try:
        success = archive.archive_lineage(
            request.event_id, request.timestamp, request.source_table,
            request.target_table, request.operation_type,
            request.user_name, request.metadata
        )
        return {"success": success, "event_id": request.event_id}
    finally:
        archive.close()


@router.get("/query", response_model=QueryResponse)
async def query_lineage(
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    table_name: Optional[str] = Query(None)
) -> QueryResponse:
    archive = LineageArchive()
    try:
        events = archive.query_lineage(start_date, end_date, table_name)
        return QueryResponse(events=events, count=len(events))
    finally:
        archive.close()


@router.get("/stats", response_model=StatsResponse)
async def get_statistics() -> StatsResponse:
    archive = LineageArchive()
    try:
        stats = archive.get_statistics()
        return StatsResponse(**stats)
    finally:
        archive.close()
