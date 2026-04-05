from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime
from tracepipe_ai.lineage_archive import LineageArchive

router = APIRouter(prefix="/lineage-archive", tags=["lineage-archive"])


class ArchiveRequest(BaseModel):
    id: Optional[str] = None
    event_time: datetime
    table_name: str
    upstream_tables: List[str] = []
    downstream_tables: List[str] = []
    operation_type: str
    user_name: str
    metadata: Dict[str, Any] = {}


class ArchiveResponse(BaseModel):
    event_id: str
    archived_at: datetime


class QueryResponse(BaseModel):
    events: List[Dict[str, Any]]
    count: int


@router.post("/archive", response_model=ArchiveResponse)
async def archive_lineage(request: ArchiveRequest):
    archive = LineageArchive()
    try:
        event_id = archive.archive_lineage(request.model_dump())
        return ArchiveResponse(
            event_id=event_id,
            archived_at=datetime.now()
        )
    finally:
        archive.close()


@router.get("/query", response_model=QueryResponse)
async def query_lineage(
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    table_name: Optional[str] = None
):
    archive = LineageArchive()
    try:
        events = archive.query_lineage(start_date, end_date, table_name)
        return QueryResponse(events=events, count=len(events))
    finally:
        archive.close()


@router.get("/table/{table_name}", response_model=QueryResponse)
async def get_table_history(table_name: str):
    archive = LineageArchive()
    try:
        events = archive.get_table_history(table_name)
        return QueryResponse(events=events, count=len(events))
    finally:
        archive.close()
