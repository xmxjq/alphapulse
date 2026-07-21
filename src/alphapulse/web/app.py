from __future__ import annotations

import re
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from alphapulse.runtime.config import Settings
from alphapulse.web.models import (
    ErrorsResponse,
    GubaBoardsResponse,
    GubaNextCrawlResponse,
    GubaReportResponse,
    PostDetailResponse,
    PostsResponse,
    RunsResponse,
    SeedsResponse,
    StatusResponse,
)
from alphapulse.web.queries import ALLOWED_SOURCES, WebQueries, build_queries


STATIC_DIR = Path(__file__).resolve().parent / "static"
ENTITY_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_source(source: str | None) -> str | None:
    if source is None:
        return None
    if source not in ALLOWED_SOURCES:
        raise HTTPException(status_code=400, detail=f"Unknown source: {source}")
    return source


def _validate_entity_id(entity_id: str) -> str:
    if not ENTITY_ID_RE.match(entity_id):
        raise HTTPException(status_code=400, detail="Invalid entity id")
    return entity_id


def create_app(settings: Settings, queries: WebQueries | None = None) -> FastAPI:
    app = FastAPI(title="AlphaPulse dashboard", version="0.1.0")
    queries = queries or build_queries(settings)

    def get_queries() -> WebQueries:
        return queries

    @app.get("/api/status", response_model=StatusResponse)
    def status(q: WebQueries = Depends(get_queries)) -> StatusResponse:
        return q.status()

    @app.get("/api/runs", response_model=RunsResponse)
    def runs(
        limit: int = Query(default=20, ge=1, le=200),
        q: WebQueries = Depends(get_queries),
    ) -> RunsResponse:
        return RunsResponse(runs=q.reader.list_runs(limit))

    @app.get("/api/errors", response_model=ErrorsResponse)
    def errors(
        limit: int = Query(default=50, ge=1, le=200),
        source: str | None = Query(default=None),
        q: WebQueries = Depends(get_queries),
    ) -> ErrorsResponse:
        return ErrorsResponse(errors=q.reader.list_errors(limit, _validate_source(source)))

    @app.get("/api/seeds", response_model=SeedsResponse)
    def seeds(q: WebQueries = Depends(get_queries)) -> SeedsResponse:
        return SeedsResponse(seed_sets=q.seed_set_summaries())

    @app.get("/api/posts", response_model=PostsResponse)
    def posts(
        limit: int = Query(default=50, ge=1, le=200),
        offset: int = Query(default=0, ge=0, le=10_000),
        source: str | None = Query(default=None),
        q: WebQueries = Depends(get_queries),
    ) -> PostsResponse:
        items = q.reader.list_posts(_validate_source(source), limit, offset)
        return PostsResponse(posts=items, limit=limit, offset=offset)

    @app.get("/api/guba/boards", response_model=GubaBoardsResponse)
    def guba_boards(
        limit: int = Query(default=50, ge=1, le=200),
        q: WebQueries = Depends(get_queries),
    ) -> GubaBoardsResponse:
        return GubaBoardsResponse(boards=q.guba_boards(limit))

    @app.get("/api/guba/next-crawl", response_model=GubaNextCrawlResponse)
    def guba_next_crawl(q: WebQueries = Depends(get_queries)) -> GubaNextCrawlResponse:
        return q.guba_next_crawl()

    @app.get("/api/guba/report/{date}", response_model=GubaReportResponse)
    def guba_report(date: str, q: WebQueries = Depends(get_queries)) -> GubaReportResponse:
        if not DATE_RE.match(date):
            raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
        try:
            return q.guba_daily_report(date)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc

    @app.get("/api/posts/{source}/{entity_id}", response_model=PostDetailResponse)
    def post_detail(
        source: str,
        entity_id: str,
        q: WebQueries = Depends(get_queries),
    ) -> PostDetailResponse:
        source = _validate_source(source) or ""
        entity_id = _validate_entity_id(entity_id)
        detail = q.post_detail(source, entity_id)
        if detail is None:
            raise HTTPException(status_code=404, detail="Post not found")
        return detail

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/report", include_in_schema=False)
    @app.get("/report/{date}", include_in_schema=False)
    def report(date: str | None = None) -> FileResponse:
        return FileResponse(STATIC_DIR / "report.html")

    return app
