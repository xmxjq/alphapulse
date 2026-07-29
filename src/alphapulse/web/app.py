from __future__ import annotations

import base64
import binascii
import ipaddress
import json
import re
import time
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from toon_format import encode

from alphapulse.runtime.config import Settings
from alphapulse.runtime.agent_pool import AgentPoolStore
from alphapulse.web.models import (
    AgentCompleteRequest,
    AgentFailRequest,
    AgentHeartbeatRequest,
    AgentLeaseRequest,
    AgentPoolResponse,
    ErrorsResponse,
    GubaBoardsResponse,
    GubaNextCrawlResponse,
    ReportResponse,
    PostDetailResponse,
    PostsResponse,
    ProxyPoolResponse,
    RunsResponse,
    SeedsResponse,
    StatusResponse,
)
from alphapulse.web.queries import ALLOWED_SOURCES, WebQueries, build_queries


def _agent_ip_address(request: Request) -> str | None:
    candidates = [
        request.headers.get("CF-Connecting-IP"),
        (request.headers.get("X-Forwarded-For") or "").split(",", 1)[0].strip(),
        request.client.host if request.client else None,
    ]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return str(ipaddress.ip_address(candidate))
        except ValueError:
            continue
    return None


STATIC_DIR = Path(__file__).resolve().parent / "static"
ENTITY_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TOON_MEDIA_TYPE = "text/toon"


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


def create_app(
    settings: Settings,
    queries: WebQueries | None = None,
    agent_pool: AgentPoolStore | None = None,
) -> FastAPI:
    app = FastAPI(title="AlphaPulse dashboard", version="0.1.0")
    queries = queries or build_queries(settings)
    agent_pool = agent_pool or AgentPoolStore(settings.crawl.agent_pool)

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

    @app.get("/api/proxy-pool", response_model=ProxyPoolResponse)
    def proxy_pool(
        hours: int = Query(default=24, ge=1, le=168),
        q: WebQueries = Depends(get_queries),
    ) -> ProxyPoolResponse:
        return q.proxy_pool(hours)

    @app.get("/api/agent-pool", response_model=AgentPoolResponse)
    def agent_pool_status() -> AgentPoolResponse:
        snapshot = agent_pool.snapshot()
        paid_slots = settings.sources.guba.concurrent_paid_requests
        agent_slot_limit = settings.sources.guba.concurrent_agent_requests
        active_agent_slots = min(
            agent_slot_limit,
            int(snapshot["online_capacity"]),
        )
        return AgentPoolResponse.model_validate(
            {
                **snapshot,
                "routing_mode": "hybrid",
                "paid_slots": paid_slots,
                "agent_slot_limit": agent_slot_limit,
                "combined_capacity": paid_slots + active_agent_slots,
            }
        )

    def authenticated_agent(
        agent_id: str = Header(alias="X-AlphaPulse-Agent-ID"),
        agent_token: str = Header(alias="X-AlphaPulse-Agent-Token"),
    ) -> str:
        if not settings.crawl.agent_pool.enabled:
            raise HTTPException(status_code=503, detail="Agent pool is disabled")
        if not agent_pool.authenticate(agent_id, agent_token):
            raise HTTPException(status_code=401, detail="Invalid agent credentials")
        return agent_id

    @app.post("/api/agent/v1/heartbeat")
    def agent_heartbeat(
        payload: AgentHeartbeatRequest,
        request: Request,
        agent_id: str = Depends(authenticated_agent),
    ) -> dict[str, object]:
        if payload.agent_id != agent_id:
            raise HTTPException(status_code=400, detail="Agent id mismatch")
        agent_pool.heartbeat(
            agent_id=agent_id,
            version=payload.version,
            os_name=payload.os,
            arch=payload.arch,
            capabilities=payload.capabilities,
            max_concurrency=payload.max_concurrency,
            ip_address=_agent_ip_address(request),
        )
        return {"ok": True}

    @app.post("/api/agent/v1/jobs/lease")
    def agent_lease(
        payload: AgentLeaseRequest,
        request: Request,
        agent_id: str = Depends(authenticated_agent),
    ) -> Response:
        if payload.agent_id != agent_id:
            raise HTTPException(status_code=400, detail="Agent id mismatch")
        agent_pool.heartbeat(
            agent_id=agent_id,
            version=payload.version,
            os_name=payload.os,
            arch=payload.arch,
            capabilities=payload.capabilities,
            max_concurrency=payload.max_concurrency,
            ip_address=_agent_ip_address(request),
        )
        deadline = time.monotonic() + payload.wait_seconds
        while True:
            job = agent_pool.lease_job(
                agent_id=agent_id,
                capabilities=payload.capabilities,
            )
            if job is not None:
                return Response(
                    content=json.dumps(job, separators=(",", ":")),
                    media_type="application/json",
                )
            if time.monotonic() >= deadline:
                return Response(status_code=204)
            time.sleep(0.5)

    @app.post("/api/agent/v1/jobs/{job_id}/complete")
    def agent_complete(
        job_id: str,
        payload: AgentCompleteRequest,
        agent_id: str = Depends(authenticated_agent),
    ) -> dict[str, object]:
        try:
            body = base64.b64decode(payload.body_base64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise HTTPException(status_code=400, detail="Invalid response body") from exc
        try:
            accepted = agent_pool.complete_job(
                agent_id=agent_id,
                job_id=job_id,
                lease_id=payload.lease_id,
                status_code=payload.status_code,
                final_url=payload.final_url,
                headers=payload.headers,
                body=body,
                duration_ms=payload.duration_ms,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not accepted:
            raise HTTPException(status_code=409, detail="Job lease is no longer valid")
        return {"ok": True}

    @app.post("/api/agent/v1/jobs/{job_id}/fail")
    def agent_fail(
        job_id: str,
        payload: AgentFailRequest,
        agent_id: str = Depends(authenticated_agent),
    ) -> dict[str, object]:
        accepted = agent_pool.fail_job(
            agent_id=agent_id,
            job_id=job_id,
            lease_id=payload.lease_id,
            error_message=payload.error_message,
            retryable=payload.retryable,
        )
        if not accepted:
            raise HTTPException(status_code=409, detail="Job lease is no longer valid")
        return {"ok": True}

    @app.get(
        "/api/llm/guba/report/{date}",
        response_class=Response,
        responses={200: {"content": {TOON_MEDIA_TYPE: {}}}},
    )
    def guba_llm_report(
        date: str,
        limit: int = Query(default=500, ge=1, le=5_000),
        include_comments: bool = Query(default=True),
        max_comments_per_post: int = Query(default=100, ge=0, le=500),
        q: WebQueries = Depends(get_queries),
    ) -> Response:
        if not DATE_RE.match(date):
            raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
        try:
            payload = q.guba_llm_report(
                date,
                limit=limit,
                include_comments=include_comments,
                max_comments_per_post=max_comments_per_post,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        return Response(content=encode(payload), media_type=TOON_MEDIA_TYPE)

    @app.get("/api/guba/report/{date}", response_model=ReportResponse)
    def guba_report(date: str, q: WebQueries = Depends(get_queries)) -> ReportResponse:
        if not DATE_RE.match(date):
            raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
        try:
            return q.guba_daily_report(date)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc

    @app.get("/api/tgb/report/{date}", response_model=ReportResponse)
    def tgb_report(date: str, q: WebQueries = Depends(get_queries)) -> ReportResponse:
        if not DATE_RE.match(date):
            raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
        try:
            return q.tgb_daily_report(date)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc

    @app.get(
        "/api/llm/tgb/report/{date}",
        response_class=Response,
        responses={200: {"content": {TOON_MEDIA_TYPE: {}}}},
    )
    def tgb_llm_report(
        date: str,
        limit: int = Query(default=500, ge=1, le=5_000),
        include_comments: bool = Query(default=True),
        max_comments_per_post: int = Query(default=100, ge=0, le=500),
        q: WebQueries = Depends(get_queries),
    ) -> Response:
        if not DATE_RE.match(date):
            raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
        try:
            payload = q.tgb_llm_report(
                date,
                limit=limit,
                include_comments=include_comments,
                max_comments_per_post=max_comments_per_post,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except NotImplementedError as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        return Response(content=encode(payload), media_type=TOON_MEDIA_TYPE)

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
    @app.get("/report/{source}/{date}", include_in_schema=False)
    def report(date: str | None = None, source: str | None = None) -> FileResponse:
        # The static page reads the source/date from the URL path itself.
        return FileResponse(STATIC_DIR / "report.html")

    return app
