"""Serves the Timeline page and the runs it reads (spec 12.3).

The page is static and offline: everything it needs is in `static/index.html`. This server
only lists the runs in a log directory, streams a run's events, and forwards the operator's
two levers to the Brain's API so the browser never has to talk to a second origin.
"""

import json
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse

STATIC = Path(__file__).resolve().parent / "static"
INDEX = STATIC / "index.html"


def _runs(log_dir: Path) -> list[dict[str, Any]]:
    out = []
    for path in sorted(log_dir.glob("*.jsonl"), key=lambda p: p.name, reverse=True):
        summary_path = path.with_name(f"{path.stem}-summary.json")
        summary = None
        if summary_path.is_file():
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except ValueError:
                summary = None
        out.append({
            "name": path.name,
            "scenario": path.name.split("-", 1)[0],
            "bytes": path.stat().st_size,
            "summary": summary,
        })
    return out


def _run_path(log_dir: Path, name: str) -> Path:
    """The log a request names, refusing anything that is not a run in this directory."""
    candidate = (log_dir / name).resolve()
    if candidate.parent != log_dir.resolve() or candidate.suffix != ".jsonl":
        raise HTTPException(status_code=400, detail="a run is one .jsonl file in the log directory")
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"no run '{name}'")
    return candidate


def _events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_bytes().splitlines() if line.strip()]


def build_timeline_app(log_dir: Path, brain_api_url: str = "http://localhost:8100") -> FastAPI:
    log_dir = Path(log_dir)
    app = FastAPI(title="City Brain Timeline", version="1", docs_url="/docs")
    app.state.log_dir = log_dir
    app.state.brain_api_url = brain_api_url.rstrip("/")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(INDEX, media_type="text/html")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "log_dir": str(log_dir), "brain_api": app.state.brain_api_url}

    @app.get("/api/runs")
    def runs() -> dict:
        log_dir.mkdir(parents=True, exist_ok=True)
        return {"runs": _runs(log_dir), "brain_api": app.state.brain_api_url}

    @app.get("/api/events")
    def events(run: str = Query(...), stream: bool = False) -> Any:
        path = _run_path(log_dir, run)
        if not stream:
            return {"run": run, "events": _events(path)}

        def body():
            for event in _events(path):
                yield f"event: brain\ndata: {json.dumps(event, separators=(',', ':'))}\n\n"
            yield "event: end\ndata: {}\n\n"

        return StreamingResponse(body(), media_type="text/event-stream")

    @app.get("/api/summary")
    def summary(run: str = Query(...)) -> dict:
        path = _run_path(log_dir, run)
        summary_path = path.with_name(f"{path.stem}-summary.json")
        if not summary_path.is_file():
            raise HTTPException(status_code=404, detail=f"no summary for '{run}'")
        return json.loads(summary_path.read_text(encoding="utf-8"))

    # ----- the operator's levers, forwarded so the page stays on one origin -----

    async def _forward(request: Request, method: str, route: str, authorization: str) -> Any:
        if not authorization:
            raise HTTPException(status_code=401, detail="operator token required")
        url = f"{app.state.brain_api_url}{route}"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                body = await request.body()
                reply = await client.request(
                    method, url, content=body or None,
                    headers={"Authorization": authorization, "Content-Type": "application/json"})
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"the Brain API is unreachable: {exc}") from exc
        if reply.status_code >= 400:
            raise HTTPException(status_code=reply.status_code, detail=reply.text)
        return reply.json()

    @app.get("/api/live/{path:path}")
    async def live(path: str, request: Request) -> Any:
        """Read-through to the Brain's own API, for the live panes."""
        return await _forward(request, "GET", f"/{path}", request.headers.get("authorization", "-"))

    @app.post("/api/control/{action}")
    async def control(action: str, request: Request, authorization: str = Header(default="")) -> Any:
        if action not in ("pause", "resume"):
            raise HTTPException(status_code=404, detail=f"no control '{action}'")
        return await _forward(request, "POST", f"/v1/control/{action}", authorization)

    @app.post("/api/approvals/{action_id}")
    async def approve(action_id: str, request: Request, authorization: str = Header(default="")) -> Any:
        return await _forward(request, "POST", f"/v1/approvals/{action_id}", authorization)

    return app
