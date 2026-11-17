"""Edge gateway: proxies requests to the currently routed upstream and
exposes the operations API + web console for on-duty target switching."""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db

STATIC_DIR = Path(__file__).parent / "static"


def parse_upstreams() -> dict[str, str]:
    raw = os.environ.get(
        "UPSTREAMS", "alpha=http://127.0.0.1:9001;beta=http://127.0.0.1:9002"
    )
    out: dict[str, str] = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part:
            continue
        name, _, url = part.partition("=")
        out[name.strip()] = url.strip().rstrip("/")
    return out


UPSTREAMS = parse_upstreams()
INITIAL_TARGET = os.environ.get("INITIAL_TARGET") or next(iter(UPSTREAMS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.boot_info = db.init_db(UPSTREAMS, INITIAL_TARGET)
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.0))
    yield
    await app.state.http.aclose()


app = FastAPI(title="edge-gateway", lifespan=lifespan)

PROBE_ERRORS = {
    "probe_invalid",
    "probe_stale",
}
CONFLICT_ERRORS = {"version_conflict"}


def error_response(res: dict) -> JSONResponse:
    if res["error"] in CONFLICT_ERRORS:
        code = 409
    elif res["error"] in PROBE_ERRORS:
        code = 400
    elif res["error"] == "unknown_target":
        code = 404
    else:
        code = 400
    return JSONResponse(status_code=code, content=res)


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/state")
def state():
    s = db.get_state()
    s["boot"] = app.state.boot_info
    return s


class CallRequest(BaseModel):
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    path: str = "/work"


@app.post("/api/call")
async def call(req: CallRequest):
    """Admit a request and proxy it to the address that is current *at
    admission time*. The full upstream response is fetched from that original
    address even if a switch (or registry change) commits while it is in
    flight."""
    path = req.path if req.path.startswith("/") else "/work"
    if "://" in path or ".." in path:
        return JSONResponse(status_code=400, content={"ok": False, "error": "bad_path"})
    try:
        admission = db.admit(path)
    except RuntimeError as exc:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": f"route_unavailable: {exc}"},
        )
    started = time.monotonic()
    try:
        params = {} if req.delay_ms is None else {"delay_ms": req.delay_ms}
        resp = await app.state.http.get(f"{admission['url']}{path}", params=params)
        try:
            body = resp.json()
        except ValueError:
            body = {"text": resp.text[:500]}
        return {
            "ok": resp.status_code == 200,
            "status": resp.status_code,
            "request_id": admission["id"],
            "target": admission["target"],
            "url": admission["url"],
            "route_version": admission["version"],
            "duration_ms": int((time.monotonic() - started) * 1000),
            "upstream": body,
        }
    except httpx.HTTPError as exc:
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": f"upstream_error: {exc}",
                "request_id": admission["id"],
                "target": admission["target"],
                "url": admission["url"],
                "route_version": admission["version"],
            },
        )
    finally:
        db.finish(admission["id"])


class ProbeRequest(BaseModel):
    target: str
    base_version: int


async def _fetch(reg: dict) -> tuple[bool, dict]:
    try:
        health = await app.state.http.get(f"{reg['url']}/health", timeout=2.5)
        work = await app.state.http.get(
            f"{reg['url']}/work", params={"delay_ms": 0}, timeout=5.0
        )
        ok = health.status_code == 200 and work.status_code == 200
        return ok, {"health_status": health.status_code, "work_status": work.status_code}
    except httpx.HTTPError as exc:
        return False, {"error": str(exc)}


@app.post("/api/switch/probe")
async def probe(req: ProbeRequest):
    """Probe a candidate target. Never mutates the route: while probing, new
    requests keep flowing to the old target, and a failed probe leaves the
    original route untouched.

    The recorded probe is bound to the registered URL/revision observed before
    the network check and to the route version current when it started. If the
    registration or the route moves while the check is running, the commit will
    refuse the probe rather than accept evidence for a different address."""
    route = db.get_route()
    reg = db.upstream(req.target)
    if reg is None:
        return error_response({"ok": False, "error": "unknown_target"})
    same_target = reg["name"] == route["current_target"]
    address_changed = (
        reg["url"] != route.get("current_url")
        or reg["revision"] != route.get("current_revision")
    )
    # Probing the current target is only meaningful when its registered address
    # moved away from the address the route was confirmed against: the effective
    # address is pinned, so live traffic is unaffected and a successful probe of
    # the new address is the only way to adopt it.
    if same_target and not address_changed:
        return {
            "ok": False,
            "error": "target_is_current",
            "current_version": route["version"],
        }
    ok, detail = await _fetch(reg)
    rec = db.record_probe(
        req.target,
        req.base_version,
        ok,
        detail,
        url=reg["url"],
        upstream_revision=reg["revision"],
        route_version=route["version"],
    )
    current = db.get_route()
    target_now = db.upstream(req.target)
    version_matches = current["version"] == req.base_version
    address_matches = (
        target_now is not None
        and target_now["url"] == reg["url"]
        and target_now["revision"] == reg["revision"]
    )
    return {
        "ok": ok,
        "probe_id": rec["id"],
        "target": req.target,
        "adopts_new_address": same_target and address_changed,
        "url": reg["url"],
        "upstream_revision": reg["revision"],
        "base_version": req.base_version,
        "current_version": current["version"],
        "version_matches": version_matches,
        "address_matches": address_matches,
        "committable": ok and version_matches and address_matches,
        "detail": detail,
    }


class CommitRequest(BaseModel):
    target: str
    base_version: int
    probe_id: str


@app.post("/api/switch/commit")
def commit(req: CommitRequest):
    """Confirm a switch. The commit is re-validated inside a single write
    transaction: the route version must still be ``base_version``, and the
    probe must be a success for the currently registered URL/revision of the
    target. A stale commit, a probe invalidated by an address change, or a
    route moved by a peer leaves the route and version untouched."""
    res = db.commit_switch(req.target, req.base_version, req.probe_id)
    if not res["ok"]:
        return error_response(res)
    return res
