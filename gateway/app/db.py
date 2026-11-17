"""SQLite persistence for the edge gateway.

Tables:
  upstreams  - registered upstream targets. ``revision`` is bumped every time
               the URL registered for a name changes, so probes made against an
               earlier address can never authorise traffic to a later one -
               including after the address is changed back to its old value.
  gateway_registrations
             - what each gateway instance (stable ``GATEWAY_ID``) last
               registered, per upstream name. This lets a gateway apply its own
               configuration changes on restart without a stale peer silently
               reverting an address change made by another gateway. A restart
               that does not change the address therefore leaves probes alone.
  routes     - single-row table holding the effective target *and effective
               URL/revision* and its version. Traffic follows the effective
               address (the one a probe confirmed), never the live registry, so
               editing a registry address cannot silently redirect live traffic
               to an address that was never probed. A registered address change
               for the current target is adopted only by confirming a probe of
               that new address.
  inflight   - one row per admitted request, including the exact upstream URL
               snapshotted at admission and the gateway instance that admitted
               it. On startup a gateway clears only its *own* previous
               instance's rows, so gateways sharing one SQLite file never erase
               each other's live in-flight bookkeeping.
  probes     - audit log of candidate probes. Each probe is bound to the exact
               registered URL and upstream revision it observed, and to the
               route version current when it ran. A commit accepts it only if
               all of those still match and the probe is within its TTL.

Every read-check-write (registration, admit-snapshot, commit) runs inside a
``BEGIN IMMEDIATE`` transaction, so gateways sharing this SQLite file serialise
them: a registration change or a competing commit that interleaves with a
commit can only make the commit fail, never let it slip through.
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(os.environ.get("GATEWAY_DB", "/data/gateway.db"))
PROBE_TTL_SECONDS = float(os.environ.get("PROBE_TTL_SECONDS", "60"))
# Stable per gateway instance: a restart with the same id is recognised as the
# same participant when it comes back to the shared registry. Deployments that
# share one SQLite file must give every gateway a distinct, stable id.
GATEWAY_ID = os.environ.get("GATEWAY_ID") or socket.gethostname()
BOOT_ID = uuid.uuid4().hex

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(DB_PATH), check_same_thread=False, timeout=10.0, isolation_level=None
    )
    conn.row_factory = sqlite3.Row
    # A peer process may hold the write lock while both boot; retry briefly.
    for attempt in range(50):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError:
            if attempt == 49:
                raise
            time.sleep(0.1)
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextmanager
def _write_tx():
    """Serialise a read-modify-write across threads and across gateway
    processes sharing the SQLite file. IMMEDIATE takes the write lock before
    anything is read, so nothing observed inside the block can change."""
    with _lock:
        conn = _db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")


def _begin_init_transaction(conn: sqlite3.Connection) -> None:
    for attempt in range(50):
        try:
            conn.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError:
            if attempt == 49:
                raise
            time.sleep(0.1)


def init_db(upstreams: dict[str, str], initial_target: str) -> dict:
    """Create/upgrade tables, register this gateway's configured addresses,
    seed the route once, and drop in-flight rows left behind by a previous
    (killed) process.

    A restart with an unchanged address is a registry no-op: revisions do not
    move, so probes still inside their TTL remain usable. Any address change
    (including changing back) bumps that name's revision and invalidates every
    earlier probe for it.
    """
    global _conn
    if not upstreams:
        raise RuntimeError("no upstreams configured")
    if any(not name or not url for name, url in upstreams.items()):
        raise RuntimeError("upstream names and URLs must be non-empty")
    if initial_target not in upstreams:
        raise RuntimeError(f"initial target {initial_target!r} not in upstreams")
    _conn = _connect()
    with _lock:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS upstreams (
                name       TEXT PRIMARY KEY,
                url        TEXT NOT NULL,
                revision   INTEGER NOT NULL DEFAULT 1,
                updated_at REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS routes (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                current_target   TEXT NOT NULL,
                current_url      TEXT,
                current_revision INTEGER,
                version          INTEGER NOT NULL,
                updated_at     REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS inflight (
                id         TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                url        TEXT,
                gateway_id TEXT,
                path       TEXT NOT NULL,
                started_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS probes (
                id              TEXT PRIMARY KEY,
                target          TEXT NOT NULL,
                url             TEXT,
                upstream_rev    INTEGER,
                base_version    INTEGER NOT NULL,
                route_version   INTEGER,
                boot_id         TEXT,
                ok              INTEGER NOT NULL,
                detail          TEXT,
                created_at      REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gateway_registrations (
                gateway_id TEXT NOT NULL,
                name       TEXT NOT NULL,
                url        TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (gateway_id, name)
            );
            """
        )
        _begin_init_transaction(_conn)
        try:
            _migrate_columns(_conn)
            # Databases created before routes carried their effective address:
            # pin the address that was effective *before* this boot applies any
            # configuration change. That keeps the "never silently follow an
            # unprobed address" rule even when an upgrade and an address change
            # happen in the same restart. A brand-new DB is seeded just below.
            legacy_pin = None
            legacy = _conn.execute(
                "SELECT current_target, current_url, current_revision "
                "FROM routes WHERE id = 1"
            ).fetchone()
            if legacy is not None and (
                legacy["current_url"] is None or legacy["current_revision"] is None
            ):
                old = _conn.execute(
                    "SELECT url, revision FROM upstreams WHERE name = ?",
                    (legacy["current_target"],),
                ).fetchone()
                if old is not None:
                    legacy_pin = (old["url"], old["revision"])
            changed = _register_upstreams(_conn, upstreams)
            seeded = False
            if _conn.execute("SELECT COUNT(*) AS c FROM routes").fetchone()["c"] == 0:
                _conn.execute(
                    "INSERT INTO routes "
                    "(id, current_target, current_url, current_revision, version, updated_at) "
                    "VALUES (1, ?, ?, (SELECT revision FROM upstreams WHERE name = ?), 0, ?)",
                    (
                        initial_target,
                        upstreams[initial_target],
                        initial_target,
                        time.time(),
                    ),
                )
                seeded = True
            elif legacy_pin is not None:
                _conn.execute(
                    "UPDATE routes SET current_url = ?, current_revision = ? "
                    "WHERE id = 1 AND (current_url IS NULL OR current_revision IS NULL)",
                    legacy_pin,
                )
            # Interrupted requests of this gateway's previous process must not
            # linger as in-flight; rows belonging to peer gateways are theirs.
            cleared = _conn.execute(
                "DELETE FROM inflight WHERE gateway_id = ? OR gateway_id IS NULL",
                (GATEWAY_ID,),
            ).rowcount
            _conn.execute("COMMIT")
        except BaseException:
            _conn.execute("ROLLBACK")
            raise
        registry = {
            r["name"]: dict(r)
            for r in _conn.execute("SELECT name, url, revision FROM upstreams")
        }
    return {
        "seeded_route": seeded,
        "cleared_inflight": cleared,
        "gateway_id": GATEWAY_ID,
        "boot_id": BOOT_ID,
        "registry_changes": changed,
        "revisions": {n: r["revision"] for n, r in registry.items()},
    }


def _migrate_columns(conn: sqlite3.Connection) -> None:
    """Add columns introduced by this version to databases created by older
    releases (``CREATE TABLE IF NOT EXISTS`` leaves them untouched)."""
    wanted = {
        "upstreams": {
            "revision": "INTEGER NOT NULL DEFAULT 1",
            "updated_at": "REAL NOT NULL DEFAULT 0",
        },
        "routes": {"current_url": "TEXT", "current_revision": "INTEGER"},
        "inflight": {"url": "TEXT", "gateway_id": "TEXT"},
        "probes": {
            "url": "TEXT",
            "upstream_rev": "INTEGER",
            "route_version": "INTEGER",
            "boot_id": "TEXT",
        },
    }
    for table, cols in wanted.items():
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        for name, decl in cols.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def _register_upstreams(
    conn: sqlite3.Connection, upstreams: dict[str, str]
) -> list[dict]:
    """Apply this gateway's configured addresses, per name.

    Only a name whose configured URL differs from what *this gateway* last
    registered is applied. Upserting the same URL is a no-op (revision and
    probes survive restarts); changing it - in either direction - bumps the
    revision. Names absent from this gateway's configuration are never removed
    or modified, so peers with overlapping configs do not fight.
    """
    now = time.time()
    own = {
        r["name"]: r["url"]
        for r in conn.execute(
            "SELECT name, url FROM gateway_registrations WHERE gateway_id = ?",
            (GATEWAY_ID,),
        )
    }
    changed: list[dict] = []
    for name, url in upstreams.items():
        if own.get(name) == url:
            continue
        row = conn.execute(
            "SELECT url, revision FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
        revision = 1
        previous = None
        if row is None:
            conn.execute(
                "INSERT INTO upstreams (name, url, revision, updated_at) "
                "VALUES (?, ?, 1, ?)",
                (name, url, now),
            )
        else:
            previous = row["url"]
            revision = row["revision"]
            if previous != url:
                revision += 1
                conn.execute(
                    "UPDATE upstreams SET url = ?, revision = ?, updated_at = ? "
                    "WHERE name = ?",
                    (url, revision, now, name),
                )
        conn.execute(
            "INSERT INTO gateway_registrations (gateway_id, name, url, updated_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(gateway_id, name) DO UPDATE SET "
            "url = excluded.url, updated_at = excluded.updated_at",
            (GATEWAY_ID, name, url, now),
        )
        if previous != url:
            changed.append(
                {"name": name, "from": previous, "to": url, "revision": revision}
            )
    return changed


def _db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialised"
    return _conn


def get_route() -> dict:
    with _lock:
        row = _db().execute(
            "SELECT current_target, current_url, current_revision, version, updated_at "
            "FROM routes WHERE id = 1"
        ).fetchone()
    return dict(row)


def upstream(name: str) -> dict | None:
    with _lock:
        row = _db().execute(
            "SELECT name, url, revision FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
    return dict(row) if row else None


def admit(path: str) -> dict:
    """Atomically snapshot the effective route (target + confirmed address) and
    register the request as in-flight. The request keeps that address even if a
    switch commits - or the registry is re-pointed - while it runs, so it always
    completes against the upstream address it was admitted to."""
    rid = uuid.uuid4().hex
    with _write_tx() as conn:
        route = conn.execute(
            "SELECT current_target, current_url, current_revision, version "
            "FROM routes WHERE id = 1"
        ).fetchone()
        # Traffic follows the address the route was *confirmed* to; a later
        # registry edit must never silently redirect live traffic to an address
        # that was never probed. (Registry changes are adopted by committing a
        # probe of the new address, or by the route's own target being switched
        # away and back.)
        if route["current_url"] is None:
            raise RuntimeError(
                f"route for {route['current_target']!r} has no effective address"
            )
        conn.execute(
            "INSERT INTO inflight (id, target, url, gateway_id, path, started_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (rid, route["current_target"], route["current_url"], GATEWAY_ID,
             path, time.time()),
        )
    return {
        "id": rid,
        "target": route["current_target"],
        "version": route["version"],
        "url": route["current_url"],
    }


def finish(request_id: str) -> None:
    with _lock:
        _db().execute("DELETE FROM inflight WHERE id = ?", (request_id,))


def record_probe(
    target: str,
    base_version: int,
    ok: bool,
    detail: dict,
    *,
    url: str,
    upstream_revision: int,
    route_version: int,
) -> dict:
    """A probe never mutates the route. It records an auditable result bound to
    the exact address/revision and route version it observed, so a commit can
    prove it is not using evidence from a different address or route."""
    pid = uuid.uuid4().hex
    with _lock:
        _db().execute(
            "INSERT INTO probes "
            "(id, target, url, upstream_rev, base_version, route_version, boot_id, "
            " ok, detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pid,
                target,
                url,
                upstream_revision,
                base_version,
                route_version,
                BOOT_ID,
                1 if ok else 0,
                json.dumps(detail),
                time.time(),
            ),
        )
    return {"id": pid}


def commit_switch(target: str, base_version: int, probe_id: str) -> dict:
    """Compare-and-swap the route on its version under one write transaction.

    All evidence is re-read *inside* the transaction, together with the current
    registry. A commit is accepted only when the probe:

      * succeeded and was made for this target,
      * was made against the exact URL *and* registry revision that is current
        now (an address change, even one reverted, bumped it),
      * was made against the route version that is still current, and
      * is within its TTL.

    Otherwise the route and its version are left exactly as they were and the
    caller gets a reason to show "please probe again" in the console.
    """
    with _write_tx() as conn:
        route = conn.execute(
            "SELECT current_target, current_url, current_revision, version "
            "FROM routes WHERE id = 1"
        ).fetchone()
        reg = conn.execute(
            "SELECT url, revision FROM upstreams WHERE name = ?", (target,)
        ).fetchone()

        def reject(error: str, reason: str | None = None, **extra) -> dict:
            out = {
                "ok": False,
                "error": error,
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
            if reason is not None:
                out["reason"] = reason
            out.update(extra)
            return out

        if route["version"] != base_version:
            return reject("version_conflict", "route_version_changed")
        if reg is None:
            return reject("unknown_target", "target_unregistered")
        same_target = route["current_target"] == target
        address_changed = (
            reg["url"] != route["current_url"]
            or reg["revision"] != route["current_revision"]
        )
        # Staying on the same target is a no-op unless its registered address
        # actually changed: in that case this is the only way to adopt the new
        # address, and it still requires a successful probe of that address.
        if same_target and not address_changed:
            return reject("target_is_current")
        probe = conn.execute(
            "SELECT * FROM probes WHERE id = ?", (probe_id,)
        ).fetchone()
        if probe is None or not probe["ok"] or probe["target"] != target:
            return reject("probe_invalid", "probe_not_successful")
        if probe["base_version"] != base_version:
            return reject("probe_invalid", "probe_base_version_mismatch")
        if probe["route_version"] != route["version"]:
            return reject("probe_invalid", "route_version_changed")
        if probe["url"] != reg["url"]:
            return reject("probe_invalid", "address_changed")
        if probe["upstream_rev"] != reg["revision"]:
            return reject("probe_invalid", "address_changed")
        if time.time() - probe["created_at"] > PROBE_TTL_SECONDS:
            return reject("probe_stale", "probe_expired")
        cur = conn.execute(
            "UPDATE routes SET current_target = ?, current_url = ?, "
            "current_revision = ?, version = version + 1, updated_at = ? "
            "WHERE id = 1 AND version = ?",
            (target, reg["url"], reg["revision"], time.time(), base_version),
        )
        if cur.rowcount != 1:  # pragma: no cover - guarded by BEGIN IMMEDIATE
            return reject("version_conflict", "route_version_changed")
        new_version = base_version + 1
        result = {
            "ok": True,
            "current_target": target,
            "version": new_version,
            "url": reg["url"],
            "upstream_revision": reg["revision"],
            "same_target": same_target,
        }
    return result


def _probe_view(p: dict, route: dict, regs: dict[str, dict], now: float) -> dict:
    reg = regs.get(p["target"])
    binding_current = (
        reg is not None
        and p.get("url") is not None
        and p.get("upstream_rev") is not None
        and p["url"] == reg["url"]
        and p["upstream_rev"] == reg["revision"]
    )
    route_current = p.get("route_version") == route["version"]
    expired = now - p["created_at"] > PROBE_TTL_SECONDS
    reasons: list[str] = []
    if not p["ok"]:
        reasons.append("probe_not_successful")
    if p["base_version"] != route["version"]:
        reasons.append("probe_base_version_mismatch")
    if not binding_current:
        reasons.append("address_changed")
    if not route_current:
        reasons.append("route_version_changed")
    if expired:
        reasons.append("probe_expired")
    p["url_current"] = binding_current
    p["route_version_current"] = route_current
    p["expired"] = expired
    p["valid"] = not reasons
    p["invalid_reasons"] = reasons
    p["from_this_boot"] = p.get("boot_id") == BOOT_ID
    return p


def get_state() -> dict:
    now = time.time()
    with _lock:
        route = dict(_db().execute("SELECT * FROM routes WHERE id = 1").fetchone())
        upstreams = [
            dict(r)
            for r in _db().execute(
                "SELECT name, url, revision, updated_at FROM upstreams ORDER BY name"
            )
        ]
        inflight = [
            dict(r)
            for r in _db().execute(
                "SELECT id, target, url, gateway_id, path, started_at FROM inflight "
                "ORDER BY started_at"
            )
        ]
        probes = [
            dict(r)
            for r in _db().execute(
                "SELECT id, target, url, upstream_rev, base_version, route_version, "
                "boot_id, ok, detail, created_at FROM probes "
                "ORDER BY created_at DESC LIMIT 10"
            )
        ]
    regs = {u["name"]: u for u in upstreams}
    counts: dict[str, int] = {}
    for row in inflight:
        counts[row["target"]] = counts.get(row["target"], 0) + 1
    for u in upstreams:
        u["inflight"] = counts.get(u["name"], 0)
        u["is_current"] = u["name"] == route["current_target"]
        # A non-current target may only be taken offline once its in-flight
        # requests have fully drained.
        u["can_offline"] = (not u["is_current"]) and u["inflight"] == 0
    for row in inflight:
        row["age_ms"] = int((now - row["started_at"]) * 1000)
        row["is_local"] = row.get("gateway_id") == GATEWAY_ID
    # The route keeps serving the address it was confirmed against; surface
    # whether that still equals what is registered so the console can warn.
    current_reg = regs.get(route["current_target"], {})
    route["registered_url"] = current_reg.get("url")
    route["registered_revision"] = current_reg.get("revision")
    route["url_matches_registration"] = (
        route.get("current_url") == route["registered_url"]
        and route.get("current_revision") == route["registered_revision"]
    )
    for p in probes:
        p["detail"] = json.loads(p["detail"]) if p["detail"] else {}
        _probe_view(p, route, regs, now)
    return {
        "now": now,
        "route": route,
        "upstreams": upstreams,
        "inflight": inflight,
        "recent_probes": probes,
        "gateway_id": GATEWAY_ID,
        "boot_id": BOOT_ID,
        "probe_ttl_seconds": PROBE_TTL_SECONDS,
    }
