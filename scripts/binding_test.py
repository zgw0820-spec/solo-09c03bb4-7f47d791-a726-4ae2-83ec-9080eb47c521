#!/usr/bin/env python3
"""Registration ↔ probe identity-binding tests.

Runs real upstream + gateway processes against temporary SQLite files and only
drives the public HTTP API. Covers what the basic smoke test does not:

  * a restart that does not change the address keeps a still-valid probe;
  * changing a candidate's registered address invalidates earlier probes, even
    for the same name and even if the address is later changed back;
  * an expired probe is not resurrected by a restart;
  * two gateways sharing one SQLite file: a registration change made by one
    gateway invalidates the other's probe; concurrent commits on one version
    have exactly one winner; an in-flight request keeps the address it was
    admitted to while the registry is re-pointed underneath it.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
WORK = Path(os.environ.get("WORK_DIR", tempfile.mkdtemp(prefix="edge-gw-bind-")))

FAILURES: list[str] = []
PROCS: list[subprocess.Popen] = []


def check(name, cond, extra=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_url(url, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status < 500:
                    return True
        except Exception:  # noqa: BLE001
            time.sleep(0.05)
    raise RuntimeError(f"timeout waiting for {url}")


def start_proc(name: str, args: list[str], env: dict) -> tuple[subprocess.Popen, Path]:
    log_path = WORK / f"{name}.log"
    log = open(log_path, "ab")  # noqa: SIM115 - handed to the child
    p = subprocess.Popen(
        args,
        stdout=log,
        stderr=subprocess.STDOUT,
        env={**os.environ, **env},
        cwd=str(ROOT),
    )
    PROCS.append(p)
    return p, log_path


def stop_proc(p: subprocess.Popen):
    if p.poll() is not None:
        return
    p.terminate()
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)


def upstream(name: str, port: int, delay_ms: int = 0):
    p, _ = start_proc(
        f"upstream-{name}",
        [
            PY, "-m", "uvicorn", "app.main:app", "--app-dir", str(ROOT / "upstream"),
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        ],
        {"UPSTREAM_NAME": name, "DEFAULT_DELAY_MS": str(delay_ms)},
    )
    wait_url(f"http://127.0.0.1:{port}/health")
    return p, f"http://127.0.0.1:{port}"


class Gateway:
    def __init__(self, db_path: Path, upstreams: dict[str, str], gateway_id: str,
                 initial: str = "alpha", ttl: float = 60.0):
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.db_path = db_path
        self.upstreams = upstreams
        self.gateway_id = gateway_id
        self.initial = initial
        self.ttl = ttl
        self.p = None
        self.start()

    def start(self):
        p, _ = start_proc(
            f"gateway-{self.gateway_id}-{self.port}",
            [
                PY, "-m", "uvicorn", "app.main:app", "--app-dir", str(ROOT / "gateway"),
                "--host", "127.0.0.1", "--port", str(self.port), "--log-level", "warning",
            ],
            {
                "GATEWAY_DB": str(self.db_path),
                "UPSTREAMS": ";".join(f"{k}={v}" for k, v in self.upstreams.items()),
                "INITIAL_TARGET": self.initial,
                "GATEWAY_ID": self.gateway_id,
                "PROBE_TTL_SECONDS": str(self.ttl),
            },
        )
        self.p = p
        wait_url(self.base + "/api/health")

    def stop(self):
        stop_proc(self.p)

    def restart(self, upstreams: dict[str, str] | None = None, ttl: float | None = None):
        self.stop()
        if upstreams is not None:
            self.upstreams = upstreams
        if ttl is not None:
            self.ttl = ttl
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.start()

    def api(self, method: str, path: str, body=None, timeout=30):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, method=method, data=data,
            headers={"content-type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def state(self):
        return self.api("GET", "/api/state")[1]

    def probe(self, target, base_version):
        return self.api("POST", "/api/switch/probe",
                        {"target": target, "base_version": base_version})

    def commit(self, target, base_version, probe_id):
        return self.api("POST", "/api/switch/commit",
                        {"target": target, "base_version": base_version,
                         "probe_id": probe_id})

    def call(self, delay_ms=0):
        return self.api("POST", "/api/call", {"delay_ms": delay_ms}, timeout=70)


def upstream_of(state, name):
    return next(u for u in state["upstreams"] if u["name"] == name)


def probe_of(state, probe_id):
    return next((p for p in state["recent_probes"] if p["id"] == probe_id), None)


def scenario(name):
    print(f"\n== {name} ==")
    d = WORK / name
    d.mkdir(parents=True, exist_ok=True)
    return d / "gateway.db"


def test_same_address_restart_keeps_probe(db, a_url, b_url, c_url):
    gw = Gateway(db, {"alpha": a_url, "beta": b_url}, "gw-restart")
    s, p = gw.probe("beta", 0)
    check("探测候选成功", p.get("ok") is True and p.get("committable") is True, p)
    probe_id = p["probe_id"]
    gw.restart()  # identical configuration, same SQLite file, same gateway id
    st = gw.state()
    check("同地址重启后路由仍为 alpha@v0",
          st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)
    check("同地址重启未改变登记修订",
          upstream_of(st, "beta")["url"] == b_url and upstream_of(st, "beta")["revision"] == 1)
    rec = probe_of(st, probe_id)
    check("重启后探测仍标记有效", rec is not None and rec["valid"] is True, rec)
    s, c = gw.commit("beta", 0, probe_id)
    check("同地址重启后仍可用有效期内的旧探测确认切换",
          s == 200 and c.get("ok") is True and c.get("version") == 1, c)
    check("切换后路由地址为登记地址", c.get("url") == b_url)
    s, r = gw.call(10)
    check("新请求走新地址", r.get("target") == "beta" and r.get("url") == b_url)
    gw.stop()


def test_address_change_invalidates_then_revert_still_invalid(db, a_url, b_url, c_url):
    gw = Gateway(db, {"alpha": a_url, "beta": b_url}, "gw-changer")
    s, p1 = gw.probe("beta", 0)
    check("旧地址探测成功", p1.get("ok") is True and p1.get("committable") is True)
    pid1 = p1["probe_id"]

    gw.restart({"alpha": a_url, "beta": c_url})  # operator edits UPSTREAMS
    st = gw.state()
    check("重启后候选登记指向新地址且修订增加",
          upstream_of(st, "beta")["url"] == c_url
          and upstream_of(st, "beta")["revision"] == 2, upstream_of(st, "beta"))
    rec = probe_of(st, pid1)
    check("旧地址探测在网页状态中标记失效(地址已变化)",
          rec is not None and rec["url_current"] is False
          and "address_changed" in rec["invalid_reasons"], rec)
    s, c = gw.commit("beta", 0, pid1)
    check("旧地址探测不能确认切换到新地址(400 probe_invalid/address_changed)",
          s == 400 and c.get("error") == "probe_invalid"
          and c.get("reason") == "address_changed", c)
    st = gw.state()
    check("被拒绝的确认未改变目标与版号",
          st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)

    gw.restart({"alpha": a_url, "beta": b_url})  # operator reverts the address
    st = gw.state()
    check("改回旧地址后登记修订再次增加",
          upstream_of(st, "beta")["revision"] == 3, upstream_of(st, "beta"))
    s, c = gw.commit("beta", 0, pid1)
    check("改回旧地址也不能复用变更前的探测",
          s == 400 and c.get("error") == "probe_invalid"
          and c.get("reason") == "address_changed", c)
    st = gw.state()
    check("改回后拒绝确认仍保持 alpha@v0",
          st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)
    s, p2 = gw.probe("beta", 0)
    check("重新探测新地址成功", p2.get("ok") is True and p2.get("committable") is True)
    s, c = gw.commit("beta", 0, p2["probe_id"])
    check("重新探测后可确认切换", s == 200 and c.get("version") == 1, c)
    gw.stop()


def test_expired_probe_not_preserved(db, a_url, b_url, c_url):
    gw = Gateway(db, {"alpha": a_url, "beta": b_url}, "gw-ttl", ttl=1.0)
    s, p = gw.probe("beta", 0)
    check("短有效期探测成功", p.get("ok") is True)
    time.sleep(1.4)
    gw.restart()
    st = gw.state()
    rec = probe_of(st, p["probe_id"])
    check("过期探测在状态中标记失效",
          rec is not None and rec["expired"] is True and rec["valid"] is False, rec)
    s, c = gw.commit("beta", 0, p["probe_id"])
    check("过期探测不能确认切换(400 probe_stale)",
          s == 400 and c.get("error") == "probe_stale", c)
    st = gw.state()
    check("过期拒绝后路由保持 alpha@v0",
          st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)
    gw.stop()


def test_two_gateways_shared_db(db, a_url, b_url, c_url):
    cfg = {"alpha": a_url, "beta": b_url}
    gwa = Gateway(db, cfg, "gateway-a")
    gwb = Gateway(db, cfg, "gateway-b")

    # 1) A probes beta at urlB; B is restarted with beta moved to urlC.
    s, pa = gwa.probe("beta", 0)
    check("网关A探测候选成功", pa.get("ok") is True and pa.get("committable") is True)
    gwb.restart({"alpha": a_url, "beta": c_url})
    st = gwa.state()
    check("网关B启动改登记后，网关A读到新地址与修订2",
          upstream_of(st, "beta")["url"] == c_url
          and upstream_of(st, "beta")["revision"] == 2, upstream_of(st, "beta"))
    s, c = gwa.commit("beta", 0, pa["probe_id"])
    check("另一网关变更登记后，旧探测确认被拒(地址已变化)",
          s == 400 and c.get("reason") == "address_changed", c)
    st = gwa.state()
    check("跨网关拒绝未改变路由 alpha@v0",
          st["route"]["current_target"] == "alpha" and st["route"]["version"] == 0)

    # 2) both gateways probe the current registration, commit concurrently.
    s, pb = gwa.probe("beta", 0)
    s, pc = gwb.probe("beta", 0)
    check("双网关探测均可提交", pb.get("committable") is True and pc.get("committable") is True)
    results: dict[str, tuple[int, dict]] = {}
    barrier = threading.Barrier(2)

    def fire(tag, gw, pid):
        barrier.wait()
        results[tag] = gw.commit("beta", 0, pid)

    t1 = threading.Thread(target=fire, args=("a", gwa, pb["probe_id"]))
    t2 = threading.Thread(target=fire, args=("b", gwb, pc["probe_id"]))
    t1.start(); t2.start(); t1.join(); t2.join()
    wins = [t for t, (s, b) in results.items() if s == 200 and b.get("ok")]
    losses = [t for t, (s, b) in results.items() if s == 409]
    check("并发确认恰好一个成功", len(wins) == 1, results)
    check("并发确认另一个收到 409 version_conflict",
          len(losses) == 1 and results[losses[0]][1].get("error") == "version_conflict",
          results)
    st = gwa.state()
    check("并发确认后路由为 beta@v1 且版本只加一次",
          st["route"]["current_target"] == "beta" and st["route"]["version"] == 1,
          st["route"])
    check("并发确认后路由地址为当前登记地址",
          upstream_of(st, "beta")["url"] == c_url)

    # 3) an in-flight request keeps the address it was admitted to while the
    #    other gateway re-points the registry underneath it.
    slow: dict = {}

    def slow_call():
        try:
            s, b = gwa.call(1500)
            slow["status"], slow["body"] = s, b
        except Exception as e:  # noqa: BLE001
            slow["error"] = str(e)

    t = threading.Thread(target=slow_call, daemon=True)
    t.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        if upstream_of(gwa.state(), "beta")["inflight"] >= 1:
            break
        time.sleep(0.05)
    check("已接纳慢请求绑定 beta", upstream_of(gwa.state(), "beta")["inflight"] >= 1)
    gwb.restart({"alpha": a_url, "beta": b_url})  # re-point while A's request runs
    st = gwa.state()
    check("登记改指向后路由仍固定在已确认地址",
          st["route"]["current_url"] == c_url and st["route"]["url_matches_registration"] is False,
          st["route"])
    s, fresh = gwa.call(10)
    check("登记变更后新请求仍走已确认地址，不跟随未探测的新地址",
          fresh.get("url") == c_url and fresh.get("target") == "beta", fresh)
    s, c = gwa.commit("beta", 1, pb["probe_id"])
    check("无探测时不能确认到新地址(探测属于旧地址)",
          s in (400, 409) and c.get("ok") is not True, c)
    st = gwa.state()
    check("拒绝后路由与版号保持 beta@v1 / 已确认地址",
          st["route"]["version"] == 1 and st["route"]["current_url"] == c_url)
    t.join(timeout=20)
    check("慢请求完成", "body" in slow, slow.get("error", ""))
    body = slow.get("body", {})
    check("慢请求仍从接纳时的地址取回完整响应",
          body.get("ok") is True and body.get("url") == c_url
          and body.get("upstream", {}).get("upstream") == "beta-c", body)

    # 3b) adopting a changed address of the *current* target requires a fresh
    #     probe of that address; then new traffic follows it.
    s, adopt = gwa.probe("beta", 1)
    check("当前目标地址变化后可对新地址发起探测",
          adopt.get("ok") is True and adopt.get("adopts_new_address") is True
          and adopt.get("url") == b_url, adopt)
    s, c = gwa.commit("beta", 1, adopt["probe_id"])
    check("探测新地址后可确认切换(版号 v2，地址为 b)",
          s == 200 and c.get("version") == 2 and c.get("url") == b_url, c)
    s, fresh = gwa.call(10)
    check("确认后新请求走新地址", fresh.get("url") == b_url and fresh.get("target") == "beta")
    check("确认后路由与登记一致", gwa.state()["route"]["url_matches_registration"] is True)

    # 4) version guard across gateways: A probes alpha at the current version,
    #    B commits that version first; A's stale commit must leave target and
    #    version untouched.
    st = gwa.state()
    base = st["route"]["version"]
    s, stale = gwa.probe("alpha", base)
    check(f"网关A探测 alpha@v{base} 成功",
          stale.get("ok") is True and stale.get("committable") is True, stale)
    s, first = gwb.probe("alpha", base)
    s, win = gwb.commit("alpha", base, first["probe_id"])
    check(f"网关B先完成切换 alpha@v{base + 1}",
          s == 200 and win.get("version") == base + 1, win)
    s, lose = gwa.commit("alpha", base, stale["probe_id"])
    check("网关A迟到的确认被版号拒绝且保留新版号",
          s == 409 and lose.get("error") == "version_conflict"
          and lose.get("current_version") == base + 1, lose)
    st = gwa.state()
    check(f"迟到确认后路由仍为 alpha@v{base + 1}",
          st["route"]["current_target"] == "alpha"
          and st["route"]["version"] == base + 1)
    stale_rec = probe_of(st, stale["probe_id"])
    check("迟到确认的探测因版号变化标记失效",
          stale_rec is not None and stale_rec["route_version_current"] is False
          and "probe_base_version_mismatch" in stale_rec["invalid_reasons"],
          stale_rec)
    s, fresh_probe = gwa.probe("beta", base + 1)
    check("重新探测 beta 成功", fresh_probe.get("committable") is True, fresh_probe)
    s, c = gwa.commit("beta", base + 1, fresh_probe.get("probe_id"))
    check(f"重新探测后可再次确认切换 beta@v{base + 2}",
          s == 200 and c.get("version") == base + 2 and c.get("url") == b_url, c)

    gwa.stop()
    gwb.stop()


def main():
    WORK.mkdir(parents=True, exist_ok=True)
    a_port, b_port, c_port = free_port(), free_port(), free_port()
    print(f"上游: alpha:{a_port} beta-b:{b_port} beta-c:{c_port}; 工作目录 {WORK}")
    upstream("alpha", a_port, delay_ms=0)
    upstream("beta-b", b_port, delay_ms=0)
    upstream("beta-c", c_port, delay_ms=0)
    a_url = f"http://127.0.0.1:{a_port}"
    b_url = f"http://127.0.0.1:{b_port}"
    c_url = f"http://127.0.0.1:{c_port}"

    test_same_address_restart_keeps_probe(
        scenario("same_address_restart"), a_url, b_url, c_url)
    test_address_change_invalidates_then_revert_still_invalid(
        scenario("address_change_revert"), a_url, b_url, c_url)
    test_expired_probe_not_preserved(
        scenario("probe_ttl"), a_url, b_url, c_url)
    test_two_gateways_shared_db(
        scenario("two_gateways"), a_url, b_url, c_url)


if __name__ == "__main__":
    try:
        main()
    finally:
        for p in PROCS:
            stop_proc(p)
    if FAILURES:
        print(f"\n{len(FAILURES)} 项检查失败:")
        for f in FAILURES:
            print(" -", f)
        print(f"日志目录: {WORK}")
        sys.exit(1)
    print(f"\n全部检查通过 (binding); 日志目录: {WORK}")
