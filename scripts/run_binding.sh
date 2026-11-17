#!/usr/bin/env bash
# Registration/probe identity-binding and shared-SQLite interleaving harness.
# Starts three upstreams (two distinct addresses for the `beta` target) and two
# gateways on one SQLite file, drives only the public HTTP API, then tears
# everything down. No Docker needed; docker-compose.yml is the container
# equivalent (gateway-a + gateway-b on the same volume).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${WORK_DIR:-/tmp/edge-gw-binding}"
PY="${PYTHON:-python3}"

rm -rf "$WORK"
mkdir -p "$WORK"

WORK_DIR="$WORK" "$PY" "$ROOT/scripts/binding_test.py"
