#!/usr/bin/env bash
# start.sh — runs the server in the foreground. Configured only by SPEC env vars
# (SQLITE_PATH, JWT_SECRET, HOST, PORT). We run behind Nginx on 127.0.0.1:3000.
set -euo pipefail
cd "$(dirname "$0")"

# 1 vCPU: keep the Go scheduler lean. One extra P can help overlap cgo/SQLite
# blocking calls with network work without oversubscribing the single core.
export GOMAXPROCS="${GOMAXPROCS:-2}"
# Reader connections in the pool (WAL concurrent readers).
export READERS="${READERS:-4}"

exec ./bin/server
