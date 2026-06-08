#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/.run/fileline-web.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "FileLine web is not running (no pid file)."
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  echo "Stopped FileLine web process $PID"
else
  echo "FileLine web process $PID is not alive."
fi
rm -f "$PID_FILE"
