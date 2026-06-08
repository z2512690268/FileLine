#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/.run/fileline-web.pid"
LOG_FILE="$ROOT_DIR/.run/fileline-web.log"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "FileLine web is running"
  echo "  PID: $(cat "$PID_FILE")"
else
  echo "FileLine web is not running"
fi

if [ -f "$LOG_FILE" ]; then
  echo "  Log: $LOG_FILE"
fi
