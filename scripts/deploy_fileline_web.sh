#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$ROOT_DIR/web"
RUN_DIR="$ROOT_DIR/.run"
PORT="${FILELINE_WEB_PORT:-8088}"
HOST="${FILELINE_WEB_HOST:-0.0.0.0}"
PID_FILE="$RUN_DIR/fileline-web.pid"
LOG_FILE="$RUN_DIR/fileline-web.log"

mkdir -p "$RUN_DIR"

cd "$WEB_DIR"
if [ ! -d node_modules ]; then
  npm install
fi
npm run build

if [ -f "$PID_FILE" ]; then
  OLD_PID="$(cat "$PID_FILE")"
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Stopping existing FileLine web process $OLD_PID"
    kill "$OLD_PID"
    for _ in $(seq 1 20); do
      if ! kill -0 "$OLD_PID" 2>/dev/null; then
        break
      fi
      sleep 0.25
    done
  fi
fi

cd "$ROOT_DIR"
setsid python -m uvicorn api_server:app --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 < /dev/null &
NEW_PID="$!"
echo "$NEW_PID" > "$PID_FILE"

for _ in $(seq 1 40); do
  if curl -fsS "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$NEW_PID" 2>/dev/null; then
    echo "FileLine web failed to start. Log:"
    tail -n 80 "$LOG_FILE" || true
    exit 1
  fi
  sleep 0.25
done

if ! curl -fsS "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1; then
  echo "FileLine web did not become healthy. Log:"
  tail -n 80 "$LOG_FILE" || true
  exit 1
fi

echo "FileLine web deployed"
echo "  URL:  http://$(hostname -I | awk '{print $1}'):$PORT"
echo "  PID:  $NEW_PID"
echo "  Log:  $LOG_FILE"
