#!/bin/bash
# Zero-downtime deploy for reporting-system
# Usage: ./deploy.sh
# - git pull updates the code (app/ is a live volume mount, no rebuild needed)
# - kill -HUP 1 sends SIGHUP to gunicorn master -> graceful worker reload
#   (new workers start with new code, old workers finish in-flight requests, then exit)

set -e
cd "$(dirname "$0")"

echo "[deploy] Pulling latest code..."
git pull

# Find the running container
CONTAINER=$(docker ps --filter "name=reporting-system" --format "{{.Names}}" | head -1)

if [ -z "$CONTAINER" ]; then
  echo "[deploy] No running container found — doing full start..."
  docker-compose up -d --build
else
  echo "[deploy] Container: $CONTAINER"
  echo "[deploy] Sending graceful reload signal (SIGHUP)..."
  docker exec "$CONTAINER" kill -HUP 1
  echo "[deploy] Done. Workers reloading with new code — no downtime."
fi
