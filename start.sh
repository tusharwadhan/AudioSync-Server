#!/usr/bin/env bash
set -e

echo "[Tailscale] Starting userspace daemon..."
/usr/sbin/tailscaled \
  --tun=userspace-networking \
  --socks5-server=localhost:1055 \
  --state=/tmp/tailscaled.state \
  --socket=/tmp/tailscaled.sock \
  >/tmp/tailscaled.log 2>&1 &

# Wait for the daemon socket
for i in 1 2 3 4 5 6 7 8 9 10; do
  if [ -S /tmp/tailscaled.sock ]; then break; fi
  sleep 1
done

echo "[Tailscale] Bringing node up..."
/usr/bin/tailscale --socket=/tmp/tailscaled.sock up \
  --authkey="${TS_AUTHKEY}" \
  --hostname="${TS_HOSTNAME:-audiosync-render}" \
  --accept-dns=false

echo "[Tailscale] Status:"
/usr/bin/tailscale --socket=/tmp/tailscaled.sock status || true

echo "[gost] Starting chain: 127.0.0.1:1080 -> tailscaled SOCKS -> home proxy 100.82.133.108:1080"
/usr/local/bin/gost \
  -L "socks5://127.0.0.1:1080" \
  -F "socks5://localhost:1055" \
  -F "socks5://100.82.133.108:1080" \
  >/tmp/gost.log 2>&1 &

# Small settle delay
sleep 2

# Apply any pending Alembic migrations before launching the API.
# Idempotent: if the schema is already at head this is a fast no-op. If
# DATABASE_URL is unset we skip and warn — the server will still boot,
# but any DB-backed endpoint (/auth/sync, future sync routes) will 500
# with a clear error from db.py until the env var is configured.
if [ -n "${DATABASE_URL:-}" ]; then
  echo "[Migrate] Running alembic upgrade head..."
  # NON-FATAL: the DB can be unreachable (e.g. Neon free-tier compute
  # paused after hitting the monthly quota). If so, alembic errors/hangs —
  # we must NOT let that crash the boot, or the whole service goes down and
  # even non-DB endpoints (announcement, update/check) become undeployable.
  # Wrap in `if` (exempt from set -e) + a timeout so a paused/slow DB just
  # logs a warning and the API still launches; DB-backed features stay
  # unavailable until the DB is back.
  if timeout 120 alembic upgrade head; then
    echo "[Migrate] Schema up to date."
  else
    echo "[Migrate] WARNING: alembic failed/timed out (DB unreachable or paused?) — booting anyway. DB-backed features (social, sync) will be unavailable until the database is restored."
  fi
else
  echo "[Migrate] DATABASE_URL not set — skipping migrations (DB endpoints will fail)."
fi

echo "[App] Launching uvicorn on port ${PORT:-8000}"
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
