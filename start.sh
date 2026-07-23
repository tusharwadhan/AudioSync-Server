#!/usr/bin/env bash
set -e

# ── Optional Tailscale home-proxy for server-side yt-dlp ──────────────────
# The app does its own ON-DEVICE extraction (residential IP), so this proxy is
# only for the server's fallback audio extraction. It is fully optional and
# gated on TS_AUTHKEY:
#   - TS_AUTHKEY unset  → skip entirely; server-side yt-dlp goes direct.
#   - TS_AUTHKEY set     → bring the tunnel up, but NON-FATALLY — an expired /
#                          invalid key logs a warning and boots anyway instead
#                          of crash-looping the whole service (which used to
#                          take down update/check, releases, social, etc. too).
# To re-enable the proxy path: set a fresh TS_AUTHKEY *and*
# YTDLP_PROXY=socks5://100.82.133.108:1080 (see get_ytdlp_opts in main.py).
if [ -n "${TS_AUTHKEY:-}" ]; then
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
  if /usr/bin/tailscale --socket=/tmp/tailscaled.sock up \
        --authkey="${TS_AUTHKEY}" \
        --hostname="${TS_HOSTNAME:-audiosync-render}" \
        --accept-dns=false; then
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
  else
    echo "[Tailscale] WARNING: 'tailscale up' failed (expired/invalid TS_AUTHKEY?) — booting WITHOUT the home proxy. Server-side yt-dlp audio extraction goes direct (may be YouTube-blocked); on-device extraction is unaffected. Refresh TS_AUTHKEY to restore."
  fi
else
  echo "[Tailscale] TS_AUTHKEY not set — skipping Tailscale + home proxy. Server-side yt-dlp goes direct; the app's on-device extraction handles playback."
fi

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
