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

echo "[App] Launching uvicorn on port ${PORT:-8000}"
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
