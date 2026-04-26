#!/bin/bash
set -e

echo "[WARP] Starting Cloudflare WARP daemon..."

# Create runtime directory for warp
mkdir -p /var/lib/cloudflare-warp /run/dbus

# Start WARP service in background
warp-svc &
WARP_PID=$!

# Wait for warp-svc to be ready
sleep 5

echo "[WARP] Registering with Cloudflare..."
# Accept TOS and register
warp-cli --accept-tos registration new || echo "[WARP] Already registered or registration failed"

echo "[WARP] Setting proxy mode..."
warp-cli --accept-tos mode proxy || true
warp-cli --accept-tos proxy port 40000 || true

echo "[WARP] Connecting..."
warp-cli --accept-tos connect || echo "[WARP] Connect attempted"

# Wait for connection
sleep 8

echo "[WARP] Status:"
warp-cli --accept-tos status || true

echo "[WARP] Testing proxy..."
curl --max-time 10 --proxy socks5://127.0.0.1:40000 https://www.cloudflare.com/cdn-cgi/trace 2>&1 | head -20 || echo "[WARP] Proxy test failed (continuing anyway)"

echo "[WARP] Starting server..."
exec uvicorn test_sabr:app --host 0.0.0.0 --port ${PORT:-8000}
