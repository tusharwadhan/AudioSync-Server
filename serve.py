"""Production entrypoint.

Exists because ws_ping_timeout=None cannot be expressed on the uvicorn
command line (--ws-ping-timeout takes a float). Why None:

- A WebSocket PONG is sent by the client's APP code, never the kernel.
  A phone whose app is frozen by its OEM (ColorOS freezes backgrounded
  apps on screen-off) cannot pong, even though its TCP socket is fully
  healthy (the kernel keeps ACKing while the app is frozen). Any finite
  pong timeout therefore executes frozen-but-healthy phones.
- Production probes (2026-10-04) showed the CURRENT deployment does not
  enforce pong timeouts (a fully silent client survived to ~305s and
  died to Render's data-idle kill, not to uvicorn) — so this setting is
  INSURANCE, pinned in requirements.txt to a uvicorn/websockets pair
  whose semantics are verified: pings keep flowing every 20s (NAT
  warmth) and the connection is never failed for a missing pong. A
  floating dependency bump could otherwise silently reintroduce a
  frozen-phone kill.
- ws_ping_timeout must be None, NOT a large float: with a finite timeout
  the next ping isn't scheduled until the previous pong arrives, so one
  unanswered ping silences server pings entirely.

Dead-peer detection lives at the app layer: client pings, the member
grace window, FCM wakes, and the ka sweep's wedged-socket close.
"""
import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        ws_ping_interval=20.0,
        ws_ping_timeout=None,
    )
