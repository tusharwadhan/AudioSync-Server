import os
import secrets
import time
import asyncio
from fastapi import APIRouter, Request, Response, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()

DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "admin")
if DASHBOARD_PASSWORD == "admin":
    print("[Dashboard] WARNING: Using default password 'admin'. Set DASHBOARD_PASSWORD env var.")


async def require_auth(request: Request):
    token = request.cookies.get("dashboard_session")
    analytics = request.app.state.analytics
    if not await analytics.validate_session(token):
        raise HTTPException(status_code=401, detail="Not authenticated")


LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AudioSync Dashboard - Login</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #0a0a0a; color: #fff; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; }
.login-card { background: #1a1a1a; border: 1px solid #333; border-radius: 16px; padding: 40px; width: 360px; text-align: center; }
.login-card h1 { font-size: 24px; margin-bottom: 8px; color: #1DB954; }
.login-card p { color: #888; margin-bottom: 24px; font-size: 14px; }
.login-card input { width: 100%; padding: 12px 16px; background: #252525; border: 1px solid #444; border-radius: 8px; color: #fff; font-size: 16px; outline: none; margin-bottom: 16px; }
.login-card input:focus { border-color: #1DB954; }
.login-card button { width: 100%; padding: 12px; background: #1DB954; color: #000; border: none; border-radius: 8px; font-size: 16px; font-weight: 600; cursor: pointer; }
.login-card button:hover { background: #1ed760; }
.error { color: #ff5252; font-size: 14px; margin-bottom: 16px; }
</style>
</head>
<body>
<div class="login-card">
<h1>AudioSync</h1>
<p>Server Dashboard</p>
__ERROR__
<form method="POST" action="/dashboard/login">
<input type="password" name="password" placeholder="Enter password" autofocus required>
<button type="submit">Login</button>
</form>
</div>
</body>
</html>"""


@router.get("/login", response_class=HTMLResponse)
async def login_page():
    return LOGIN_HTML.replace("__ERROR__", "")


@router.post("/login")
async def login(request: Request):
    form = await request.form()
    password = form.get("password", "")
    if password != DASHBOARD_PASSWORD:
        html = LOGIN_HTML.replace("__ERROR__", '<p class="error">Wrong password</p>')
        return HTMLResponse(html, status_code=401)

    token = secrets.token_urlsafe(32)
    analytics = request.app.state.analytics
    await analytics.create_session(token)
    response = RedirectResponse(url="/dashboard", status_code=303)
    response.set_cookie("dashboard_session", token, httponly=True, max_age=86400, samesite="lax")
    return response


@router.post("/logout")
async def logout(request: Request):
    token = request.cookies.get("dashboard_session")
    if token:
        analytics = request.app.state.analytics
        await analytics.delete_session(token)
    response = RedirectResponse(url="/dashboard/login", status_code=303)
    response.delete_cookie("dashboard_session")
    return response


@router.get("", response_class=HTMLResponse, dependencies=[Depends(require_auth)])
async def dashboard_page():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


# ---- Analytics API endpoints ----

@router.get("/api/summary", dependencies=[Depends(require_auth)])
async def api_summary(request: Request, hours: int = 24):
    analytics = request.app.state.analytics
    rm = request.app.state.room_manager
    counts = await analytics.get_event_counts(hours)
    counts["active_rooms"] = len(rm.rooms)
    return counts


@router.get("/api/songs", dependencies=[Depends(require_auth)])
async def api_songs(request: Request, days: int = 7, limit: int = 10):
    analytics = request.app.state.analytics
    hours = days * 24
    return {
        "top_songs": await analytics.get_top_songs(days, limit),
        "top_searches": await analytics.get_top_searches(days, limit),
        "play_timeline": await analytics.get_play_timeline(hours),
        "recent_plays": await analytics.get_recent_plays(50),
    }


@router.get("/api/users", dependencies=[Depends(require_auth)])
async def api_users(request: Request, days: int = 7):
    analytics = request.app.state.analytics
    return {
        "users": await analytics.get_user_activity(days),
        "timeline": await analytics.get_user_timeline(days * 24),
    }


@router.get("/api/user/{client_id}", dependencies=[Depends(require_auth)])
async def api_user_detail(request: Request, client_id: str, limit: int = 100):
    analytics = request.app.state.analytics
    return {"events": await analytics.get_user_log(client_id, limit)}


@router.get("/api/rooms", dependencies=[Depends(require_auth)])
async def api_rooms(request: Request, days: int = 7):
    analytics = request.app.state.analytics
    rm = request.app.state.room_manager
    active = rm.list_rooms()
    return {
        "active_rooms": active,
        "stats": await analytics.get_room_stats(days),
        "history": await analytics.get_room_history_list(50),
    }


@router.get("/api/health", dependencies=[Depends(require_auth)])
async def api_health(request: Request, hours: int = 24):
    analytics = request.app.state.analytics
    start_time = getattr(request.app.state, "start_time", time.time())
    caches = getattr(request.app.state, "caches", {})
    cache_sizes = {name: len(c) for name, c in caches.items()}

    return {
        "uptime_seconds": time.time() - start_time,
        "performance": await analytics.get_api_performance(hours),
        "request_timeline": await analytics.get_request_timeline(hours),
        "error_rates": await analytics.get_error_rates(hours),
        "cache_sizes": cache_sizes,
    }


@router.get("/api/fcm/tokens", dependencies=[Depends(require_auth)])
async def api_fcm_tokens(request: Request):
    from main import _fcm_tokens, room_manager
    tokens = []
    for client_id, token in _fcm_tokens.items():
        room_code = room_manager._client_to_room.get(client_id)
        tokens.append({
            "client_id": client_id,
            "token_preview": token[:20] + "...",
            "room": room_code,
        })
    return {"tokens": tokens}


@router.post("/api/fcm/test", dependencies=[Depends(require_auth)])
async def api_fcm_test(request: Request):
    from main import _fcm_tokens
    from firebase_admin import messaging

    body = await request.json()
    target = body.get("target", "all")

    if target == "all":
        targets = dict(_fcm_tokens)
    else:
        token = _fcm_tokens.get(target)
        if not token:
            return {"success": False, "error": "No FCM token for that client"}
        targets = {target: token}

    sent = 0
    failed = 0
    for client_id, token in targets.items():
        try:
            msg = messaging.Message(
                data={"type": "fcm_test", "message": "Test from dashboard"},
                token=token,
                android=messaging.AndroidConfig(priority="high"),
            )
            await asyncio.to_thread(messaging.send, msg)
            sent += 1
            print(f"[FCM] Test sent to {client_id[:8]}")
        except Exception as e:
            failed += 1
            print(f"[FCM] Test failed for {client_id[:8]}: {e}")

    return {"success": True, "sent": sent, "failed": failed}
