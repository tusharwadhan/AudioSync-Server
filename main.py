from fastapi import (
    FastAPI,
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    Request,
    UploadFile,
    File,
)
from fastapi.responses import HTMLResponse, JSONResponse
import tempfile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
import yt_dlp
import time
import asyncio
import concurrent.futures
import httpx
import os
import threading
import json
import uuid
import re
from datetime import datetime, timezone
from room_manager import RoomManager
import control_session
from control_session import control_manager
from ytmusicapi import YTMusic
from analytics_db import AnalyticsDB
import firebase_admin
from firebase_admin import credentials, messaging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import AuthedUser, get_current_user
from db import get_session
import models
from sync import router as sync_router
import social

app = FastAPI(title="SyncAura API")

# Serve release APKs from /releases directory
os.makedirs(os.path.join(os.path.dirname(__file__), "releases"), exist_ok=True)
app.mount(
    "/releases",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "releases")),
    name="releases",
)

# Allow all origins for mobile app access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Analytics is a no-op stub; see analytics_db.py for context. Call sites
# throughout main.py remain in place so the diff stays small until a real
# replacement (Postgres-backed) is wired up.
analytics = AnalyticsDB()

# Versioned API router — all app endpoints live under /api/v1
api = APIRouter(prefix="/api/v1")


# ==================== API KEY SECURITY ====================
API_KEY = os.getenv("SYNCAURA_API_KEY", "sk_syncaura_v1_8f3k9x2m7q4w1p6y")

# Extra secret guarding admin-only actions (e.g. broadcasting a silent
# app update to every install). Must be set as an env var in production;
# when unset the admin endpoints refuse all requests.
ADMIN_SECRET = os.getenv("SYNCAURA_ADMIN_SECRET", "")

# Endpoints under /api/v1/* that bypass the API-key check. Add new public
# /api/v1 routes here (don't add prefixes for routes that already live at
# the root — those are never API-key-gated to begin with).
PUBLIC_API_V1_PATHS = (
    "/api/v1/update/check",
    "/api/v1/announcement",
)


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    path = request.url.path
    # Only /api/v1/* endpoints require an API key, minus the small public
    # allowlist above (first-launch / signed-out devices need to be able to
    # read these before they have credentials).
    needs_key = path.startswith("/api/v1/") and not any(
        path.startswith(p) for p in PUBLIC_API_V1_PATHS
    )
    if needs_key:
        key = request.headers.get("X-API-Key")
        if key != API_KEY:
            return JSONResponse(
                status_code=401, content={"detail": "Invalid or missing API key"}
            )
    return await call_next(request)


@app.middleware("http")
async def analytics_middleware(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    path = request.url.path
    if not path.startswith("/dashboard"):
        analytics.log_api_request(
            method=request.method,
            path=path,
            status_code=response.status_code,
            response_time_ms=(time.time() - start) * 1000,
            client_ip=request.client.host if request.client else "unknown",
        )
    return response


# ==================== PIPED CONFIGURATION ====================
# Self-hosted Piped instance (NewPipe Extractor on server)
# Deploy with: docker run -d -p 8080:8080 1337kavin/piped-backend
# Or use docker-compose up -d for both Piped and API
PIPED_URL = os.getenv("PIPED_URL", "http://localhost:8080")
PIPED_TIMEOUT = int(os.getenv("PIPED_TIMEOUT", "5"))
PIPED_ENABLED = (
    os.getenv("PIPED_ENABLED", "false").lower() == "true"
)  # Disabled by default until Piped is fixed

print(f"[Config] Piped: {'ENABLED' if PIPED_ENABLED else 'DISABLED'} ({PIPED_URL})")


class AudioResponse(BaseModel):
    success: bool
    videoId: str
    url: Optional[str] = None
    title: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None
    error: Optional[str] = None
    source: Optional[str] = None  # "piped" or "ytdlp"


class StreamSuggestion(BaseModel):
    videoId: str
    title: str
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None


class StreamResponse(BaseModel):
    success: bool
    videoId: str
    audioUrl: Optional[str] = None
    title: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None
    suggestions: List[StreamSuggestion] = []  # Next suggestions included!
    error: Optional[str] = None


class UpdateResponse(BaseModel):
    updateAvailable: bool
    mandatory: bool = False
    latestVersion: str
    latestVersionCode: int
    currentVersion: str
    currentVersionCode: int
    apkUrl: Optional[str] = None
    releaseNotes: Optional[str] = None
    # Emergency flag — when true the client renders the aggressive red/orange
    # full-screen blocker instead of the standard major-update modal. Forces
    # `mandatory` semantics regardless of the mandatoryBelow gate. Reserve for
    # security fixes and "playback fundamentally broken" releases.
    isEmergency: bool = False


class AnnouncementResponse(BaseModel):
    """Server-controlled message shown to every user on every cold start
    until [visible] is flipped back to false. Independent of update flow —
    the announcement modal renders even when no update is pending."""
    visible: bool
    message: Optional[str] = None


class RoomListItem(BaseModel):
    code: str
    hostName: str
    memberCount: int
    hasPassword: bool
    currentSong: Optional[str] = None


class RoomListResponse(BaseModel):
    success: bool
    rooms: List[RoomListItem] = []


# App update configuration - modify these values to control updates
APP_UPDATE_CONFIG = {
    "latestVersion": "5.17.20",
    "latestVersionCode": 75,
    "apkUrl": "https://raw.githubusercontent.com/tusharwadhan/AudioSync-Server/tushar/releases/syncaura-5.17.20.apk",
    "releaseNotes": "A rebuilt player header. The top of the player now tells you what you are actually listening to - the playlist you started from - instead of just saying Now Playing. Save, Add to playlist and Share moved into a labelled menu that opens inside the bar, and the sleep timer sits on the left with its presets one tap away, including Stop at a time. The bar itself doubles as a meter: it drains as your sleep timer counts down and fills as a song downloads, with the exact numbers alongside. Downloads can now be cancelled mid-way, and a song you saved yourself is marked in blue while one kept by auto-backup stays white. Songs that will not play also explain why now - not available in your region, removed from YouTube, needs a paid account - and SyncAura skips past them instead of stopping the queue.",
    # 5.15.0 → 5.15.1 is a patch-level bump → the client classifier
    # routes this to the Minor tier (quiet card in Settings, red dot
    # on the home gear). `mandatoryBelow` is effectively ignored for
    # Minor — leaving it at 41 simply means anyone still on <5.15.0
    # would get the standard mandatory treatment for THIS version
    # too if they somehow reached this far without 5.15.0.
    "mandatoryBelow": 41,
    "isEmergency": False,
    # 5.17.1 is a public release — empty list = offered to EVERYONE
    # (all users, including signed-out). Repopulate (or call
    # /admin/set-target-emails) only for a future cohort/TestFlight build.
    "targetEmails": [],
}


# Announcement configuration - independent of update flow.
# When `visible` is true, the client shows a modal on every cold start
# with `message` as the body. Flip to false to suppress immediately.
ANNOUNCEMENT_CONFIG = {
    "visible": False,
    "message": "",
}


class SearchResult(BaseModel):
    videoId: str
    title: str
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None


class SearchResponse(BaseModel):
    success: bool
    query: str
    results: List[SearchResult] = []
    error: Optional[str] = None


class RelatedResponse(BaseModel):
    success: bool
    videoId: str
    related: List[SearchResult] = []
    error: Optional[str] = None
    source: Optional[str] = None  # "piped" or "ytdlp"


class NextSongInfo(BaseModel):
    videoId: str
    title: str
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None
    audioUrl: str


class NextResponse(BaseModel):
    success: bool
    currentVideoId: str
    nextSong: Optional[NextSongInfo] = None
    suggestions: List[SearchResult] = []
    error: Optional[str] = None


class PrefetchResult(BaseModel):
    videoId: str
    audioUrl: Optional[str] = None
    success: bool = False


class PrefetchResponse(BaseModel):
    success: bool
    results: List[PrefetchResult] = []


# Simple in-memory cache for audio URLs
# YouTube URLs expire after ~6 hours, so we cache for 4 hours (safe margin)
_cache: dict = {}
_suggestions_cache: dict = {}
CACHE_TTL = 4 * 60 * 60  # 4 hours

# Room manager for Listen Together feature
room_manager = RoomManager()

# Firebase Cloud Messaging
_firebase_creds_json = os.getenv("FIREBASE_CREDENTIALS")
if _firebase_creds_json:
    _firebase_cred = credentials.Certificate(json.loads(_firebase_creds_json))
    firebase_admin.initialize_app(_firebase_cred)
    print("[FCM] Firebase initialized from FIREBASE_CREDENTIALS env var")
else:
    # Fallback: load from local file (for local development)
    _firebase_file = os.path.join(
        os.path.dirname(__file__),
        "audiosync-dfee2-firebase-adminsdk-fbsvc-4fe0940bca.json",
    )
    if os.path.exists(_firebase_file):
        _firebase_cred = credentials.Certificate(_firebase_file)
        firebase_admin.initialize_app(_firebase_cred)
        print("[FCM] Firebase initialized from local file")
    else:
        print("[FCM] WARNING: No Firebase credentials found. FCM disabled.")

# FCM token storage: client_id -> fcm_token
_fcm_tokens: dict[str, str] = {}
_fcm_token_to_client: dict[str, str] = {}


def register_fcm_token(client_id: str, token: str):
    """Store FCM token for a client."""
    old_client = _fcm_token_to_client.get(token)
    if old_client and old_client != client_id:
        _fcm_tokens.pop(old_client, None)

    old_token = _fcm_tokens.get(client_id)
    if old_token and old_token != token:
        _fcm_token_to_client.pop(old_token, None)

    _fcm_tokens[client_id] = token
    _fcm_token_to_client[token] = client_id
    print(f"[FCM] Token registered for {client_id[:8]}: {token[:20]}...")


# FCM sends are blocking HTTPS calls. They must NOT share the default executor:
# that pool also runs every yt-dlp / ytmusic extraction, and a mass disconnect
# (redeploy, network partition) would queue hundreds of pushes ahead of every
# /audio and /search request — stalling playback for users who never dropped,
# while health checks still pass because the event loop stays responsive.
_FCM_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="fcm")

# Per-device wake throttle. Caps amplification when several code paths want to
# wake the same device at once (the 3 retries here overlap with the host-action
# and chat-message wakes, which together could otherwise fire ~10 high-priority
# pushes in 20s and burn the device's FCM quota).
_FCM_WAKE_MIN_INTERVAL = 5.0
_last_wake_at: dict[str, float] = {}   # fcm token -> monotonic timestamp

_shutting_down = False


async def send_fcm_wake(client_id: str, room_code: str) -> bool:
    """High-priority data push telling one client to re-establish its room WS.

    High priority is what lets it through Doze; the client has no user-visible
    notification to show, it just needs the process woken.
    """
    if not firebase_admin._apps or _shutting_down:
        return False
    token = _fcm_tokens.get(client_id)
    if not token:
        return False

    now = time.monotonic()
    last = _last_wake_at.get(token, 0.0)
    if now - last < _FCM_WAKE_MIN_INTERVAL:
        return False
    _last_wake_at[token] = now

    try:
        message = messaging.Message(
            data={"type": "room_reconnect", "roomCode": room_code},
            token=token,
            android=messaging.AndroidConfig(priority="high"),
        )
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(_FCM_POOL, messaging.send, message)
        print(f"[FCM] room_reconnect -> {client_id[:8]} ({room_code})")
        return True
    except Exception as e:
        # Prune tokens that can never succeed. Only pop each map if it still
        # points at THIS client: the device may have reconnected under a new
        # client_id and re-registered the same token while we were awaiting,
        # and clobbering that would desync the two maps permanently.
        dead = isinstance(e, messaging.UnregisteredError) or isinstance(
            e, (messaging.SenderIdMismatchError, ValueError)
        )
        if dead:
            print(f"[FCM] Dropping unusable token for {client_id[:8]}: {type(e).__name__}")
            if _fcm_tokens.get(client_id) == token:
                _fcm_tokens.pop(client_id, None)
            if _fcm_token_to_client.get(token) == client_id:
                _fcm_token_to_client.pop(token, None)
            _last_wake_at.pop(token, None)
        else:
            print(f"[FCM] Failed to send to {client_id[:8]}: {e}")
    return False


async def send_fcm_to_disconnected_members(room_code: str):
    """Send FCM wake-up to every currently-disconnected member of a room."""
    if not firebase_admin._apps:
        return
    room = room_manager.rooms.get(room_code)
    if room is None:
        return
    for client_id, info in list(room_manager._pending_disconnects.items()):
        if info["code"] != room_code:
            continue
        # Same roster guard the per-client wake uses. Without it a stale
        # pending entry (they linger up to 60s after a room dies) could be
        # matched by a NEW room that reused the 4-char code, waking a stranger.
        member = room.members.get(client_id)
        if member is None or not member.reconnecting:
            continue
        await send_fcm_wake(client_id, room_code)


# Live wake tasks, keyed by the disconnected client_id. Handles are kept so a
# kick / leave / rejoin / shutdown can CANCEL a pending wake — without this a
# kicked member is actively paged back into the room they were removed from.
_wake_tasks: dict[str, "asyncio.Task"] = {}


def cancel_wake(client_id: str):
    """Stop any in-flight wake retries for a client."""
    t = _wake_tasks.pop(client_id, None)
    if t and not t.done():
        t.cancel()


def cancel_room_session(client_id: str):
    """Forget a client entirely: no deferred 'left', no wake, no pending entry.

    Used by paths where the member's departure is INTENTIONAL or enforced
    (explicit leave, kick), so nothing may later page their device or announce
    a leave for an identity that is already gone.

    Purges by DEVICE, not just by client_id. client_id is a fresh uuid per
    WebSocket connection, so a client that dropped and reconnected owns several
    ids: leaving as the newest one used to leave the older one's pending entry
    and roster ghost behind, still passing every wake guard. The FCM token is
    the stable per-device identity, so we clear every id sharing it.
    """
    ids = {client_id}
    token = _fcm_tokens.get(client_id)
    if token:
        ids.update(cid for cid, t in _fcm_tokens.items() if t == token)

    for cid in ids:
        cancel_wake(cid)
        t = _disconnect_grace_tasks.pop(cid, None)
        if t and not t.done():
            t.cancel()
        info = room_manager._pending_disconnects.pop(cid, None)
        # Drop any roster ghost the stale id left behind, so the room doesn't
        # later announce "X left" for someone who is sitting in it (or count
        # a phantom member forever).
        if info:
            ghost_room = room_manager.rooms.get(info.get("code", ""))
            if ghost_room is not None:
                m = ghost_room.members.get(cid)
                if m is not None and m.reconnecting:
                    ghost_room.members.pop(cid, None)
                    room_manager._client_to_room.pop(cid, None)
    if token:
        _last_wake_at.pop(token, None)


async def wake_after_disconnect(client_id: str, room_code: str):
    """Nudge a client that JUST dropped, and keep nudging inside the grace window.

    Previously the only wake-ups happened when the host next acted (play /
    pause / next / seek), so a member whose process was killed during quiet
    playback was never poked at all and simply aged out of the grace window.

    Retries because the first push often lands while the process is still
    being torn down, and because Doze can defer delivery.

    NEVER wakes someone who left on purpose. Three independent guards:
      1. Explicit leave / kick pop `_client_to_room` BEFORE the socket closes,
         so `disconnect_member` bails and no pending entry is ever created —
         this coroutine is not even started for them.
      2. Each attempt requires a live `_pending_disconnects` entry, which a
         leave never creates and a rejoin removes.
      3. Each attempt requires the member to still be in the roster flagged
         `reconnecting`; leaving mid-grace drops them from the roster.
    """
    # Last retry at 15s, not 20s: the HOST's room is destroyed at 30s, and the
    # client needs FCM delivery + process start + WS connect + a 2s rejoin
    # delay after that. A 20s push left almost no budget for the case this
    # feature most wants to save. All three stay inside the 45s member grace.
    for delay in (0, 5, 15):
        if delay:
            # No try/except: CancelledError must propagate so a kick/leave can
            # actually stop the retries and shutdown isn't silently swallowed.
            await asyncio.sleep(delay)
        # Back already, or the room is gone -> nothing to wake.
        if client_id not in room_manager._pending_disconnects:
            return
        room = room_manager.rooms.get(room_code)
        if room is None:
            return
        # Only wake someone the grace window is actually still holding open.
        member = room.members.get(client_id)
        if member is None or not member.reconnecting:
            return
        await send_fcm_wake(client_id, room_code)


def get_best_thumbnail(info: dict) -> str:
    """Pick the highest resolution thumbnail from yt-dlp or ytmusic info dict."""
    thumbs = info.get("thumbnails") or []
    if not thumbs:
        return info.get("thumbnail") or info.get("thumbnailUrl") or ""
    best = ""
    best_score = -1
    for t in thumbs:
        if not isinstance(t, dict):
            continue
        url = t.get("url", "")
        if not url:
            continue
        # yt-dlp uses 'preference', ytmusic uses 'width'
        pref = t.get("preference") or 0
        w = t.get("width") or 0
        score = pref * 10000 + w
        if score > best_score:
            best_score = score
            best = url
    return best or info.get("thumbnail") or ""


def get_cached(video_id: str) -> Optional[dict]:
    """Get cached audio URL result if still valid"""
    if video_id in _cache:
        entry = _cache[video_id]
        if time.time() - entry["timestamp"] < CACHE_TTL:
            return entry["data"]
        del _cache[video_id]
    return None


def set_cache(video_id: str, data: dict):
    """Cache audio URL result"""
    _cache[video_id] = {"data": data, "timestamp": time.time()}


def get_cached_suggestions(video_id: str) -> Optional[list]:
    """Get cached suggestions if still valid"""
    if video_id in _suggestions_cache:
        entry = _suggestions_cache[video_id]
        if time.time() - entry["timestamp"] < CACHE_TTL:
            return entry["data"]
        del _suggestions_cache[video_id]
    return None


def set_suggestions_cache(video_id: str, suggestions: list):
    """Cache suggestions"""
    _suggestions_cache[video_id] = {"data": suggestions, "timestamp": time.time()}


# ==================== YTMUSICAPI (Browse) ====================
_ytmusic = YTMusic()
_browse_cache: dict = {}
BROWSE_CACHE_TTL = 24 * 60 * 60  # 24 hours


def get_browse_cached(key: str):
    """Get cached browse result if still valid"""
    if key in _browse_cache:
        entry = _browse_cache[key]
        if time.time() - entry["timestamp"] < entry.get("ttl", BROWSE_CACHE_TTL):
            return entry["data"]
        del _browse_cache[key]
    return None


def set_browse_cache(key: str, data, ttl: int = BROWSE_CACHE_TTL):
    """Cache browse result"""
    _browse_cache[key] = {"data": data, "timestamp": time.time(), "ttl": ttl}


# Browse response models
class BrowseMoodCategory(BaseModel):
    title: str
    params: str


class BrowseMoodSection(BaseModel):
    title: str
    categories: List[BrowseMoodCategory]


class BrowseMoodsResponse(BaseModel):
    success: bool
    sections: List[BrowseMoodSection] = []


class BrowsePlaylistItem(BaseModel):
    playlistId: str
    title: str
    thumbnails: List[str] = []
    description: Optional[str] = None
    count: Optional[str] = None
    author: Optional[str] = None


class BrowseMoodPlaylistsResponse(BaseModel):
    success: bool
    title: str = ""
    playlists: List[BrowsePlaylistItem] = []


class BrowsePlaylistTrack(BaseModel):
    videoId: str
    title: str
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None


class BrowsePlaylistDetailResponse(BaseModel):
    success: bool
    title: str = ""
    description: Optional[str] = None
    thumbnail: Optional[str] = None
    trackCount: Optional[int] = None
    tracks: List[BrowsePlaylistTrack] = []


class BrowseChartTrack(BaseModel):
    videoId: str
    title: str
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None
    rank: int


class BrowseChartsResponse(BaseModel):
    success: bool
    country: str = ""
    songs: List[BrowseChartTrack] = []


class LyricsLine(BaseModel):
    text: str
    startMs: int
    endMs: int


class LyricsResponse(BaseModel):
    success: bool
    videoId: str
    hasTimestamps: bool = False
    lines: List[LyricsLine] = []
    plainLyrics: Optional[str] = None
    source: Optional[str] = None
    error: Optional[str] = None


# ==================== PIPED FUNCTIONS ====================


async def get_from_piped(video_id: str) -> Optional[dict]:
    """
    Try Piped first (fast ~300-500ms)
    Returns audio URL + metadata + related videos in one call
    """
    if not PIPED_ENABLED:
        return None

    try:
        async with httpx.AsyncClient(timeout=PIPED_TIMEOUT) as client:
            response = await client.get(f"{PIPED_URL}/streams/{video_id}")

            if response.status_code == 200:
                data = response.json()

                # Check for error in response
                if "error" in data:
                    print(f"Piped error: {data['error']}")
                    return None

                # Get best audio stream (highest bitrate)
                audio_streams = data.get("audioStreams", [])
                if not audio_streams:
                    print(f"Piped: No audio streams for {video_id}")
                    return None

                best_audio = max(audio_streams, key=lambda x: x.get("bitrate", 0))

                # Parse related videos
                related = []
                for item in data.get("relatedStreams", [])[:25]:
                    vid_url = item.get("url", "")
                    # Extract video ID from URL like "/watch?v=xyz"
                    vid = ""
                    if "v=" in vid_url:
                        vid = vid_url.split("v=")[-1].split("&")[0]
                    elif vid_url.startswith("/watch?v="):
                        vid = vid_url[9:].split("&")[0]

                    if vid and vid != video_id and len(vid) == 11:
                        related.append(
                            {
                                "videoId": vid,
                                "title": item.get("title", "Unknown"),
                                "duration": item.get("duration"),
                                "thumbnail": item.get("thumbnail"),
                                "uploader": item.get("uploaderName", "Unknown"),
                            }
                        )

                return {
                    "success": True,
                    "source": "piped",
                    "url": best_audio.get("url"),
                    "title": data.get("title"),
                    "uploader": data.get("uploader"),
                    "duration": data.get("duration"),
                    "thumbnail": data.get("thumbnailUrl"),
                    "related": related,
                }

    except httpx.TimeoutException:
        print(f"Piped timeout for {video_id}")
    except httpx.ConnectError:
        print(f"Piped connection error - is Piped running on {PIPED_URL}?")
    except Exception as e:
        print(f"Piped failed for {video_id}: {e}")

    return None


async def get_stream_url_from_piped(video_id: str) -> Optional[str]:
    """Get just the audio URL from Piped (for prefetching)"""
    result = await get_from_piped(video_id)
    if result and result.get("url"):
        return result["url"]
    return None


# ==================== YT-DLP CONFIGURATION ====================

# Cache directory for yt-dlp (persists player signatures across restarts)
YTDLP_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".ytdlp_cache"
)
os.makedirs(YTDLP_CACHE_DIR, exist_ok=True)

YTDLP_COOKIE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "cookies.txt"
)

# Reusable yt-dlp instance for audio extraction (caches player JS in memory)
_ydl_audio: yt_dlp.YoutubeDL | None = None
_ydl_audio_lock = threading.Lock()


def get_ytdlp_opts():
    """Get yt-dlp options with Deno configuration and caching"""
    import platform
    import shutil

    # Find JS runtime - prefer Node.js (faster startup) over Deno
    js_runtime = None

    # Try Node.js first (faster cold-start ~200ms vs Deno ~1s)
    node_path = shutil.which("node")
    if node_path:
        js_runtime = f"nodejs:{node_path}"
        print(f"[yt-dlp] Using Node.js: {node_path}") if not _ydl_audio else None
    else:
        # Fallback to Deno
        if platform.system() == "Windows":
            deno_path = os.path.expanduser("~/.deno/bin/deno.exe")
        else:
            possible_paths = [
                "/opt/render/.deno/bin/deno",
                os.path.expanduser("~/.deno/bin/deno"),
                shutil.which("deno"),
            ]
            deno_path = None
            for path in possible_paths:
                if path and os.path.exists(path):
                    deno_path = path
                    break
            if not deno_path:
                deno_path = "deno"
        js_runtime = f"deno:{deno_path}"

    opts = {
        "format": "ba/b",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "cachedir": YTDLP_CACHE_DIR,
        "extractor_args": {
            "youtube": {
                "player_client": ["default", "-android_sdkless"],
                "js_runtimes": [js_runtime],
            }
        },
    }
    # Home-proxy (Tailscale) for server-side extraction is now OPTIONAL — the
    # app extracts on-device (residential IP) as the primary path. Only route
    # through the proxy if YTDLP_PROXY is explicitly set (paired with a live
    # TS_AUTHKEY in start.sh). Default = direct, so an expired Tailscale key no
    # longer wedges server-side audio extraction against an unreachable proxy.
    _ytdlp_proxy = os.environ.get("YTDLP_PROXY", "").strip()
    if _ytdlp_proxy:
        opts["proxy"] = _ytdlp_proxy
    opts.update(_get_cookie_opts())
    return opts


def _get_cookie_opts() -> dict:
    if os.path.isfile(YTDLP_COOKIE_FILE):
        return {"cookiefile": YTDLP_COOKIE_FILE}
    return {}


def _get_audio_ydl() -> yt_dlp.YoutubeDL:
    """Get or create reusable YoutubeDL instance for audio extraction.
    Caches player JS in memory - first call slow, subsequent calls fast."""
    global _ydl_audio
    if _ydl_audio is None:
        _ydl_audio = yt_dlp.YoutubeDL(get_ytdlp_opts())
    return _ydl_audio


def _reset_audio_ydl():
    """Reset the reusable instance (if YouTube updates player or on error)"""
    global _ydl_audio
    if _ydl_audio:
        try:
            _ydl_audio.close()
        except Exception:
            pass
    _ydl_audio = None


def _extract_audio_ytdlp(video_id: str) -> AudioResponse:
    """Sync yt-dlp audio extraction (runs in thread pool).
    Uses reusable instance to avoid re-downloading player JS."""

    try:
        url = f"https://www.youtube.com/watch?v={video_id}"

        t_lock = time.time()
        with _ydl_audio_lock:
            t_got_lock = time.time()
            ydl = _get_audio_ydl()
            t_instance = time.time()
            info = ydl.extract_info(url, download=False)
            t_extract = time.time()

        print(
            f"[yt-dlp timing] {video_id}: lock_wait={t_got_lock - t_lock:.2f}s, instance={t_instance - t_got_lock:.2f}s, extract={t_extract - t_instance:.2f}s, total={t_extract - t_lock:.2f}s"
        )

        if not info:
            return AudioResponse(
                success=False, videoId=video_id, error="Failed to extract video info"
            )

        # Get the best audio URL
        audio_url = info.get("url")

        # If no direct URL, check formats
        if not audio_url and "formats" in info:
            for fmt in reversed(info["formats"]):
                if fmt.get("acodec") != "none" and fmt.get("url"):
                    audio_url = fmt["url"]
                    break

        if not audio_url:
            return AudioResponse(
                success=False, videoId=video_id, error="No audio URL found"
            )

        return AudioResponse(
            success=True,
            videoId=video_id,
            url=audio_url,
            title=info.get("title", "Unknown"),
            duration=info.get("duration", 0),
            thumbnail=get_best_thumbnail(info),
            uploader=info.get("uploader", "Unknown"),
            source="ytdlp",
        )

    except Exception as e:
        # If instance is broken, reset it for next request
        _reset_audio_ydl()
        return AudioResponse(success=False, videoId=video_id, error=str(e))


async def get_audio_ytdlp(video_id: str) -> AudioResponse:
    """Truly async: runs yt-dlp in thread pool"""
    return await asyncio.to_thread(_extract_audio_ytdlp, video_id)


def _extract_related_ytdlp(video_id: str, limit: int = 50) -> RelatedResponse:
    """Sync yt-dlp related extraction (runs in thread pool)"""

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "skip_download": True,
        "playlist_items": f"1-{limit + 1}",
    }
    ydl_opts.update(_get_cookie_opts())

    try:
        mix_url = f"https://www.youtube.com/watch?v={video_id}&list=RD{video_id}"

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(mix_url, download=False)

            if not info or "entries" not in info:
                return RelatedResponse(
                    success=False, videoId=video_id, error="No related songs found"
                )

            related = []
            for entry in info["entries"]:
                if not entry:
                    continue

                vid = entry.get("id", "")
                if not vid or vid == video_id:
                    continue

                related.append(
                    SearchResult(
                        videoId=vid,
                        title=entry.get("title", "Unknown"),
                        duration=entry.get("duration"),
                        thumbnail=get_best_thumbnail(entry),
                        uploader=entry.get("uploader")
                        or entry.get("channel", "Unknown"),
                    )
                )

                if len(related) >= limit:
                    break

            return RelatedResponse(
                success=True, videoId=video_id, related=related, source="ytdlp"
            )

    except Exception as e:
        return RelatedResponse(success=False, videoId=video_id, error=str(e))


async def get_related_ytdlp(video_id: str, limit: int = 50) -> RelatedResponse:
    """Truly async: runs yt-dlp in thread pool"""
    return await asyncio.to_thread(_extract_related_ytdlp, video_id, limit)


# ==================== API ENDPOINTS ====================


@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "AudioSync API",
        "piped_enabled": PIPED_ENABLED,
        "piped_url": PIPED_URL if PIPED_ENABLED else None,
    }


# Accept both GET and HEAD so external uptime pingers (UptimeRobot, etc.)
# that default to HEAD don't get a 405. HEAD responses automatically have
# their body stripped by Starlette — we still return the same payload for GET.
@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"status": "healthy", "piped_enabled": PIPED_ENABLED}


@app.on_event("startup")
async def startup_analytics():
    await analytics.init()
    app.state.analytics = analytics
    app.state.room_manager = room_manager
    app.state.caches = {
        "audio": _cache,
        "suggestions": _suggestions_cache,
        "browse": _browse_cache,
    }
    app.state.start_time = time.time()


@app.on_event("shutdown")
async def shutdown_analytics():
    # Stop emitting wakes first: on a redeploy every member drops at once, and
    # paging them all back to a process that is going away just makes their
    # rejoin fail with "Room no longer exists". Also prevents "Task was
    # destroyed but it is pending" noise and a hung FCM call delaying exit.
    global _shutting_down
    _shutting_down = True
    for t in list(_wake_tasks.values()):
        if not t.done():
            t.cancel()
    _wake_tasks.clear()
    _FCM_POOL.shutdown(wait=False, cancel_futures=True)
    await analytics.close()


@app.on_event("startup")
async def startup_analytics_cleanup():
    async def _cleanup_loop():
        while True:
            await asyncio.sleep(86400)  # Daily
            await analytics.cleanup_old_data()

    asyncio.create_task(_cleanup_loop())


@app.on_event("startup")
async def startup_room_cleanup():
    """Periodically clean up zombie rooms with no active host."""

    async def _cleanup_loop():
        while True:
            await asyncio.sleep(60)
            for code in list(room_manager.rooms.keys()):
                room = room_manager.rooms.get(code)
                if room is None:
                    continue
                # If host_id is not in active members, room is a zombie
                if room.host_id not in room.members:
                    await analytics.log_room_destroyed(
                        code,
                        room.host_name,
                        room.created_at,
                        room.peak_members,
                        room.songs_played,
                        room.password is not None,
                    )
                    remaining_ws = [m.websocket for m in room.members.values()]
                    for mid in list(room.members.keys()):
                        room_manager._client_to_room.pop(mid, None)
                    del room_manager.rooms[code]
                    for ws in remaining_ws:
                        await ws_send(ws, {"type": "room_closed"})
                    print(f"[Cleanup] Destroyed zombie room {code} (no active host)")
            room_manager.cleanup_stale_disconnects()

    asyncio.create_task(_cleanup_loop())


@app.on_event("startup")
async def startup_prewarm():
    """Pre-warm yt-dlp on server start - downloads and caches player JS"""
    print("[Startup] Pre-warming yt-dlp (caching player JS)...")

    def _warmup():
        try:
            t = time.time()
            with _ydl_audio_lock:
                ydl = _get_audio_ydl()
                # Extract a short, well-known video to cache player JS
                ydl.extract_info(
                    "https://www.youtube.com/watch?v=jNQXAC9IVRw", download=False
                )
            print(
                f"[Startup] Pre-warm complete! ({time.time() - t:.2f}s) - subsequent requests will be faster"
            )
        except Exception as e:
            print(f"[Startup] Pre-warm failed (non-critical): {e}")

    await asyncio.to_thread(_warmup)


@app.on_event("startup")
async def startup_browse_cache():
    """Pre-warm browse cache on server start (moods + charts)"""

    async def _warmup_browse():
        try:
            t = time.time()
            # Pre-warm moods
            moods = await asyncio.to_thread(_ytmusic.get_mood_categories)
            set_browse_cache("moods", moods)
            print(f"[Startup] Browse: moods cached ({time.time() - t:.2f}s)")

            # Pre-warm charts (fetch playlist tracks)
            t2 = time.time()
            raw_charts = await asyncio.to_thread(_ytmusic.get_charts, "ZZ")
            tracks_raw = []
            for src_key in ["daily", "weekly", "videos"]:
                src = raw_charts.get(src_key)
                if not src or not isinstance(src, list):
                    continue
                for entry in src:
                    if not isinstance(entry, dict):
                        continue
                    pid = entry.get("playlistId", "")
                    if not pid or pid.startswith("OLAK"):
                        continue
                    try:
                        pl = await asyncio.to_thread(_ytmusic.get_playlist, pid, 50)
                        tracks_raw = pl.get("tracks") or []
                        if tracks_raw:
                            break
                    except Exception:
                        continue
                if tracks_raw:
                    break
            if tracks_raw:
                songs = []
                for idx, track in enumerate(tracks_raw):
                    if not isinstance(track, dict):
                        continue
                    vid = track.get("videoId")
                    if not vid:
                        continue
                    thumb = get_best_thumbnail(track)
                    artists = track.get("artists") or []
                    artist = (
                        artists[0].get("name")
                        if artists and isinstance(artists[0], dict)
                        else None
                    )
                    songs.append(
                        BrowseChartTrack(
                            videoId=vid,
                            title=track.get("title", "Unknown"),
                            duration=track.get("duration_seconds"),
                            thumbnail=thumb,
                            uploader=artist,
                            rank=idx + 1,
                        )
                    )
                response = BrowseChartsResponse(success=True, country="ZZ", songs=songs)
                set_browse_cache("charts_ZZ", response)
                print(
                    f"[Startup] Browse: charts cached ({len(songs)} songs, {time.time() - t2:.2f}s)"
                )

            print(f"[Startup] Browse cache pre-warmed ({time.time() - t:.2f}s total)")
        except Exception as e:
            print(f"[Startup] Browse pre-warm failed (non-critical): {e}")

    asyncio.create_task(_warmup_browse())


# ==================== BROWSE ENDPOINTS (ytmusicapi) ====================


@api.get("/browse/moods", response_model=BrowseMoodsResponse)
async def browse_moods():
    """Get mood/genre categories from YouTube Music"""
    start = time.time()
    print("[/browse/moods] Request")

    cached = get_browse_cached("moods")
    if cached:
        # Parse cached dict into response
        sections = []
        for section_title, cats in cached.items():
            categories = [
                BrowseMoodCategory(title=c["title"], params=c["params"]) for c in cats
            ]
            sections.append(
                BrowseMoodSection(title=section_title, categories=categories)
            )
        print(f"[/browse/moods] CACHE HIT ({time.time() - start:.2f}s)")
        return BrowseMoodsResponse(success=True, sections=sections)

    try:
        raw = await asyncio.to_thread(_ytmusic.get_mood_categories)
        set_browse_cache("moods", raw)

        sections = []
        for section_title, cats in raw.items():
            categories = [
                BrowseMoodCategory(title=c["title"], params=c["params"]) for c in cats
            ]
            sections.append(
                BrowseMoodSection(title=section_title, categories=categories)
            )

        print(
            f"[/browse/moods] {sum(len(s.categories) for s in sections)} categories ({time.time() - start:.2f}s)"
        )
        analytics.log_event("browse", detail=json.dumps({"type": "moods"}))
        return BrowseMoodsResponse(success=True, sections=sections)
    except Exception as e:
        print(f"[/browse/moods] ERROR: {e}")
        return BrowseMoodsResponse(success=False)


@api.get("/browse/mood_playlists", response_model=BrowseMoodPlaylistsResponse)
async def browse_mood_playlists(params: str):
    """Get playlists for a specific mood/genre category"""
    start = time.time()
    cache_key = f"mood_playlists_{params}"
    print(f"[/browse/mood_playlists] Request: {params[:20]}...")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/browse/mood_playlists] CACHE HIT ({time.time() - start:.2f}s)")
        return cached

    try:
        raw = await asyncio.to_thread(_ytmusic.get_mood_playlists, params)

        # raw is a flat list of playlist dicts
        playlists = []
        for p in raw:
            if not isinstance(p, dict):
                continue
            playlist_id = p.get("playlistId", "")
            if not playlist_id:
                continue
            thumbs = []
            for t in p.get("thumbnails", []):
                if isinstance(t, dict) and t.get("url"):
                    thumbs.append(t["url"])
            playlists.append(
                BrowsePlaylistItem(
                    playlistId=playlist_id,
                    title=p.get("title", "Unknown"),
                    thumbnails=thumbs,
                    description=p.get("description"),
                    count=p.get("count"),
                    author=p.get("author"),
                )
            )

        response = BrowseMoodPlaylistsResponse(
            success=True, title="", playlists=playlists
        )
        set_browse_cache(cache_key, response)
        print(
            f"[/browse/mood_playlists] {len(playlists)} playlists ({time.time() - start:.2f}s)"
        )
        analytics.log_event("browse", detail=json.dumps({"type": "mood_playlists"}))
        return response
    except Exception as e:
        print(f"[/browse/mood_playlists] ERROR: {e}")
        return BrowseMoodPlaylistsResponse(success=False)


@api.get("/browse/playlist/{playlist_id}", response_model=BrowsePlaylistDetailResponse)
async def browse_playlist_detail(playlist_id: str, limit: int = 50):
    """Get playlist tracks"""
    start = time.time()
    cache_key = f"playlist_{playlist_id}_{limit}"
    print(f"[/browse/playlist] Request: {playlist_id}")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/browse/playlist] CACHE HIT ({time.time() - start:.2f}s)")
        return cached

    try:
        raw = await asyncio.to_thread(_ytmusic.get_playlist, playlist_id, limit)

        tracks = []
        for t in raw.get("tracks", []):
            video_id = t.get("videoId")
            if not video_id:
                continue

            # Get thumbnail
            thumbnail = get_best_thumbnail(t)

            # Get artist
            uploader = None
            artists = t.get("artists", [])
            if artists and isinstance(artists, list) and isinstance(artists[0], dict):
                uploader = artists[0].get("name")

            # Duration
            duration = t.get("duration_seconds")

            tracks.append(
                BrowsePlaylistTrack(
                    videoId=video_id,
                    title=t.get("title", "Unknown"),
                    duration=duration,
                    thumbnail=thumbnail,
                    uploader=uploader,
                )
            )

        # Playlist thumbnail
        pl_thumb = None
        pl_thumbs = raw.get("thumbnails", [])
        if pl_thumbs and isinstance(pl_thumbs, list):
            pl_thumb = (
                pl_thumbs[-1].get("url") if isinstance(pl_thumbs[-1], dict) else None
            )

        response = BrowsePlaylistDetailResponse(
            success=True,
            title=raw.get("title", ""),
            description=raw.get("description"),
            thumbnail=pl_thumb,
            trackCount=raw.get("trackCount"),
            tracks=tracks,
        )
        set_browse_cache(cache_key, response)
        print(f"[/browse/playlist] {len(tracks)} tracks ({time.time() - start:.2f}s)")
        analytics.log_event(
            "browse", video_id=playlist_id, detail=json.dumps({"type": "playlist"})
        )
        return response
    except Exception as e:
        print(f"[/browse/playlist] ERROR: {e}")
        return BrowsePlaylistDetailResponse(success=False)


@api.get("/browse/charts", response_model=BrowseChartsResponse)
async def browse_charts(country: str = "ZZ"):
    """Get music charts (top songs) — fetches first chart playlist tracks"""
    start = time.time()
    cache_key = f"charts_{country}"
    print(f"[/browse/charts] Request: country={country}")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/browse/charts] CACHE HIT ({time.time() - start:.2f}s)")
        return cached

    try:
        # get_charts returns different structures depending on country
        raw = await asyncio.to_thread(_ytmusic.get_charts, country)

        print(f"[/browse/charts] Raw keys: {list(raw.keys())}")

        tracks_raw = []
        # Country-specific charts: "daily"/"weekly" are lists of playlist refs
        # Global charts: "videos" is also a list of playlist refs
        # OLAK5uy_ prefixed IDs are album IDs that crash get_playlist — skip them
        for source_key in ["daily", "weekly", "videos"]:
            source = raw.get(source_key)
            if not source or not isinstance(source, list):
                continue
            # Find first valid playlist (PL prefix, skip OLAK album IDs)
            for entry in source:
                if not isinstance(entry, dict):
                    continue
                pid = entry.get("playlistId", "")
                if not pid or pid.startswith("OLAK"):
                    continue
                try:
                    print(
                        f"[/browse/charts] Fetching '{entry.get('title', '')}' ({pid})"
                    )
                    pl = await asyncio.to_thread(_ytmusic.get_playlist, pid, 50)
                    tracks_raw = pl.get("tracks") or []
                    if tracks_raw:
                        print(f"[/browse/charts] Got {len(tracks_raw)} tracks")
                        break
                except Exception as e:
                    print(f"[/browse/charts] Playlist {pid} failed: {e}")
                    continue
            if tracks_raw:
                break

        if not tracks_raw:
            print(f"[/browse/charts] No tracks found for country={country}")
            return BrowseChartsResponse(success=False, country=country)

        songs = []
        for idx, track in enumerate(tracks_raw):
            if not isinstance(track, dict):
                continue
            video_id = track.get("videoId")
            if not video_id:
                continue

            # Get thumbnail
            thumbnail = None
            thumbs = track.get("thumbnails") or []
            if thumbs and isinstance(thumbs, list):
                thumbnail = (
                    thumbs[-1].get("url") if isinstance(thumbs[-1], dict) else None
                )

            # Get artist
            uploader = None
            artists = track.get("artists") or []
            if artists and isinstance(artists, list) and isinstance(artists[0], dict):
                uploader = artists[0].get("name")

            songs.append(
                BrowseChartTrack(
                    videoId=video_id,
                    title=track.get("title", "Unknown"),
                    duration=track.get("duration_seconds"),
                    thumbnail=thumbnail,
                    uploader=uploader,
                    rank=idx + 1,
                )
            )

        response = BrowseChartsResponse(success=True, country=country, songs=songs)
        set_browse_cache(cache_key, response)
        print(f"[/browse/charts] {len(songs)} songs ({time.time() - start:.2f}s)")
        analytics.log_event(
            "browse", detail=json.dumps({"type": "charts", "country": country})
        )
        return response
    except Exception as e:
        print(f"[/browse/charts] ERROR: {e}")
        return BrowseChartsResponse(success=False)


def _parse_lrc(lrc_text: str) -> List[LyricsLine]:
    """Parse LRC format timestamps into LyricsLine objects.
    Handles multi-timestamp lines like [00:28.62][00:32.62]text"""
    ts_pattern = re.compile(r"\[(\d{2}):(\d{2})\.(\d{2,3})\]")
    entries = []

    for line in lrc_text.split("\n"):
        timestamps = ts_pattern.findall(line)
        if not timestamps:
            continue
        # Strip all timestamps to get clean text
        text = ts_pattern.sub("", line).strip()
        if not text:
            continue
        for mins, secs, ms_part in timestamps:
            ms = int(ms_part) * 10 if len(ms_part) == 2 else int(ms_part)
            start_ms = int(mins) * 60000 + int(secs) * 1000 + ms
            entries.append({"startMs": start_ms, "text": text})

    # Sort by startMs and set endMs = next line's startMs (last gets +5s)
    entries.sort(key=lambda x: x["startMs"])
    result = []
    for i, entry in enumerate(entries):
        end_ms = (
            entries[i + 1]["startMs"]
            if i + 1 < len(entries)
            else entry["startMs"] + 5000
        )
        result.append(
            LyricsLine(text=entry["text"], startMs=entry["startMs"], endMs=end_ms)
        )
    return result


# LRCLIB availability circuit breaker: when the service is down, every
# lyrics request would otherwise burn the full timeout ladder before the
# YTM fallback even starts. Two consecutive hard failures open the
# circuit for 5 minutes; any successful response closes it.
_LRCLIB_BREAKER = {"failures": 0, "open_until": 0.0}


def _lrclib_available() -> bool:
    return time.time() >= _LRCLIB_BREAKER["open_until"]


def _lrclib_record(ok: bool) -> None:
    if ok:
        _LRCLIB_BREAKER["failures"] = 0
    else:
        _LRCLIB_BREAKER["failures"] += 1
        if _LRCLIB_BREAKER["failures"] >= 2:
            _LRCLIB_BREAKER["open_until"] = time.time() + 300
            _LRCLIB_BREAKER["failures"] = 0
            print("[/lyrics] LRCLIB circuit OPEN — skipping for 5 min")


async def _lrclib_request(
    client: httpx.AsyncClient, url: str, params: dict, lrclib_down: list,
    attempts: int = 2,
) -> Optional[httpx.Response]:
    """Make an LRCLIB request, early exit if LRCLIB is down"""
    if lrclib_down[0] or not _lrclib_available():
        return None
    for attempt in range(attempts):
        try:
            resp = await client.get(
                url, params=params, headers={"User-Agent": "AudioSync/1.0"}
            )
            _lrclib_record(True)  # any HTTP response means the service is up
            if resp.status_code == 200:
                return resp
            return None  # 404 or other status — don't retry, LRCLIB is reachable
        except (httpx.TimeoutException, httpx.ConnectError):
            if attempt < attempts - 1:
                await asyncio.sleep(0.5)
            else:
                lrclib_down[0] = True
                _lrclib_record(False)
                return None
        except Exception:
            return None
    return None


async def _fetch_lrclib(
    title: str, artist: str, duration_secs: int = 0
) -> Optional[dict]:
    """Fetch synced lyrics from LRCLIB as fallback"""
    # Strip parenthetical suffixes like (From "Movie") for cleaner matching
    clean_title = re.sub(
        r'\s*\(From\s+"[^"]*"\)', "", title, flags=re.IGNORECASE
    ).strip()
    # Handle pipe-separated titles like "SONG NAME | VIDEO SONG | ARTIST"
    if "|" in clean_title:
        clean_title = clean_title.split("|")[0].strip()
    titles = [title, clean_title] if clean_title != title else [title]
    lrclib_down = [False]  # mutable flag for early exit
    if not _lrclib_available():
        return None

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            # Try exact match first if we have duration
            if duration_secs > 0:
                for t in titles:
                    resp = await _lrclib_request(
                        client,
                        "https://lrclib.net/api/get",
                        {
                            "track_name": t,
                            "artist_name": artist,
                            "duration": duration_secs,
                        },
                        lrclib_down,
                    )
                    if resp:
                        data = resp.json()
                        if data.get("syncedLyrics"):
                            return data

            # Search with artist
            for t in titles:
                resp = await _lrclib_request(
                    client,
                    "https://lrclib.net/api/search",
                    {"track_name": t, "artist_name": artist},
                    lrclib_down,
                )
                if resp:
                    for r in resp.json():
                        if r.get("syncedLyrics"):
                            return r

            # Fallback: search with title only (no artist) for better matching
            for t in titles:
                resp = await _lrclib_request(
                    client,
                    "https://lrclib.net/api/search",
                    {"track_name": t},
                    lrclib_down,
                )
                if resp:
                    results = resp.json()
                    for r in results:
                        if r.get("syncedLyrics"):
                            return r
                    # Return plain lyrics as last resort
                    if results and results[0].get("plainLyrics"):
                        return results[0]
    except Exception as e:
        print(f"[/lyrics] LRCLIB error: {e}")
    return None


async def _fetch_lrclib_precise(
    title: str, artist: str, duration_secs: int = 0
) -> Optional[dict]:
    """Fast, high-precision LRCLIB probe for when the client supplied the
    exact track metadata: one duration-verified /get, then one artist
    search whose results must actually match the title. Max 2 requests —
    the fuzzy multi-step ladder stays in _fetch_lrclib for fallback use."""
    clean_title = re.sub(
        r'\s*\(From\s+"[^"]*"\)', "", title, flags=re.IGNORECASE
    ).strip()
    if "|" in clean_title:
        clean_title = clean_title.split("|")[0].strip()
    lrclib_down = [False]
    if not _lrclib_available():
        return None

    def _norm(x: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (x or "").lower())

    try:
        # UI-blocking path: tight timeout, no retry — a healthy LRCLIB
        # answers in well under a second; the breaker handles a dead one.
        async with httpx.AsyncClient(timeout=3) as client:
            if duration_secs > 0:
                resp = await _lrclib_request(
                    client,
                    "https://lrclib.net/api/get",
                    {
                        "track_name": clean_title,
                        "artist_name": artist,
                        "duration": duration_secs,
                    },
                    lrclib_down,
                    attempts=1,
                )
                if resp:
                    data = resp.json()
                    if data.get("syncedLyrics") or data.get("plainLyrics"):
                        return data

            resp = await _lrclib_request(
                client,
                "https://lrclib.net/api/search",
                {"track_name": clean_title, "artist_name": artist},
                lrclib_down,
                attempts=1,
            )
            if resp:
                want = _norm(clean_title)
                for r in resp.json():
                    got = _norm(r.get("trackName", ""))
                    if want and (want in got or got in want):
                        if r.get("syncedLyrics") or r.get("plainLyrics"):
                            return r
    except Exception as e:
        print(f"[/lyrics] LRCLIB precise error: {e}")
    return None


# ── YTM lyrics fast path ────────────────────────────────────────────────
# Two raw anonymous innertube calls instead of ytmusicapi's heavyweight
# chain. Sizes with the (unofficial) `fields` mask: /next 1.8MB -> ~1.5KB,
# /browse 653KB -> ~18KB — which matters enormously on a 0.1 vCPU box.
# Client versions + field masks are env-overridable so schema churn can be
# handled without a redeploy (set on Render, restart).

_YTM_HEADERS = {
    "content-type": "application/json",
    "origin": "https://music.youtube.com",
    "user-agent": "Mozilla/5.0",
}
_YTM_WEB_VERSION = os.getenv("YTM_WEB_REMIX_VERSION", "1.20260708.03.00")
_YTM_MOBILE_VERSION = os.getenv("YTM_LYRICS_CLIENT_VERSION", "7.21.50")
_YTM_NEXT_FIELDS = os.getenv(
    "YTM_NEXT_FIELDS",
    "contents.singleColumnMusicWatchNextResultsRenderer.tabbedRenderer."
    "watchNextTabbedResultsRenderer.tabs.tabRenderer(title,unselectable,endpoint)",
)
_YTM_BROWSE_FIELDS = os.getenv(
    "YTM_BROWSE_FIELDS",
    "contents.elementRenderer.newElement.type.componentType.model."
    "timedLyricsModel.lyricsData",
)
_YTM_BLOAT_BYTES = 100_000  # fields mask stopped being honored
_LYR_BID_TTL = 7 * 86400    # browseId cache
_LYR_BID_NONE = "__none__"  # negative cache marker (fresh discovery only)

_ytm_http: Optional[httpx.AsyncClient] = None


def _ytm_client() -> httpx.AsyncClient:
    global _ytm_http
    if _ytm_http is None or _ytm_http.is_closed:
        _ytm_http = httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=3.0))
    return _ytm_http


async def _ytm_discover_lyrics_browse_id(video_id: str) -> Optional[str]:
    """Slim /next call: only exists to learn the MPLYt… lyrics browseId
    (proven NOT derivable from the videoId). Regex extraction — works even
    if Google stops honoring the fields mask and the response re-bloats."""
    resp = await _ytm_client().post(
        f"https://music.youtube.com/youtubei/v1/next?alt=json&fields={_YTM_NEXT_FIELDS}",
        json={
            "videoId": video_id,
            "context": {
                "client": {
                    "clientName": "WEB_REMIX",
                    "clientVersion": _YTM_WEB_VERSION,
                    "hl": "en",
                },
                "user": {},
            },
        },
        headers=_YTM_HEADERS,
    )
    resp.raise_for_status()
    if len(resp.content) > _YTM_BLOAT_BYTES:
        print(f"[/lyrics] WARNING: slim next response {len(resp.content)}B — fields mask ignored?")
    m = re.search(r'"(MPLYt[\w-]+)"', resp.text)
    if m:
        return m.group(1)
    # No browseId: only trust that as "song has no lyrics" if the response
    # is a genuine watch-next payload. Consent pages / challenge bodies /
    # A-B shape changes must NOT get negative-cached.
    if "watchNextTabbedResultsRenderer" not in resp.text:
        raise ValueError(f"unrecognized /next payload ({len(resp.content)}B)")
    return None


async def _ytm_fetch_timed_lyrics(browse_id: str) -> tuple[list, str]:
    """ANDROID_MUSIC /browse with a per-request context (no shared-state
    as_mobile() mutation, no thread race). Returns ([], "") on no lyrics."""
    resp = await _ytm_client().post(
        f"https://music.youtube.com/youtubei/v1/browse?alt=json&fields={_YTM_BROWSE_FIELDS}",
        json={
            "browseId": browse_id,
            "context": {
                "client": {
                    "clientName": "ANDROID_MUSIC",
                    "clientVersion": _YTM_MOBILE_VERSION,
                    "hl": "en",
                },
                "user": {},
            },
        },
        headers=_YTM_HEADERS,
    )
    resp.raise_for_status()
    if len(resp.content) > _YTM_BLOAT_BYTES:
        print(f"[/lyrics] WARNING: browse response {len(resp.content)}B — fields mask ignored?")
    try:
        data = resp.json()
    except ValueError:
        raise ValueError(f"non-JSON browse payload ({len(resp.content)}B)")
    if "contents" not in data:
        # {} is the legit "no timed lyrics" shape (fields-masked empty
        # model); anything else non-empty without contents is drift.
        if data:
            raise ValueError(f"browse envelope drift? ({len(resp.content)}B)")
        return [], ""
    try:
        lyr = (
            data["contents"]["elementRenderer"]["newElement"]["type"]
            ["componentType"]["model"]["timedLyricsModel"]["lyricsData"]
        )
    except (KeyError, TypeError):
        # contents present but no timed model — plain-only song
        return [], ""

    def _ms(v) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    lines = []
    for entry in lyr.get("timedLyricsData", []):
        cue = entry.get("cueRange", {})
        lines.append(
            LyricsLine(
                text=entry.get("lyricLine", ""),
                startMs=_ms(cue.get("startTimeMilliseconds")),
                endMs=_ms(cue.get("endTimeMilliseconds")),
            )
        )
    source = (lyr.get("sourceMessage") or "").replace("Source: ", "")
    return lines, source


async def _fetch_ytm_lyrics_fast(video_id: str) -> tuple[list, str, bool]:
    """Full fast path with a 7-day browseId cache.

    Returns (lines, source, confirmed_none). confirmed_none is True only
    when a FRESH, envelope-validated exchange affirmatively showed YTM has
    no timed lyrics — the caller may then skip the legacy chain. Errors and
    inconclusive states leave it False so fallbacks stay eligible. A cached
    browseId that errors or returns empty falls through to one fresh
    re-discovery (stale ids are indistinguishable from "no lyrics")."""
    bid_key = f"lyrbid_{video_id}"
    cached_bid = get_browse_cached(bid_key)
    if cached_bid == _LYR_BID_NONE:
        return [], "", True

    if cached_bid:
        try:
            lines, source = await _ytm_fetch_timed_lyrics(cached_bid)
            if lines:
                return lines, source, False
        except (httpx.HTTPError, ValueError) as e:
            print(f"[/lyrics] cached browseId failed ({e!r}) — re-discovering")
        # stale/erroring — fall through to fresh discovery

    browse_id = await _ytm_discover_lyrics_browse_id(video_id)
    if not browse_id:
        # envelope-validated "no lyrics tab" — safe to negative-cache
        set_browse_cache(bid_key, _LYR_BID_NONE, ttl=86400)
        return [], "", True
    if browse_id != cached_bid:
        set_browse_cache(bid_key, browse_id, ttl=_LYR_BID_TTL)
        lines, source = await _ytm_fetch_timed_lyrics(browse_id)
        if lines:
            return lines, source, False
        # fresh id + valid-but-empty model = genuinely no timed lyrics
        return [], "", True
    # re-discovery returned the id that just failed — inconclusive
    return [], "", False


# TEMPORARY Phase-0 gate: verifies from Render's egress IP that anonymous
# innertube accepts us, the fields mask is honored, and lyrics exist for
# region-mixed tracks. Remove with the other debug endpoint once trusted.
@api.get("/debug/ytm-lyrics-probe")
async def ytm_lyrics_probe(videoId: str):
    out: dict = {"videoId": videoId}
    t0 = time.time()
    try:
        resp = await _ytm_client().post(
            f"https://music.youtube.com/youtubei/v1/next?alt=json&fields={_YTM_NEXT_FIELDS}",
            json={
                "videoId": videoId,
                "context": {
                    "client": {
                        "clientName": "WEB_REMIX",
                        "clientVersion": _YTM_WEB_VERSION,
                        "hl": "en",
                    },
                    "user": {},
                },
            },
            headers=_YTM_HEADERS,
        )
        out["nextStatus"] = resp.status_code
        out["nextBytes"] = len(resp.content)
        out["nextMs"] = round((time.time() - t0) * 1000)
        m = re.search(r'"(MPLYt[\w-]+)"', resp.text)
        out["browseId"] = m.group(1) if m else None
    except Exception as e:
        out["nextError"] = repr(e)[:300]
        return out
    if not out.get("browseId"):
        return out
    t1 = time.time()
    try:
        lines, source = await _ytm_fetch_timed_lyrics(out["browseId"])
        out["browseMs"] = round((time.time() - t1) * 1000)
        out["timedLines"] = len(lines)
        out["source"] = source
        out["firstLine"] = lines[0].text if lines else None
    except Exception as e:
        out["browseError"] = repr(e)[:300]
    return out


@api.get("/lyrics/{video_id}", response_model=LyricsResponse)
async def get_lyrics_endpoint(
    video_id: str,
    title: str = "",
    artist: str = "",
    duration: int = 0,
):
    """Time-synced lyrics, cheapest source first.

    On this host (0.1 vCPU) racing sources concurrently just time-slices
    the CPU, so the flow is strictly serial but ORDERED BY COST:

      1. client sent title/artist/duration -> LRCLIB exact/artist probe
         (~1-2s, small JSON, duration-verified so precision is high)
      2. YTM chain: get_watch_playlist -> get_lyrics (~3-4s, heavy parse)
      3. last resort: LRCLIB again with watch-derived metadata (covers
         old clients that sent no metadata) + title-only search

    Old clients (no query params) effectively get the previous behavior.
    """
    start = time.time()
    cache_key = f"lyrics_{video_id}"
    print(f"[/lyrics] Request: {video_id} (client meta: {bool(title)})")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/lyrics] CACHE HIT ({time.time() - start:.2f}s)")
        return cached

    lines: List[LyricsLine] = []
    plain_lyrics = None
    source = ""
    lr_plain_backup = None  # plain lyrics found early, used only as last resort

    # ── 1. YTM fast path first: two slim anonymous innertube calls,
    # measured ~0.2-0.5s total from Render's egress (Phase-0 gate) ──
    track_title, track_artist, track_duration_secs = title, artist, duration
    fast_confirmed_none = False
    try:
        lines, source, fast_confirmed_none = await asyncio.wait_for(
            _fetch_ytm_lyrics_fast(video_id), timeout=6.0
        )
        if lines:
            print(f"[/lyrics] YTM fast: {len(lines)} timed lines ({time.time() - start:.2f}s)")
    except Exception as fast_err:
        print(f"[/lyrics] YTM fast path error: {fast_err!r}")
        lines = []

    # ── 2. LRCLIB precise probe when YTM had no timed lyrics and the
    # client told us exactly what's playing ──
    if not lines and title:
        lr = None
        try:
            # Hard budget: LRCLIB's latency is wildly variable (0.3s when
            # healthy, 7-30s degraded) — never let it stall the chain.
            lr = await asyncio.wait_for(
                _fetch_lrclib_precise(title, artist, duration), timeout=4.0
            )
        except asyncio.TimeoutError:
            print("[/lyrics] LRCLIB precise probe timed out (4s budget)")
        except Exception as e:
            print(f"[/lyrics] LRCLIB precise error: {e}")
        if lr and lr.get("syncedLyrics"):
            lines = _parse_lrc(lr["syncedLyrics"])
            source = "LRCLIB"
            print(f"[/lyrics] LRCLIB: {len(lines)} synced lines ({time.time() - start:.2f}s)")
        elif lr and lr.get("plainLyrics"):
            lr_plain_backup = lr["plainLyrics"]

    # ── 2b. legacy ytmusicapi chain — only when the fast path failed/was
    # inconclusive, or for old clients that sent no metadata (it backfills
    # title/artist for the LRCLIB ladder). When the fast path CONFIRMED
    # "no timed lyrics" and we have metadata, this 1.8MB chain would cost
    # 5-10s on this box just to maybe find plain lyrics — the LRCLIB
    # ladder below covers that for a fraction of the price.
    # Known upstream bug: get_watch_playlist KeyErrors when YouTube
    # injects a Comments tab, hence the broad try.
    if not lines and (not fast_confirmed_none or not title):
        try:
            watch = await asyncio.to_thread(_ytmusic.get_watch_playlist, video_id)
            lyrics_browse_id = watch.get("lyrics") if watch else None

            if not track_title and watch and watch.get("tracks"):
                track = watch["tracks"][0]
                track_title = track.get("title", "")
                artists = track.get("artists", [])
                track_artist = artists[0].get("name", "") if artists else ""
                length_str = track.get("length", "")
                if ":" in length_str:
                    parts = length_str.split(":")
                    try:
                        track_duration_secs = int(parts[0]) * 60 + int(parts[1])
                    except ValueError:
                        pass

            if lyrics_browse_id:
                try:
                    raw_lyrics = await asyncio.to_thread(
                        _ytmusic.get_lyrics, lyrics_browse_id, True
                    )
                    if raw_lyrics and raw_lyrics.get("lyrics"):
                        source = raw_lyrics.get("source", "")
                        has_timestamps = raw_lyrics.get("hasTimestamps", False)
                        lyrics_data = raw_lyrics.get("lyrics")
                        if has_timestamps and isinstance(lyrics_data, list):
                            for entry in lyrics_data:
                                lines.append(
                                    LyricsLine(
                                        text=getattr(entry, "text", ""),
                                        startMs=int(getattr(entry, "start_time", 0)),
                                        endMs=int(getattr(entry, "end_time", 0)),
                                    )
                                )
                        elif isinstance(lyrics_data, str):
                            plain_lyrics = lyrics_data
                except Exception as yt_err:
                    print(f"[/lyrics] YTMusic get_lyrics error: {yt_err}")
        except Exception as watch_err:
            print(f"[/lyrics] get_watch_playlist error: {watch_err}")

    # ── 3. last-resort LRCLIB (old clients / fuzzy search) ──
    if not lines and not plain_lyrics and not lr_plain_backup and track_title:
        print(
            f"[/lyrics] falling back to LRCLIB search for '{track_title}' - '{track_artist}'"
        )
        lrclib_data = None
        try:
            # Hard budget — the ladder is up to 6 requests and LRCLIB
            # degrades to 7-30s per request some nights.
            lrclib_data = await asyncio.wait_for(
                _fetch_lrclib(track_title, track_artist, track_duration_secs),
                timeout=8.0,
            )
        except asyncio.TimeoutError:
            print("[/lyrics] LRCLIB fuzzy ladder timed out (8s budget)")
        except Exception as e:
            print(f"[/lyrics] LRCLIB fuzzy ladder error: {e}")
        if lrclib_data:
            synced = lrclib_data.get("syncedLyrics")
            if synced:
                lines = _parse_lrc(synced)
                source = "LRCLIB"
                print(f"[/lyrics] LRCLIB: {len(lines)} synced lines")
            elif lrclib_data.get("plainLyrics"):
                lr_plain_backup = lrclib_data["plainLyrics"]

    if not lines and not plain_lyrics and lr_plain_backup:
        plain_lyrics = lr_plain_backup
        source = "LRCLIB"
        print(f"[/lyrics] LRCLIB: plain lyrics ({len(plain_lyrics)} chars)")

    if not lines and not plain_lyrics:
        print(f"[/lyrics] No lyrics found ({time.time() - start:.2f}s)")
        return LyricsResponse(
            success=False,
            videoId=video_id,
            error="No lyrics available for this song",
        )

    response = LyricsResponse(
        success=True,
        videoId=video_id,
        hasTimestamps=len(lines) > 0,
        lines=lines,
        plainLyrics=plain_lyrics,
        source=source,
    )
    set_browse_cache(cache_key, response, ttl=86400)  # Cache for 24 hours
    print(
        f"[/lyrics] {len(lines)} timed lines, source={source} ({time.time() - start:.2f}s)"
    )
    analytics.log_event(
        "lyrics_fetch",
        video_id=video_id,
        detail=json.dumps({"source": source, "lines": len(lines)}),
    )
    return response


@api.get("/cache/stats")
async def cache_stats():
    """Get cache statistics"""
    now = time.time()
    valid_urls = sum(1 for v in _cache.values() if now - v["timestamp"] < CACHE_TTL)
    valid_suggestions = sum(
        1 for v in _suggestions_cache.values() if now - v["timestamp"] < CACHE_TTL
    )
    return {
        "url_cache": {"total": len(_cache), "valid": valid_urls},
        "suggestions_cache": {
            "total": len(_suggestions_cache),
            "valid": valid_suggestions,
        },
        "cache_ttl_hours": CACHE_TTL / 3600,
        "cached_video_ids": list(_cache.keys())[:20],
    }


@api.delete("/cache/clear")
async def cache_clear():
    """Clear all cache"""
    url_count = len(_cache)
    sug_count = len(_suggestions_cache)
    _cache.clear()
    _suggestions_cache.clear()
    return {"cleared_urls": url_count, "cleared_suggestions": sug_count}


@api.post("/ytdlp/reset")
async def ytdlp_reset():
    """Reset the reusable yt-dlp instance (forces re-download of player JS)"""

    def _reset():
        with _ydl_audio_lock:
            _reset_audio_ydl()

    await asyncio.to_thread(_reset)
    return {
        "status": "reset",
        "message": "yt-dlp instance reset. Next request will re-download player JS.",
    }


def _check_update_logic(versionCode: int, versionName: str = "", email: str = ""):
    latest_code = APP_UPDATE_CONFIG["latestVersionCode"]
    latest_version = APP_UPDATE_CONFIG["latestVersion"]
    mandatory_below = APP_UPDATE_CONFIG["mandatoryBelow"]
    target_emails = APP_UPDATE_CONFIG.get("targetEmails") or []

    update_available = versionCode < latest_code

    # TestFlight-style targeted rollout. When the config has a non-empty
    # `targetEmails` list, the update is offered ONLY to clients whose
    # email matches one of those entries. Clients with no email (old
    # builds that don't send `?email=`, or signed-out users) and
    # clients not in the list see `updateAvailable: false` — they keep
    # running their current version until the rollout opens up.
    # Empty list = no targeting = update offered to everyone (the normal
    # release case).
    if update_available and target_emails:
        normalized = (email or "").strip().lower()
        allow = {e.strip().lower() for e in target_emails if e}
        if normalized not in allow:
            update_available = False

    is_mandatory = versionCode < mandatory_below

    is_emergency = bool(APP_UPDATE_CONFIG.get("isEmergency", False))

    return UpdateResponse(
        updateAvailable=update_available,
        # Emergency releases force mandatory semantics; the client uses both
        # flags together (isEmergency → red blocker, mandatory → cannot skip).
        mandatory=(is_mandatory or is_emergency) if update_available else False,
        latestVersion=latest_version,
        latestVersionCode=latest_code,
        currentVersion=versionName,
        currentVersionCode=versionCode,
        apkUrl=APP_UPDATE_CONFIG["apkUrl"] if update_available else None,
        releaseNotes=APP_UPDATE_CONFIG["releaseNotes"] if update_available else None,
        isEmergency=is_emergency if update_available else False,
    )


@api.get("/update/check", response_model=UpdateResponse)
async def check_update(versionCode: int, versionName: str = "", email: str = ""):
    """Check if app update is available.

    `email` is optional — when provided (by signed-in clients only) it
    is matched against the `targetEmails` allowlist in APP_UPDATE_CONFIG
    for TestFlight-style rollouts. Old clients that don't send it are
    treated as "not in the test cohort" when targeting is active.
    """
    return _check_update_logic(versionCode, versionName, email)


@app.get("/update/check", response_model=UpdateResponse)
async def check_update_legacy(versionCode: int, versionName: str = "", email: str = ""):
    """Legacy path for old app versions that don't use /api/v1"""
    return _check_update_logic(versionCode, versionName, email)


@api.get("/announcement", response_model=AnnouncementResponse)
async def get_announcement():
    """Return the current announcement. Always returns 200 — clients treat
    `visible: false` as "no announcement". Public (no API key) so first-launch
    devices can read it before they have credentials."""
    return AnnouncementResponse(
        visible=bool(ANNOUNCEMENT_CONFIG.get("visible", False)),
        message=ANNOUNCEMENT_CONFIG.get("message") or None,
    )


@api.post("/admin/set-target-emails")
async def admin_set_target_emails(request: Request):
    """Admin-only: set the TestFlight-style allowlist for the current
    APP_UPDATE_CONFIG version. Same admin-guard pattern as
    set-announcement / broadcast-update.

    Body: { "emails": ["tushar.code05@gmail.com", "..."] }
    Pass an empty list `{"emails": []}` to roll out to everyone.
    """
    admin_header = request.headers.get("X-Admin-Secret", "")
    if not ADMIN_SECRET or admin_header != ADMIN_SECRET:
        return JSONResponse({"error": "forbidden"}, status_code=403)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "bad json"}, status_code=400)
    raw = body.get("emails")
    if not isinstance(raw, list):
        return JSONResponse(
            {"error": "expected `emails` to be a list of strings"},
            status_code=400,
        )
    cleaned = [
        e.strip().lower()
        for e in raw
        if isinstance(e, str) and e.strip()
    ]
    # De-dup while preserving order.
    seen: set[str] = set()
    deduped: List[str] = []
    for e in cleaned:
        if e not in seen:
            seen.add(e)
            deduped.append(e)
    APP_UPDATE_CONFIG["targetEmails"] = deduped
    return {
        "ok": True,
        "targetEmails": deduped,
        "count": len(deduped),
        "version": APP_UPDATE_CONFIG["latestVersion"],
    }


@api.post("/admin/set-announcement")
async def admin_set_announcement(request: Request):
    """Admin-only: flip the announcement on/off and set its text. Mirrors the
    `broadcast_update` admin guard pattern — requires both X-API-Key (handled
    by middleware) and X-Admin-Secret here.

    Body: { "visible": bool, "message": "..." }
    """
    admin_header = request.headers.get("X-Admin-Secret", "")
    if not ADMIN_SECRET or admin_header != ADMIN_SECRET:
        return JSONResponse({"error": "forbidden"}, status_code=403)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "bad json"}, status_code=400)
    visible = bool(body.get("visible", False))
    message = (body.get("message") or "").strip()
    ANNOUNCEMENT_CONFIG["visible"] = visible
    ANNOUNCEMENT_CONFIG["message"] = message
    return {"ok": True, "visible": visible, "message": message}


@api.post("/admin/broadcast-update")
async def broadcast_update(request: Request):
    """
    Admin-only: wake EVERY install with an "app_update" FCM data message
    (topic "app_updates") so the on-device UpdateWorker downloads + silently
    installs the latest APK in the background. Reads the target version from
    APP_UPDATE_CONFIG, so bump that (and upload the APK) before calling.

    Guarded by the X-Admin-Secret header in addition to the usual X-API-Key.
    Call manually only when you actually intend to ship a release:

        curl -X POST https://<host>/api/v1/admin/broadcast-update \\
             -H "X-API-Key: <api key>" \\
             -H "X-Admin-Secret: <admin secret>"
    """
    if not ADMIN_SECRET or request.headers.get("X-Admin-Secret") != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    if not firebase_admin._apps:
        raise HTTPException(status_code=503, detail="FCM not initialized")

    cfg = APP_UPDATE_CONFIG
    message = messaging.Message(
        # Data-only (no notification block) so it never shows a banner —
        # it just wakes onMessageReceived to enqueue the UpdateWorker.
        data={
            "type": "app_update",
            "latestVersion": str(cfg["latestVersion"]),
            "latestVersionCode": str(cfg["latestVersionCode"]),
        },
        topic="app_updates",
        android=messaging.AndroidConfig(priority="high"),
    )
    try:
        msg_id = await asyncio.to_thread(messaging.send, message)
    except Exception as e:
        print(f"[FCM] app_update broadcast FAILED: {e}")
        raise HTTPException(status_code=500, detail=f"broadcast failed: {e}")

    print(
        f"[FCM] app_update broadcast to topic 'app_updates' "
        f"(v{cfg['latestVersion']} code {cfg['latestVersionCode']}): {msg_id}"
    )
    return {
        "status": "sent",
        "messageId": msg_id,
        "topic": "app_updates",
        "latestVersion": cfg["latestVersion"],
        "latestVersionCode": cfg["latestVersionCode"],
    }


# ==================== REMOTE PLAYBACK-ERROR REPORTS ====================
# Devices POST a report whenever playback/download of a song fails, with a
# snapshot of the app's recent in-memory DebugLogger buffer attached — the
# same lines we'd otherwise have to pull off the device over adb. Kept in
# memory (ring buffer) and appended to a JSONL file. The file lives on the
# instance disk, so it survives restarts but not redeploys — good enough
# for diagnostics; don't treat it as durable storage.

PLAYBACK_ERRORS: list = []          # newest last
PLAYBACK_ERRORS_MAX = 300
PLAYBACK_ERRORS_FILE = os.path.join(os.path.dirname(__file__), "playback_errors.jsonl")


class PlaybackErrorReport(BaseModel):
    deviceId: str = ""
    deviceModel: str = ""
    androidVersion: str = ""
    appVersion: str = ""
    appVersionCode: int = 0
    network: str = ""               # wifi / cellular / offline / unknown
    errorType: str = ""             # player_error / download_error / fetch_error
    songId: str = ""
    songTitle: str = ""
    message: str = ""
    recentLogs: str = ""            # tail of the app's DebugLogger buffer


async def _persist_playback_error_to_db(entry: dict):
    """Durable copy of a device error report in Postgres.

    Strictly best-effort. The ring buffer and JSONL file are written first and
    synchronously, so a paused/unreachable database costs us only cross-deploy
    history — never a dropped report and never a failed request.
    """
    import db as _db

    factory = _db.try_session_factory()
    if factory is None:
        return
    try:
        received = datetime.fromisoformat(entry["serverTime"])
    except Exception:
        received = datetime.now(timezone.utc)
    try:
        async with factory() as session:
            session.add(models.PlaybackErrorLog(
                device_id=(entry.get("deviceId") or "")[:128] or None,
                device_model=(entry.get("deviceModel") or "")[:128] or None,
                android_version=(entry.get("androidVersion") or "")[:32] or None,
                app_version=(entry.get("appVersion") or "")[:32] or None,
                app_version_code=entry.get("appVersionCode") or None,
                network=(entry.get("network") or "")[:32] or None,
                error_type=(entry.get("errorType") or "")[:64] or None,
                song_id=(entry.get("songId") or "")[:64] or None,
                song_title=(entry.get("songTitle") or "")[:512] or None,
                message=entry.get("message") or None,
                recent_logs=entry.get("recentLogs") or None,
                client_ip=(entry.get("clientIp") or "")[:64] or None,
                received_at=received,
            ))
            await session.commit()
    except Exception as e:
        print(f"[errlog] DB persist failed (non-fatal): {type(e).__name__}: {e}")


@api.post("/log/playback-error")
async def log_playback_error(
    report: PlaybackErrorReport,
    request: Request,
    background_tasks: BackgroundTasks,
):
    entry = report.dict()
    # Cap the log payload so a misbehaving client can't balloon the file.
    entry["recentLogs"] = entry["recentLogs"][-32000:]
    entry["serverTime"] = datetime.now(timezone.utc).isoformat()
    entry["clientIp"] = request.client.host if request.client else "unknown"

    PLAYBACK_ERRORS.append(entry)
    del PLAYBACK_ERRORS[:-PLAYBACK_ERRORS_MAX]
    try:
        with open(PLAYBACK_ERRORS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[errlog] failed to persist playback error: {e}")

    # Durable copy, off the request path so the device never waits on the DB.
    background_tasks.add_task(_persist_playback_error_to_db, entry)

    print(
        f"[errlog] {entry['errorType']} on {entry['deviceModel']} "
        f"(app {entry['appVersion']}) song={entry['songId']} : {entry['message'][:120]}"
    )
    return {"status": "logged"}


@api.get("/log/playback-errors")
async def get_playback_errors(
    request: Request,
    limit: int = 50,
    include_logs: bool = False,
    device: str = "",
):
    """
    Admin-only viewer (same guard as the other admin endpoints):

        curl "https://<host>/api/v1/log/playback-errors?limit=20&include_logs=true" \\
             -H "X-API-Key: <api key>" -H "X-Admin-Secret: <admin secret>"
    """
    if not ADMIN_SECRET or request.headers.get("X-Admin-Secret") != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    capped = max(1, min(limit, PLAYBACK_ERRORS_MAX))

    # Merge all three sources rather than picking one. Postgres holds the
    # durable history (survives redeploys); the ring holds anything this
    # instance saw that the DB write may have missed (e.g. DB paused); the
    # JSONL file covers a restart within the same deploy. Previously the file
    # was only consulted when the ring was empty, so a single fresh report
    # hid every older entry.
    merged: list[dict] = []
    sources = {"db": 0, "memory": 0, "file": 0}

    db_items = await _load_playback_errors_from_db(capped, device)
    sources["db"] = len(db_items)
    merged.extend(db_items)

    sources["memory"] = len(PLAYBACK_ERRORS)
    merged.extend(PLAYBACK_ERRORS)

    if os.path.exists(PLAYBACK_ERRORS_FILE):
        try:
            with open(PLAYBACK_ERRORS_FILE, encoding="utf-8") as f:
                file_items = [json.loads(line) for line in f if line.strip()]
            file_items = file_items[-PLAYBACK_ERRORS_MAX:]
            sources["file"] = len(file_items)
            merged.extend(file_items)
        except Exception as e:
            print(f"[errlog] failed to read persisted errors: {e}")

    # Dedupe on the ingest timestamp + device + message; the DB row is stamped
    # with the same serverTime the ring entry carries, so copies collapse.
    seen: set = set()
    unique: list[dict] = []
    for e in merged:
        key = (e.get("serverTime"), e.get("deviceId"), (e.get("message") or "")[:120])
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)

    if device:
        unique = [e for e in unique if e.get("deviceId") == device]

    # Newest first. serverTime is ISO-8601 so lexical sort is chronological.
    unique.sort(key=lambda e: e.get("serverTime") or "", reverse=True)
    out = unique[:capped]
    if not include_logs:
        out = [{k: v for k, v in e.items() if k != "recentLogs"} for e in out]
    return {"count": len(out), "total": len(unique), "sources": sources, "errors": out}


async def _load_playback_errors_from_db(limit: int, device: str) -> list[dict]:
    """Durable reports from Postgres, shaped like the in-memory entries.

    Returns [] (never raises) when the DB is unavailable, so the viewer keeps
    working off the ring/file during a database outage.
    """
    import db as _db
    from sqlalchemy import select as _select

    factory = _db.try_session_factory()
    if factory is None:
        return []
    try:
        async with factory() as session:
            stmt = _select(models.PlaybackErrorLog)
            if device:
                stmt = stmt.where(models.PlaybackErrorLog.device_id == device)
            stmt = stmt.order_by(models.PlaybackErrorLog.received_at.desc()).limit(limit)
            rows = (await session.execute(stmt)).scalars().all()
    except Exception as e:
        print(f"[errlog] DB read failed (non-fatal): {type(e).__name__}: {e}")
        return []

    return [{
        "deviceId": r.device_id or "",
        "deviceModel": r.device_model or "",
        "androidVersion": r.android_version or "",
        "appVersion": r.app_version or "",
        "appVersionCode": r.app_version_code or 0,
        "network": r.network or "",
        "errorType": r.error_type or "",
        "songId": r.song_id or "",
        "songTitle": r.song_title or "",
        "message": r.message or "",
        "recentLogs": r.recent_logs or "",
        "serverTime": r.received_at.isoformat() if r.received_at else "",
        "clientIp": r.client_ip or "",
    } for r in rows]


# TEMPORARY diagnostic for the listen_events push 500s (2026-07-14).
# Read-only apart from an optional, idempotent sequence repair. Remove
# once the sync failure is resolved.
@api.get("/debug/listen-events-write-check")
async def listen_events_write_check(
    repair: bool = False,
    session=Depends(get_session),
):
    from sqlalchemy import text as _text

    out: dict = {}
    try:
        seq_name = (
            await session.execute(
                _text("SELECT pg_get_serial_sequence('user_listen_events','id')")
            )
        ).scalar()
        out["sequenceName"] = seq_name
        if seq_name:
            row = (
                await session.execute(
                    _text(f"SELECT last_value, is_called FROM {seq_name}")
                )
            ).first()
            out["sequenceLastValue"] = row[0]
            out["sequenceIsCalled"] = row[1]
    except Exception as e:
        out["sequenceError"] = repr(e)[:300]

    try:
        row = (
            await session.execute(
                _text("SELECT COALESCE(MAX(id),0), COUNT(*) FROM user_listen_events")
            )
        ).first()
        out["maxId"] = row[0]
        out["rowCount"] = row[1]
    except Exception as e:
        out["tableError"] = repr(e)[:300]

    # Canary insert, always rolled back — captures the real write error.
    try:
        uid = (
            await session.execute(_text("SELECT id FROM users LIMIT 1"))
        ).scalar()
        nested = await session.begin_nested()
        await session.execute(
            _text(
                "INSERT INTO user_listen_events "
                "(user_id, video_id, played_at, received_at) "
                "VALUES (:u, 'canary-check', now(), now())"
            ),
            {"u": uid},
        )
        await nested.rollback()
        out["insertCheck"] = "ok"
    except Exception as e:
        out["insertCheck"] = repr(e)[:600]
    finally:
        try:
            await session.rollback()
        except Exception:
            pass

    # Sweep every autoincrement table the migration copied: a sequence
    # sitting below MAX(id) makes every INSERT collide (duplicate pkey).
    out["sequences"] = {}
    for tbl in ("user_listen_events", "dm_messages", "lounge_messages"):
        info: dict = {}
        try:
            seq = (
                await session.execute(
                    _text(f"SELECT pg_get_serial_sequence('{tbl}','id')")
                )
            ).scalar()
            info["sequence"] = seq
            if seq:
                info["lastValue"] = (
                    await session.execute(_text(f"SELECT last_value FROM {seq}"))
                ).scalar()
            info["maxId"] = (
                await session.execute(
                    _text(f"SELECT COALESCE(MAX(id),0) FROM {tbl}")
                )
            ).scalar()
            info["behind"] = bool(seq) and info["lastValue"] < info["maxId"]
            if repair and info.get("behind"):
                info["repairedTo"] = (
                    await session.execute(
                        _text(
                            f"SELECT setval('{seq}', "
                            f"(SELECT COALESCE(MAX(id),0)+1 FROM {tbl}), false)"
                        )
                    )
                ).scalar()
                await session.commit()
        except Exception as e:
            info["error"] = repr(e)[:300]
            try:
                await session.rollback()
            except Exception:
                pass
        out["sequences"][tbl] = info

    return out


@api.get("/rooms", response_model=RoomListResponse)
async def list_rooms():
    """List all active rooms for discovery"""
    rooms = room_manager.list_rooms()
    return RoomListResponse(success=True, rooms=[RoomListItem(**r) for r in rooms])


# ==================== SHARE ENDPOINTS ====================


@app.get("/share/song/{video_id}", response_class=HTMLResponse)
async def share_song_page(video_id: str, request: Request):
    """HTML page with Open Graph tags for song link previews."""
    result = await extract_audio_url(video_id)
    title = (
        result.get("title", "Unknown Song") if result.get("success") else "Unknown Song"
    )
    uploader = (
        result.get("uploader", "Unknown Artist")
        if result.get("success")
        else "Unknown Artist"
    )
    thumbnail = result.get("thumbnail", "") if result.get("success") else ""
    duration = result.get("duration")
    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else ""

    description = f"{uploader}"
    if duration_str:
        description += f" • {duration_str}"

    deep_link = f"syncaura://song/{video_id}"

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta property="og:title" content="{title}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:image" content="{thumbnail}" />
    <meta property="og:url" content="{request.url}" />
    <meta property="og:type" content="music.song" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta http-equiv="refresh" content="0;url={deep_link}" />
    <title>{title} - AudioSync</title>
    <style>
        body {{ background: #121212; color: #fff; font-family: -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
        .card {{ background: #1e1e1e; border-radius: 16px; padding: 32px; max-width: 400px; text-align: center; }}
        .thumb {{ width: 200px; height: 200px; border-radius: 12px; object-fit: cover; margin-bottom: 16px; }}
        h2 {{ margin: 8px 0 4px; }}
        .artist {{ color: #888; margin-bottom: 20px; }}
        .btn {{ display: inline-block; background: #1DB954; color: #fff; padding: 12px 32px; border-radius: 24px; text-decoration: none; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="card">
        {"<img class='thumb' src='" + thumbnail + "' />" if thumbnail else ""}
        <h2>{title}</h2>
        <p class="artist">{description}</p>
        <a class="btn" href="{deep_link}">Open in AudioSync</a>
    </div>
</body>
</html>"""


@app.get("/share/room/{room_code}", response_class=HTMLResponse)
async def share_room_page(
    room_code: str, request: Request, invite: Optional[str] = None
):
    """HTML page with Open Graph tags for room invite link previews."""
    room = room_manager.rooms.get(room_code.upper())

    if not room:
        return HTMLResponse(
            content=f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>AudioSync</title>
<style>body {{ background: #121212; color: #fff; font-family: sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
.card {{ background: #1e1e1e; border-radius: 16px; padding: 32px; text-align: center; }} .btn {{ display: inline-block; background: #1DB954; color: #fff; padding: 12px 32px; border-radius: 24px; text-decoration: none; font-weight: bold; margin-top: 16px; }}</style>
</head><body><div class="card"><h2>Room Not Available</h2><p style="color:#888">This room no longer exists or has been closed.</p></div></body></html>""",
            status_code=200,
        )

    host_name = room.host_name
    member_count = len(room.members)
    current_song = room.current_song.get("title") if room.current_song else None

    title = f"{host_name}'s Room"
    description = f"{member_count} listening"
    if current_song:
        description += f" • ♪ {current_song}"

    deep_link = f"syncaura://room/{room_code.upper()}"
    if invite:
        deep_link += f"?invite={invite}"

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta property="og:title" content="{title}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:type" content="website" />
    <meta property="og:url" content="{request.url}" />
    <meta name="twitter:card" content="summary" />
    <meta http-equiv="refresh" content="0;url={deep_link}" />
    <title>{title} - AudioSync</title>
    <style>
        body {{ background: #121212; color: #fff; font-family: -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
        .card {{ background: #1e1e1e; border-radius: 16px; padding: 32px; max-width: 400px; text-align: center; }}
        h2 {{ margin: 8px 0 4px; }}
        .info {{ color: #888; margin-bottom: 20px; }}
        .btn {{ display: inline-block; background: #1DB954; color: #fff; padding: 12px 32px; border-radius: 24px; text-decoration: none; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="card">
        <h2>{title}</h2>
        <p class="info">{description}</p>
        <a class="btn" href="{deep_link}">Join in AudioSync</a>
    </div>
</body>
</html>"""


@api.get("/room/{room_code}")
async def get_room_info(room_code: str):
    """JSON endpoint for app to fetch room info when handling deep links."""
    room = room_manager.rooms.get(room_code.upper())
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")
    return {
        "code": room.code,
        "hostName": room.host_name,
        "memberCount": len(room.members),
        "hasPassword": room.password is not None,
        "currentSong": room.current_song.get("title") if room.current_song else None,
        "currentSongThumbnail": room.current_song.get("thumbnail")
        if room.current_song
        else None,
    }


class InviteRequest(BaseModel):
    clientId: str


@api.post("/room/{room_code}/invite")
async def create_room_invite(room_code: str, req: InviteRequest):
    """Generate a single-use invite token for a locked room."""
    room = room_manager.rooms.get(room_code.upper())
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")
    if req.clientId not in room.members:
        raise HTTPException(status_code=403, detail="Not a member of this room")
    token = room.create_invite_token()
    return {"token": token}


# ==================== SHARED PLAYLISTS ====================

PLAYLISTS_FILE = os.path.join(os.path.dirname(__file__), "shared_playlists.json")
shared_playlists: dict = {}


def _load_shared_playlists():
    global shared_playlists
    if os.path.exists(PLAYLISTS_FILE):
        try:
            with open(PLAYLISTS_FILE, "r") as f:
                shared_playlists = json.load(f)
            print(f"[SharedPlaylists] Loaded {len(shared_playlists)} playlists")
        except Exception as e:
            print(f"[SharedPlaylists] Failed to load: {e}")
            shared_playlists = {}


def _save_shared_playlists():
    try:
        with open(PLAYLISTS_FILE, "w") as f:
            json.dump(shared_playlists, f)
    except Exception as e:
        print(f"[SharedPlaylists] Failed to save: {e}")


_load_shared_playlists()


class SharedPlaylistSong(BaseModel):
    videoId: str
    title: str
    uploader: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    position: int


class SharePlaylistRequest(BaseModel):
    name: str
    canEdit: bool = False
    songs: List[SharedPlaylistSong]


class UpdatePlaylistRequest(BaseModel):
    name: str
    songs: List[SharedPlaylistSong]


@api.post("/playlist/share")
async def create_shared_playlist(req: SharePlaylistRequest):
    """Upload a playlist to share. Returns shareId and ownerToken."""
    share_id = str(uuid.uuid4())[:8]
    owner_token = str(uuid.uuid4())
    now = time.time()

    shared_playlists[share_id] = {
        "shareId": share_id,
        "name": req.name,
        "ownerToken": owner_token,
        "canEdit": req.canEdit,
        "songs": [s.dict() for s in req.songs],
        "version": 1,
        "createdAt": now,
        "updatedAt": now,
    }
    _save_shared_playlists()
    analytics.log_event(
        "playlist_share",
        detail=json.dumps(
            {"shareId": share_id, "songs": len(req.songs), "canEdit": req.canEdit}
        ),
    )

    return {"shareId": share_id, "ownerToken": owner_token, "version": 1}


@api.get("/playlist/{share_id}")
async def get_shared_playlist(share_id: str):
    """Fetch shared playlist data (songs + metadata). Never exposes ownerToken."""
    playlist = shared_playlists.get(share_id)
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist not found")
    return {
        "shareId": playlist["shareId"],
        "name": playlist["name"],
        "canEdit": playlist.get("canEdit", False),
        "songs": playlist["songs"],
        "version": playlist["version"],
        "songCount": len(playlist["songs"]),
    }


@api.put("/playlist/{share_id}")
async def update_shared_playlist(
    share_id: str, req: UpdatePlaylistRequest, request: Request
):
    """Update shared playlist songs. Requires ownerToken if canEdit is false."""
    playlist = shared_playlists.get(share_id)
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist not found")

    # Auth check: if canEdit is disabled, require owner token
    if not playlist.get("canEdit", False):
        token = request.headers.get("x-owner-token", "")
        if token != playlist["ownerToken"]:
            raise HTTPException(status_code=403, detail="Not authorized")

    playlist["name"] = req.name
    playlist["songs"] = [s.dict() for s in req.songs]
    playlist["version"] += 1
    playlist["updatedAt"] = time.time()
    _save_shared_playlists()
    analytics.log_event(
        "playlist_update",
        detail=json.dumps(
            {
                "shareId": share_id,
                "version": playlist["version"],
                "songs": len(req.songs),
            }
        ),
    )

    return {"version": playlist["version"]}


@api.get("/playlist/{share_id}/version")
async def get_shared_playlist_version(share_id: str):
    """Lightweight version check — returns just version and song count."""
    playlist = shared_playlists.get(share_id)
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist not found")
    return {
        "version": playlist["version"],
        "songCount": len(playlist["songs"]),
        "name": playlist["name"],
        "updatedAt": playlist["updatedAt"],
    }


@app.get("/share/playlist/{share_id}", response_class=HTMLResponse)
async def share_playlist_page(share_id: str, request: Request):
    """HTML page with Open Graph tags for playlist link previews."""
    playlist = shared_playlists.get(share_id)

    if not playlist:
        return HTMLResponse(
            content=f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>AudioSync</title>
<style>body {{ background: #121212; color: #fff; font-family: sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
.card {{ background: #1e1e1e; border-radius: 16px; padding: 32px; text-align: center; }}</style>
</head><body><div class="card"><h2>Playlist Not Found</h2><p style="color:#888">This shared playlist no longer exists.</p></div></body></html>""",
            status_code=200,
        )

    name = playlist["name"]
    song_count = len(playlist["songs"])
    thumbnail = (
        playlist["songs"][0]["thumbnail"]
        if playlist["songs"] and playlist["songs"][0].get("thumbnail")
        else ""
    )
    description = f"{song_count} {'song' if song_count == 1 else 'songs'}"

    if playlist["songs"]:
        titles = [s["title"] for s in playlist["songs"][:3]]
        description += " — " + ", ".join(titles)
        if song_count > 3:
            description += f" +{song_count - 3} more"

    deep_link = f"syncaura://playlist/{share_id}"

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta property="og:title" content="{name}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:image" content="{thumbnail}" />
    <meta property="og:url" content="{request.url}" />
    <meta property="og:type" content="music.playlist" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta http-equiv="refresh" content="0;url={deep_link}" />
    <title>{name} - AudioSync</title>
    <style>
        body {{ background: #121212; color: #fff; font-family: -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
        .card {{ background: #1e1e1e; border-radius: 16px; padding: 32px; max-width: 400px; text-align: center; }}
        .thumb {{ width: 200px; height: 200px; border-radius: 12px; object-fit: cover; margin-bottom: 16px; }}
        h2 {{ margin: 8px 0 4px; }}
        .info {{ color: #888; margin-bottom: 20px; }}
        .btn {{ display: inline-block; background: #1DB954; color: #fff; padding: 12px 32px; border-radius: 24px; text-decoration: none; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="card">
        {"<img class='thumb' src='" + thumbnail + "' />" if thumbnail else ""}
        <h2>{name}</h2>
        <p class="info">{description}</p>
        <a class="btn" href="{deep_link}">Open in AudioSync</a>
    </div>
</body>
</html>"""


async def extract_audio_url(video_id: str) -> dict:
    """Reusable audio extraction — used by /audio endpoint and WebSocket play handler.
    Returns dict with success, videoId, url, title, duration, thumbnail, uploader, source."""
    # Check cache first
    cached = get_cached(video_id)
    if cached:
        return cached

    # Try Piped first (fast ~300-500ms)
    piped_result = await get_from_piped(video_id)
    if piped_result and piped_result.get("url"):
        result = {
            "success": True,
            "videoId": video_id,
            "url": piped_result["url"],
            "title": piped_result.get("title"),
            "duration": piped_result.get("duration"),
            "thumbnail": piped_result.get("thumbnail"),
            "uploader": piped_result.get("uploader"),
            "source": "piped",
        }
        set_cache(video_id, result)
        return result

    # Fallback to yt-dlp (slow but reliable)
    ytdlp_result = await get_audio_ytdlp(video_id)
    if ytdlp_result.success:
        result = {
            "success": True,
            "videoId": video_id,
            "url": ytdlp_result.url,
            "title": ytdlp_result.title,
            "duration": ytdlp_result.duration,
            "thumbnail": ytdlp_result.thumbnail,
            "uploader": ytdlp_result.uploader,
            "source": "ytdlp",
        }
        set_cache(video_id, result)
        return result

    return {"success": False, "videoId": video_id, "error": ytdlp_result.error}


@api.get("/audio/{video_id}", response_model=AudioResponse)
async def get_audio(video_id: str, fresh: bool = False):
    """
    Extract audio URL for a YouTube video.
    Tries Piped first (fast), falls back to yt-dlp (reliable).
    Pass fresh=true to bypass cache (used on playback retry).
    """
    start = time.time()
    print(f"[/audio] Request: {video_id} (fresh={fresh})")

    if fresh and video_id in _cache:
        del _cache[video_id]

    result = await extract_audio_url(video_id)
    source = result.get("source", "unknown")
    print(f"[/audio] {video_id} → {source} ({time.time() - start:.2f}s)")
    analytics.log_event(
        "audio_extract",
        video_id=video_id,
        detail=json.dumps(
            {"source": source, "time_ms": round((time.time() - start) * 1000)}
        ),
    )

    return AudioResponse(**result)


@api.get("/stream/{video_id}", response_model=StreamResponse)
async def get_stream(video_id: str, include_suggestions: bool = True):
    """
    Get stream URL + suggestions (for prefetching next song).
    Tries Piped first, falls back to yt-dlp.
    Returns audio URL, metadata, and next suggestions in one call.
    """
    start = time.time()
    print(f"[/stream] Request: {video_id}")

    suggestions = []

    # Check cache first
    cached = get_cached(video_id)
    if cached and cached.get("url"):
        # Got cached URL, check suggestions cache too
        if include_suggestions:
            cached_sug = get_cached_suggestions(video_id)
            if cached_sug:
                suggestions = [StreamSuggestion(**r) for r in cached_sug[:50]]
                print(
                    f"[/stream] {video_id} → FULL CACHE HIT ({time.time() - start:.2f}s)"
                )
            else:
                t1 = time.time()
                related_response = await get_related_ytdlp(video_id, limit=50)
                print(
                    f"[/stream] {video_id} → suggestions fetch: {time.time() - t1:.2f}s"
                )
                if related_response.success:
                    suggestions = [
                        StreamSuggestion(
                            videoId=r.videoId,
                            title=r.title,
                            duration=r.duration,
                            thumbnail=r.thumbnail,
                            uploader=r.uploader,
                        )
                        for r in related_response.related
                    ]
                    set_suggestions_cache(
                        video_id, [r.model_dump() for r in related_response.related]
                    )
        print(
            f"[/stream] {video_id} → CACHE HIT + {len(suggestions)} suggestions ({time.time() - start:.2f}s)"
        )
        analytics.log_event(
            "song_play",
            video_id=video_id,
            title=cached.get("title"),
            detail=json.dumps({"source": "cache"}),
        )
        return StreamResponse(
            success=True,
            videoId=video_id,
            audioUrl=cached["url"],
            title=cached.get("title"),
            duration=cached.get("duration"),
            thumbnail=cached.get("thumbnail"),
            uploader=cached.get("uploader"),
            suggestions=suggestions,
        )

    # Try Piped first (fast, includes related videos)
    piped_result = await get_from_piped(video_id)
    if piped_result and piped_result.get("url"):
        # Cache it
        set_cache(
            video_id,
            {
                "success": True,
                "videoId": video_id,
                "url": piped_result["url"],
                "title": piped_result.get("title"),
                "duration": piped_result.get("duration"),
                "thumbnail": piped_result.get("thumbnail"),
                "uploader": piped_result.get("uploader"),
                "source": "piped",
            },
        )

        # Get suggestions from Piped response
        if include_suggestions and piped_result.get("related"):
            suggestions = [
                StreamSuggestion(
                    videoId=r["videoId"],
                    title=r["title"],
                    duration=r.get("duration"),
                    thumbnail=r.get("thumbnail"),
                    uploader=r.get("uploader"),
                )
                for r in piped_result["related"][:50]
            ]

        analytics.log_event(
            "song_play",
            video_id=video_id,
            title=piped_result.get("title"),
            detail=json.dumps({"source": "piped"}),
        )
        return StreamResponse(
            success=True,
            videoId=video_id,
            audioUrl=piped_result["url"],
            title=piped_result.get("title"),
            duration=piped_result.get("duration"),
            thumbnail=piped_result.get("thumbnail"),
            uploader=piped_result.get("uploader"),
            suggestions=suggestions,
        )

    # Fallback to yt-dlp - fetch audio + suggestions IN PARALLEL
    t1 = time.time()
    if include_suggestions:
        ytdlp_result, related_response = await asyncio.gather(
            get_audio_ytdlp(video_id), get_related_ytdlp(video_id, limit=50)
        )
    else:
        ytdlp_result = await get_audio_ytdlp(video_id)
        related_response = None
    print(f"[/stream] {video_id} → yt-dlp parallel fetch: {time.time() - t1:.2f}s")

    if ytdlp_result.success and ytdlp_result.url:
        # Cache yt-dlp result
        set_cache(
            video_id,
            {
                "success": True,
                "videoId": video_id,
                "url": ytdlp_result.url,
                "title": ytdlp_result.title,
                "duration": ytdlp_result.duration,
                "thumbnail": ytdlp_result.thumbnail,
                "uploader": ytdlp_result.uploader,
                "source": "ytdlp",
            },
        )

        # Use suggestions from parallel fetch + cache them
        if related_response and related_response.success:
            suggestions = [
                StreamSuggestion(
                    videoId=r.videoId,
                    title=r.title,
                    duration=r.duration,
                    thumbnail=r.thumbnail,
                    uploader=r.uploader,
                )
                for r in related_response.related
            ]
            set_suggestions_cache(
                video_id, [r.model_dump() for r in related_response.related]
            )

        print(
            f"[/stream] {video_id} → yt-dlp OK + {len(suggestions)} suggestions ({time.time() - start:.2f}s total)"
        )
        analytics.log_event(
            "song_play",
            video_id=video_id,
            title=ytdlp_result.title,
            detail=json.dumps({"source": "ytdlp"}),
        )
        return StreamResponse(
            success=True,
            videoId=video_id,
            audioUrl=ytdlp_result.url,
            title=ytdlp_result.title,
            duration=ytdlp_result.duration,
            thumbnail=ytdlp_result.thumbnail,
            uploader=ytdlp_result.uploader,
            suggestions=suggestions,
        )

    print(f"[/stream] {video_id} → FAILED ({time.time() - start:.2f}s)")
    return StreamResponse(
        success=False, videoId=video_id, error="Failed to get stream URL"
    )


@api.get("/related/{video_id}", response_model=RelatedResponse)
async def get_related(video_id: str, limit: int = 50):
    """
    Get related songs (suggestions).
    Checks cache first, then Piped, then yt-dlp.
    """
    start = time.time()
    print(f"[/related] Request: {video_id}")

    # Check suggestions cache first
    cached = get_cached_suggestions(video_id)
    if cached:
        print(f"[/related] {video_id} → CACHE HIT ({time.time() - start:.2f}s)")
        return RelatedResponse(
            success=True,
            videoId=video_id,
            related=[SearchResult(**r) for r in cached[:limit]],
            source="cache",
        )

    # Try Piped first (fast, includes related)
    piped_result = await get_from_piped(video_id)
    if piped_result and piped_result.get("related"):
        # Cache suggestions
        set_suggestions_cache(video_id, piped_result["related"])
        print(f"[/related] {video_id} → PIPED ({time.time() - start:.2f}s)")
        return RelatedResponse(
            success=True,
            videoId=video_id,
            related=[SearchResult(**r) for r in piped_result["related"][:limit]],
            source="piped",
        )

    # Fallback to yt-dlp
    result = await get_related_ytdlp(video_id, limit)
    if result.success and result.related:
        # Cache suggestions
        set_suggestions_cache(video_id, [r.model_dump() for r in result.related])
    print(
        f"[/related] {video_id} → yt-dlp: {len(result.related)} results ({time.time() - start:.2f}s)"
    )
    return result


@api.get("/prefetch", response_model=PrefetchResponse)
async def prefetch_batch(video_ids: str):
    """
    Prefetch multiple audio URLs in parallel (for rapid skipping).
    Accepts comma-separated video IDs, max 10.
    """
    ids = [vid.strip() for vid in video_ids.split(",") if vid.strip()][:10]

    if not ids:
        return PrefetchResponse(success=False, results=[])

    async def fetch_one(vid: str) -> PrefetchResult:
        # Check cache first
        cached = get_cached(vid)
        if cached and cached.get("url"):
            return PrefetchResult(videoId=vid, audioUrl=cached["url"], success=True)

        # Try Piped
        result = await get_from_piped(vid)
        if result and result.get("url"):
            # Cache it
            set_cache(
                vid,
                {
                    "success": True,
                    "videoId": vid,
                    "url": result["url"],
                    "title": result.get("title"),
                    "duration": result.get("duration"),
                    "thumbnail": result.get("thumbnail"),
                    "uploader": result.get("uploader"),
                    "source": "piped",
                },
            )
            return PrefetchResult(videoId=vid, audioUrl=result["url"], success=True)

        # Don't fallback to yt-dlp for prefetch (too slow)
        # Return failure, app will fetch individually if needed
        return PrefetchResult(videoId=vid, audioUrl=None, success=False)

    # Fetch all in parallel
    results = await asyncio.gather(*[fetch_one(vid) for vid in ids])

    return PrefetchResponse(success=True, results=list(results))


@api.get("/search", response_model=SearchResponse)
async def search(q: str, limit: int = 30):
    """Search YouTube videos using yt-dlp (no Piped equivalent)"""
    start = time.time()
    print(f"[/search] Request: '{q}'")

    if not q or len(q.strip()) == 0:
        return SearchResponse(success=False, query=q, error="Query cannot be empty")

    # Cap the page size — 30 is the product max. extract_flat keeps this
    # cheap, but beyond ~30 latency rises for low-relevance tail results,
    # so clamp regardless of what the client requests.
    limit = max(1, min(limit, 30))

    def _search_ytdlp():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }
        ydl_opts.update(_get_cookie_opts())

        try:
            search_query = f"ytsearch{limit}:{q}"

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(search_query, download=False)

                if not info or "entries" not in info:
                    return SearchResponse(
                        success=False, query=q, error="No results found"
                    )

                results = []
                for entry in info["entries"]:
                    if entry:
                        results.append(
                            SearchResult(
                                videoId=entry.get("id", ""),
                                title=entry.get("title", "Unknown"),
                                duration=entry.get("duration"),
                                thumbnail=get_best_thumbnail(entry),
                                uploader=entry.get("uploader")
                                or entry.get("channel", "Unknown"),
                            )
                        )

                return SearchResponse(success=True, query=q, results=results)

        except Exception as e:
            return SearchResponse(success=False, query=q, error=str(e))

    result = await asyncio.to_thread(_search_ytdlp)
    print(
        f"[/search] '{q}' → {len(result.results)} results ({time.time() - start:.2f}s)"
    )
    analytics.log_event(
        "search",
        query=q,
        detail=json.dumps(
            {
                "results": len(result.results),
                "time_ms": round((time.time() - start) * 1000),
            }
        ),
    )
    return result


@api.get("/next/{video_id}", response_model=NextResponse)
async def get_next(video_id: str):
    """
    Get next song with audio URL and suggestions.
    Tries Piped first for speed.
    """

    # Try Piped first (gets related in same call)
    piped_result = await get_from_piped(video_id)

    if piped_result and piped_result.get("related"):
        related = piped_result["related"]

        if related:
            # Get first related video
            first_related = related[0]
            next_video_id = first_related["videoId"]

            # Get audio URL for next video (also via Piped)
            next_piped = await get_from_piped(next_video_id)

            if next_piped and next_piped.get("url"):
                # Success! Return with Piped data
                return NextResponse(
                    success=True,
                    currentVideoId=video_id,
                    nextSong=NextSongInfo(
                        videoId=next_video_id,
                        title=next_piped.get(
                            "title", first_related.get("title", "Unknown")
                        ),
                        duration=next_piped.get(
                            "duration", first_related.get("duration")
                        ),
                        thumbnail=next_piped.get(
                            "thumbnail", first_related.get("thumbnail")
                        ),
                        uploader=next_piped.get(
                            "uploader", first_related.get("uploader")
                        ),
                        audioUrl=next_piped["url"],
                    ),
                    suggestions=[
                        SearchResult(**r) for r in related[1:51]
                    ],  # Skip first, next 50
                )

    # Fallback to yt-dlp
    # Step 1: Get related songs
    related_result = await get_related_ytdlp(video_id, limit=1)

    if not related_result.success or not related_result.related:
        return NextResponse(
            success=False, currentVideoId=video_id, error="No related songs found"
        )

    # Step 2: Get the first related song
    next_song_info = related_result.related[0]
    next_video_id = next_song_info.videoId

    # Step 3+4: Get audio URL AND suggestions IN PARALLEL
    audio_result, next_suggestions = await asyncio.gather(
        get_audio_ytdlp(next_video_id), get_related_ytdlp(next_video_id, limit=50)
    )

    if not audio_result.success or not audio_result.url:
        return NextResponse(
            success=False,
            currentVideoId=video_id,
            error=f"Failed to get audio URL: {audio_result.error}",
        )

    return NextResponse(
        success=True,
        currentVideoId=video_id,
        nextSong=NextSongInfo(
            videoId=next_video_id,
            title=next_song_info.title,
            duration=next_song_info.duration,
            thumbnail=next_song_info.thumbnail,
            uploader=next_song_info.uploader,
            audioUrl=audio_result.url,
        ),
        suggestions=next_suggestions.related if next_suggestions.success else [],
    )


# ==================== WEBSOCKET - LISTEN TOGETHER ====================


async def ws_send(websocket: WebSocket, message: dict):
    """Safe send — ignores if connection is closed"""
    try:
        await websocket.send_text(json.dumps(message))
    except Exception:
        pass


async def handle_create_room(client_id: str, websocket: WebSocket, msg: dict):
    # Leave any existing room first
    old_code, was_host, remaining_ws = room_manager.leave_room(client_id)
    if old_code and was_host:
        for ws in remaining_ws:
            await ws_send(ws, {"type": "room_closed"})

    host_name = msg.get("hostName", "Unknown")
    password = msg.get("password") or None  # treat empty string as None

    room = room_manager.create_room(
        client_id, websocket, host_name=host_name, password=password
    )
    print(
        f"[WS] Room {room.code} created by {client_id[:8]} ({host_name}), locked={password is not None}"
    )
    analytics.log_event(
        "room_create",
        client_id=client_id,
        client_name=host_name,
        room_code=room.code,
        detail=json.dumps({"locked": password is not None}),
    )

    # If the host was already playing a song when they created the room,
    # seed `room.current_song` immediately so the first guest to join
    # hears it. We don't broadcast `sync_play` here — the host is the
    # only member at this moment, and they're already playing locally.
    seed = msg.get("currentSong")
    if isinstance(seed, dict):
        seed_url = seed.get("audioUrl") or ""
        seed_video_id = seed.get("videoId") or ""
        if (
            seed_video_id
            and isinstance(seed_url, str)
            and seed_url.startswith("https://")
            and "googlevideo.com" in seed_url
        ):
            position_ms = seed.get("positionMs", 0)
            try:
                position_sec = float(position_ms) / 1000.0
            except (TypeError, ValueError):
                position_sec = 0.0
            room.current_song = {
                "videoId": seed_video_id,
                "title": seed.get("title", "") or "",
                "duration": seed.get("duration"),
                "thumbnail": seed.get("thumbnail", "") or "",
                "uploader": seed.get("uploader", "") or "",
                "audioUrl": seed_url,
            }
            room.position = position_sec
            room.is_playing = bool(seed.get("isPlaying", False))
            # Anchor `play_start_time` so `get_estimated_position()`
            # returns the right value when a new joiner asks for it:
            # estimated = position + (now - play_start_time).
            room.play_start_time = time.time()
            print(
                f"[WS] Room {room.code} seeded with currentSong "
                f"{seed_video_id} @ {position_sec:.2f}s (playing={room.is_playing})"
            )

    await ws_send(
        websocket,
        {
            "type": "room_created",
            "code": room.code,
            "hostName": host_name,
            "hasPassword": password is not None,
            "members": room_manager.get_member_list(room),
            # Persist this and send it back as memberSecret on rejoin. It is
            # what proves you are the host, instead of a client_id anyone in
            # the room can read off a member-list broadcast.
            "memberSecret": room.members[client_id].secret,
        },
    )


async def handle_join_room(client_id: str, websocket: WebSocket, msg: dict):
    code = msg.get("code", "").upper()
    if not code:
        await ws_send(websocket, {"type": "error", "message": "Room code required"})
        return

    # Leave any existing room first (prevent stale membership)
    old_code, was_host, remaining_ws = room_manager.leave_room(client_id)
    if old_code and was_host:
        for ws in remaining_ws:
            await ws_send(ws, {"type": "room_closed"})
    elif old_code:
        old_room = room_manager.rooms.get(old_code)
        if old_room:
            await room_manager.broadcast(
                old_room,
                {
                    "type": "member_left",
                    "count": len(old_room.members),
                    "members": room_manager.get_member_list(old_room),
                },
            )

    # Check room exists first (before joining) to validate password
    room = room_manager.rooms.get(code)
    if room is None:
        await ws_send(websocket, {"type": "error", "message": "Room not found"})
        return

    # Validate password if room is locked
    if room.password is not None:
        invite_token = msg.get("invite")
        if invite_token and room.validate_invite_token(invite_token):
            pass  # Valid invite token — bypass password
        else:
            provided = msg.get("password", "")
            if provided != room.password:
                await ws_send(websocket, {"type": "error", "message": "Wrong password"})
                return

    name = msg.get("name", "Unknown")
    room, promoted = room_manager.join_room(code, client_id, websocket, name=name)
    role = "host" if promoted else "guest"
    print(
        f"[WS] {client_id[:8]} ({name}) joined room {code} as {role} ({len(room.members)} members)"
    )
    analytics.log_event(
        "room_join",
        client_id=client_id,
        client_name=name,
        room_code=code,
        detail=json.dumps({"role": role, "members": len(room.members)}),
    )

    # Send current room state to the joiner (with personalized queue)
    state = {
        "code": room.code,
        "hostName": room.host_name,
        "memberCount": len(room.members),
        "currentSong": room.current_song,
        "position": room_manager.get_estimated_position(room),
        "isPlaying": room.is_playing,
        "queue": room.serialize_queue_for_client(client_id),
        "members": room_manager.get_member_list(room),
        "role": role,
    }
    # Defensive: a KeyError here would break joining outright, whereas an
    # absent secret merely falls back to the legacy rejoin path.
    _m = room.members.get(client_id)
    if _m:
        state["memberSecret"] = _m.secret
    await ws_send(websocket, {"type": "room_joined", "state": state})

    # Notify others with updated member list
    await room_manager.broadcast(
        room,
        {
            "type": "member_joined",
            "count": len(room.members),
            "members": room_manager.get_member_list(room),
        },
        exclude_id=client_id,
    )


async def handle_play(client_id: str, msg: dict):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    video_id = msg.get("videoId", "")
    if not video_id:
        return

    # Block host from playing a different song if there's a voted request (>1 vote)
    if room.has_voted_request():
        sorted_q = room.get_sorted_queue()
        top = sorted_q[0] if sorted_q and not sorted_q[0].is_suggestion else None
        if top and top.video_id != video_id:
            member = room.members.get(client_id)
            if member:
                await ws_send(
                    member.websocket,
                    {
                        "type": "play_blocked",
                        "reason": "voted_song_pending",
                        "topVideoId": top.video_id,
                        "topTitle": top.title,
                    },
                )
            return

    print(f"[WS] Room {room.code}: host playing {video_id}")
    analytics.log_event(
        "room_song_change",
        room_code=room.code,
        video_id=video_id,
        title=msg.get("title", ""),
    )
    room.songs_played += 1

    # Listen Together v2 (Option A): if the host already extracted the URL
    # on-device and shipped it in the `play` message, use that and skip
    # the server's own extraction. Empirically YouTube CDN URLs are
    # portable across IPs in our setup, so the host's URL plays fine for
    # co-listeners. Falls back to server-side extract if the field is
    # absent (older client) or rejected by sanity-check.
    host_supplied_url = msg.get("audioUrl") or ""
    is_valid_googlevideo = (
        isinstance(host_supplied_url, str)
        and host_supplied_url.startswith("https://")
        and "googlevideo.com" in host_supplied_url
    )

    if is_valid_googlevideo:
        print(f"[WS] Room {room.code}: using host-supplied URL for {video_id}")
        audio_data = {
            "success": True,
            "url": host_supplied_url,
            "title": msg.get("title", ""),
            "duration": msg.get("duration"),
            "thumbnail": msg.get("thumbnail", ""),
            "uploader": msg.get("uploader", ""),
        }
    else:
        # Legacy / fallback path: server extracts.
        audio_data = await extract_audio_url(video_id)
        if not audio_data.get("success") or not audio_data.get("url"):
            await room_manager.broadcast(
                room,
                {
                    "type": "error",
                    "message": f"Failed to extract audio: {audio_data.get('error', 'Unknown error')}",
                },
            )
            return

    # Update room state
    room.current_song = {
        "videoId": video_id,
        "title": audio_data.get("title") or msg.get("title", ""),
        "duration": audio_data.get("duration") or msg.get("duration"),
        "thumbnail": audio_data.get("thumbnail") or msg.get("thumbnail", ""),
        "uploader": audio_data.get("uploader") or msg.get("uploader", ""),
        "audioUrl": audio_data["url"],
    }
    room.position = 0.0
    room.is_playing = True
    room.play_start_time = time.time()

    # Broadcast to ALL members — each device calculates elapsed time and seeks
    play_start_time = int(time.time() * 1000)
    await room_manager.broadcast(
        room,
        {
            "type": "sync_play",
            "videoId": video_id,
            "title": room.current_song["title"],
            "audioUrl": audio_data["url"],
            "thumbnail": room.current_song["thumbnail"],
            "uploader": room.current_song["uploader"],
            "duration": room.current_song["duration"],
            "position": 0,
            "playStartTime": play_start_time,
        },
    )
    await send_fcm_to_disconnected_members(room.code)


async def handle_pause(client_id: str, msg: dict):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    position = msg.get("position", 0.0)
    room.is_playing = False
    room.position = position
    print(f"[WS] Room {room.code}: paused at {position:.1f}s")

    await room_manager.broadcast(
        room,
        {
            "type": "sync_pause",
            "position": position,
        },
        # The host initiated this action — echoing it back made the host
        # seek/pause against itself, wobbling the room's reference clock.
        exclude_id=client_id,
    )
    await send_fcm_to_disconnected_members(room.code)


async def handle_resume(client_id: str, msg: dict):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    position = msg.get("position", 0.0)
    room.is_playing = True
    room.position = position
    room.play_start_time = time.time()

    resume_time = int(time.time() * 1000)
    print(f"[WS] Room {room.code}: resumed at {position:.1f}s")

    await room_manager.broadcast(
        room,
        {
            "type": "sync_resume",
            "position": position,
            "resumeTime": resume_time,
        },
        # The host initiated this action — echoing it back made the host
        # seek/pause against itself, wobbling the room's reference clock.
        exclude_id=client_id,
    )
    await send_fcm_to_disconnected_members(room.code)


async def handle_seek(client_id: str, msg: dict):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    position = msg.get("position", 0.0)
    room.position = position
    if room.is_playing:
        room.play_start_time = time.time()
    print(f"[WS] Room {room.code}: seek to {position:.1f}s")

    await room_manager.broadcast(
        room,
        {
            "type": "sync_seek",
            "position": position,
        },
        # The host initiated this action — echoing it back made the host
        # seek/pause against itself, wobbling the room's reference clock.
        exclude_id=client_id,
    )
    await send_fcm_to_disconnected_members(room.code)


# ── Host-side URL extraction (queue-advance path) ───────────────────────
#
# Listen Together v2 step 2: when the server pops a song off the queue
# (via `next`) it doesn't know in advance which videoId will play, so the
# host can't pre-ship the audio URL the way they do for `play`. Instead
# the server asks the host to extract on-device with this request/
# response pair:
#
#   server -> host : { type: "extract_request", requestId, videoId }
#   host   -> server: { type: "extract_response", requestId, audioUrl }   (success)
#                  or: { type: "extract_response", requestId, error }      (failure)
#
# The server awaits the response with a bounded timeout. On timeout /
# error / host disconnect, it falls back to its own (legacy) extractor so
# the room never goes silent because of a single client hiccup.

_pending_extract_requests: dict = {}  # requestId -> asyncio.Future


async def request_url_from_host(
    host_ws,
    video_id: str,
    room_code: str,
    timeout: float = 8.0,
) -> Optional[str]:
    """Ask the host to extract `video_id` on-device. Returns URL or None."""
    request_id = uuid.uuid4().hex
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    _pending_extract_requests[request_id] = future
    try:
        await ws_send(
            host_ws,
            {
                "type": "extract_request",
                "requestId": request_id,
                "videoId": video_id,
            },
        )
        result = await asyncio.wait_for(future, timeout=timeout)
        if isinstance(result, dict) and result.get("audioUrl"):
            return result["audioUrl"]
        # Host returned an error response — caller will fall back.
        print(
            f"[WS] Room {room_code}: host extract response for {video_id} had no URL "
            f"(error={result.get('error') if isinstance(result, dict) else result!r})"
        )
        return None
    except asyncio.TimeoutError:
        print(
            f"[WS] Room {room_code}: extract_request for {video_id} timed out after {timeout}s; "
            f"falling back to server extract"
        )
        return None
    except Exception as e:
        print(f"[WS] Room {room_code}: extract_request for {video_id} raised: {e}")
        return None
    finally:
        _pending_extract_requests.pop(request_id, None)


async def handle_extract_response(client_id: str, msg: dict):
    """Resolve the matching pending future. Stale/unknown ids are ignored."""
    request_id = msg.get("requestId", "")
    if not request_id:
        return
    future = _pending_extract_requests.get(request_id)
    if future is None or future.done():
        # Either the request already timed out (Future got popped) or
        # the host is replaying. Drop silently.
        return
    future.set_result(msg)


async def handle_next(client_id: str):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room or not room.queue:
        return

    # Auto-skip cap: if a song can't be extracted (NewPipe rejects + server
    # yt-dlp dead, e.g. SOCKS chain offline, or the videoId is genuinely
    # restricted), pop the next one and try again instead of dead-ending
    # the room. Streaming services (Spotify / YT Music) do the same. Cap
    # the loop so a fully-broken queue doesn't churn forever.
    MAX_SKIPS = 5
    skipped: list[str] = []

    for attempt in range(MAX_SKIPS):
        # Pop from sorted queue (highest-voted request first, then suggestions)
        sorted_q = room.get_sorted_queue()
        if not sorted_q:
            break
        next_item = sorted_q[0]
        room.queue.remove(next_item)
        await room_manager.broadcast_queue(room)

        video_id = next_item.video_id
        print(
            f"[WS] Room {room.code}: next → {video_id} "
            f"(votes={len(next_item.votes)}, suggestion={next_item.is_suggestion}, attempt={attempt + 1})"
        )

        # ── Listen Together v2: prefer host-side extraction ──────────────
        # Ask the host first. If they fail / time out / are gone, fall back
        # to the server's legacy extract_audio_url path. If both fail,
        # auto-skip and try the next queued song.
        audio_data: Optional[dict] = None
        host_member = room.members.get(client_id)
        if host_member is not None:
            host_url = await request_url_from_host(
                host_member.websocket, video_id, room.code
            )
            if host_url and host_url.startswith("https://") and "googlevideo.com" in host_url:
                print(f"[WS] Room {room.code}: using host-extracted URL for {video_id}")
                audio_data = {
                    "success": True,
                    "url": host_url,
                    "title": next_item.title,
                    "duration": next_item.duration,
                    "thumbnail": next_item.thumbnail,
                    "uploader": next_item.uploader,
                }

        if audio_data is None:
            server_data = await extract_audio_url(video_id)
            if server_data.get("success") and server_data.get("url"):
                audio_data = server_data

        if audio_data is None:
            skipped.append(video_id)
            print(
                f"[WS] Room {room.code}: skipping unextractable {video_id} "
                f"({len(skipped)}/{MAX_SKIPS}); trying next queued song"
            )
            continue

        # ── Success — wire up room state and broadcast sync_play ────────
        analytics.log_event(
            "room_song_change",
            room_code=room.code,
            video_id=video_id,
            title=next_item.title,
        )
        room.songs_played += 1

        room.current_song = {
            "videoId": video_id,
            "title": audio_data.get("title") or next_item.title,
            "duration": audio_data.get("duration") or next_item.duration,
            "thumbnail": audio_data.get("thumbnail") or next_item.thumbnail,
            "uploader": audio_data.get("uploader") or next_item.uploader,
            "audioUrl": audio_data["url"],
        }
        room.position = 0.0
        room.is_playing = True
        room.play_start_time = time.time()

        play_start_time = int(time.time() * 1000)
        await room_manager.broadcast(
            room,
            {
                "type": "sync_play",
                "videoId": video_id,
                "title": room.current_song["title"],
                "audioUrl": audio_data["url"],
                "thumbnail": room.current_song["thumbnail"],
                "uploader": room.current_song["uploader"],
                "duration": room.current_song["duration"],
                "position": 0,
                "playStartTime": play_start_time,
            },
        )
        await send_fcm_to_disconnected_members(room.code)
        if skipped:
            print(
                f"[WS] Room {room.code}: auto-skipped {len(skipped)} song(s) "
                f"before landing on {video_id}: {skipped}"
            )
        return

    # Loop exited without a successful play — either the queue ran dry
    # mid-skip or we hit MAX_SKIPS. Tell the room so the host knows
    # auto-advance gave up and they can pick a different track.
    print(f"[WS] Room {room.code}: handle_next gave up after {len(skipped)} skip(s)")
    if skipped:
        await room_manager.broadcast(
            room,
            {
                "type": "error",
                "message": (
                    f"Skipped {len(skipped)} unplayable song(s) in a row. "
                    f"Try queueing different ones."
                ),
            },
        )


async def handle_queue_update(client_id: str, msg: dict):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    # Preserve existing requests, only replace suggestions
    from room_manager import QueueItem

    requests = [q for q in room.queue if not q.is_suggestion]
    new_suggestions = []
    for item in msg.get("queue", []):
        new_suggestions.append(
            QueueItem(
                video_id=item.get("videoId", ""),
                title=item.get("title", "Unknown"),
                duration=item.get("duration", 0),
                thumbnail=item.get("thumbnail", ""),
                uploader=item.get("uploader", ""),
                requested_by=client_id,
                requested_by_name=room.host_name,
                votes=set(),
                timestamp=time.time(),
                is_suggestion=True,
            )
        )
    room.queue = requests + new_suggestions
    await room_manager.broadcast_queue(room)


async def handle_position_report(client_id: str, msg: dict):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    position = msg.get("position", 0.0)
    room.position = position
    if room.is_playing:
        room.play_start_time = time.time()

    # Host beacon extras (new clients): hostServerTime is the host's clock
    # mapped to the server clock at capture — guests use it to compute the
    # report's true age. isPlaying/videoId let guests self-heal lost
    # pause/resume/play messages. Old clients simply omit them.
    host_server_time = msg.get("hostServerTime")
    host_is_playing = msg.get("isPlaying", True)
    host_video_id = msg.get("videoId", "")

    # Broadcast as `position_correction` (NOT `sync_seek`) so the client's
    # smart-drift handler runs — it has a 3s threshold and uses the
    # rolling-median ping-derived `serverTimeOffset` to compensate for
    # WS travel time. Sending this as `sync_seek` (the previous behavior)
    # made guests do an unconditional seek every 30s, causing audible
    # blips even when perfectly aligned. `sync_seek` is now reserved
    # for explicit host-initiated seeks only.
    await room_manager.broadcast(
        room,
        {
            "type": "position_correction",
            "position": position,
            "serverTime": int(time.time() * 1000),
            "hostServerTime": host_server_time or int(time.time() * 1000),
            "isPlaying": host_is_playing,
            "videoId": host_video_id,
        },
        exclude_id=client_id,
    )


async def handle_chat_message(client_id: str, msg: dict):
    text = msg.get("text", "").strip()
    if not text or len(text) > 500:
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    member = room.members.get(client_id)
    if not member:
        return

    from room_manager import ChatMessageRecord, MAX_CHAT_HISTORY

    msg_id = uuid.uuid4().hex
    now_ms = int(time.time() * 1000)

    # Optional reply-to fields. Client sends just the id; we look up the
    # quoted snippet from history so the broadcast is self-contained for
    # late joiners and for clients that pruned the original locally.
    reply_to_id = msg.get("replyToId")
    reply_to_sender_name = None
    reply_to_text = None
    if reply_to_id:
        for prev in room.messages:
            if prev.id == reply_to_id and not prev.deleted:
                reply_to_sender_name = prev.sender_name
                reply_to_text = (prev.text or "")[:120]
                break
        else:
            reply_to_id = None  # quoted message not found / was deleted

    record = ChatMessageRecord(
        id=msg_id,
        sender_id=client_id,
        sender_name=member.name,
        text=text,
        timestamp=now_ms,
        reply_to_id=reply_to_id,
        reply_to_sender_name=reply_to_sender_name,
        reply_to_text=reply_to_text,
    )
    room.messages.append(record)
    if len(room.messages) > MAX_CHAT_HISTORY:
        # Drop oldest first; reactions/replies keyed to dropped ids are
        # forgivably orphaned (clients hide unresolvable references).
        del room.messages[: len(room.messages) - MAX_CHAT_HISTORY]

    payload = {
        "type": "chat_message",
        "id": msg_id,
        "senderClientId": client_id,
        "senderName": member.name,
        "text": text,
        "timestamp": now_ms,
    }
    if reply_to_id:
        payload["replyToId"] = reply_to_id
        payload["replyToSenderName"] = reply_to_sender_name
        payload["replyToText"] = reply_to_text
    await room_manager.broadcast(room, payload)
    await send_fcm_to_disconnected_members(room.code)


async def handle_typing(client_id: str, msg: dict):
    """Lightweight presence ping while a member is composing.

    We don't broadcast every keystroke — clients send `typing` once when
    they start (debounced ~1s) and `typing_stop` when input clears or
    after a short idle. We re-broadcast verbatim so other members can
    show the "<name> is typing…" dots. Stale entries time out client-side
    via TYPING_TTL_MS so a dropped `typing_stop` doesn't pin the
    indicator forever.
    """
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    member = room.members.get(client_id)
    if not member:
        return
    is_typing = bool(msg.get("isTyping", True))
    if is_typing:
        room.typing[client_id] = int(time.time() * 1000)
    else:
        room.typing.pop(client_id, None)
    await room_manager.broadcast(
        room,
        {
            "type": "typing",
            "senderClientId": client_id,
            "senderName": member.name,
            "isTyping": is_typing,
        },
        exclude_id=client_id,
    )


async def handle_add_reaction(client_id: str, msg: dict):
    """Add an emoji reaction to a chat message.

    Reactions are a {emoji: set(clientId)} map per message. A given
    client can hold at most one reaction per emoji on a message; the
    same client adding the same emoji twice is a no-op (idempotent —
    safe under retry / double-tap).
    """
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    member = room.members.get(client_id)
    if not member:
        return
    msg_id = msg.get("messageId", "")
    emoji = (msg.get("emoji", "") or "").strip()
    if not msg_id or not emoji or len(emoji) > 16:
        return
    target = None
    for rec in room.messages:
        if rec.id == msg_id and not rec.deleted:
            target = rec
            break
    if not target:
        return
    bucket = target.reactions.setdefault(emoji, set())
    if client_id in bucket:
        return  # idempotent
    bucket.add(client_id)
    await room_manager.broadcast(
        room,
        {
            "type": "reaction_changed",
            "messageId": msg_id,
            "emoji": emoji,
            "senderClientId": client_id,
            "senderName": member.name,
            "added": True,
            "count": len(bucket),
        },
    )


async def handle_remove_reaction(client_id: str, msg: dict):
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    member = room.members.get(client_id)
    if not member:
        return
    msg_id = msg.get("messageId", "")
    emoji = (msg.get("emoji", "") or "").strip()
    if not msg_id or not emoji:
        return
    target = None
    for rec in room.messages:
        if rec.id == msg_id and not rec.deleted:
            target = rec
            break
    if not target:
        return
    bucket = target.reactions.get(emoji)
    if not bucket or client_id not in bucket:
        return
    bucket.discard(client_id)
    if not bucket:
        target.reactions.pop(emoji, None)
    await room_manager.broadcast(
        room,
        {
            "type": "reaction_changed",
            "messageId": msg_id,
            "emoji": emoji,
            "senderClientId": client_id,
            "senderName": member.name,
            "added": False,
            "count": len(bucket),
        },
    )


async def handle_edit_message(client_id: str, msg: dict):
    """Edit own message. Only the original sender can edit.

    We mutate the in-memory record so subsequent late-arriving features
    (history fetch, reaction lookups) see the new text. Broadcast the
    edited text + an `editedAt` timestamp so clients can show the
    "(edited)" subscript next to the bubble.
    """
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    msg_id = msg.get("messageId", "")
    new_text = (msg.get("text", "") or "").strip()
    if not msg_id or not new_text or len(new_text) > 500:
        return
    target = None
    for rec in room.messages:
        if rec.id == msg_id:
            target = rec
            break
    if not target or target.deleted:
        return
    if target.sender_id != client_id:
        return  # only sender can edit
    if target.is_suggestion or target.is_share_moment:
        return  # don't allow editing system/suggestion/moment cards
    target.text = new_text
    target.edited_at = int(time.time() * 1000)
    await room_manager.broadcast(
        room,
        {
            "type": "message_edited",
            "messageId": msg_id,
            "text": new_text,
            "editedAt": target.edited_at,
        },
    )


async def handle_delete_message(client_id: str, msg: dict):
    """Delete own message. Host can delete anyone's message (moderation).

    Soft-delete: we keep the record so reply-threads still resolve to
    "(deleted)" instead of vanishing. Reactions are wiped.
    """
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    msg_id = msg.get("messageId", "")
    if not msg_id:
        return
    target = None
    for rec in room.messages:
        if rec.id == msg_id:
            target = rec
            break
    if not target or target.deleted:
        return
    is_host = room.host_id == client_id
    if target.sender_id != client_id and not is_host:
        return
    target.deleted = True
    target.text = ""
    target.reactions.clear()
    await room_manager.broadcast(
        room,
        {
            "type": "message_deleted",
            "messageId": msg_id,
            "deletedBy": client_id,
            "byHost": is_host and target.sender_id != client_id,
        },
    )


async def handle_share_moment(client_id: str, msg: dict):
    """Share a timestamped moment of the current song into chat.

    Body: { videoId, title, thumbnail, positionMs, note? }.
    Becomes a special `share_moment` chat card so other members can tap
    and seek directly to that point. We broadcast a chat-message-shaped
    payload with an extra `moment` block; clients render it as a card.
    """
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    member = room.members.get(client_id)
    if not member:
        return
    video_id = msg.get("videoId", "")
    title = msg.get("title", "")
    thumbnail = msg.get("thumbnail", "")
    position_ms = int(msg.get("positionMs", 0) or 0)
    note = (msg.get("note", "") or "").strip()[:200]
    if not video_id or position_ms < 0:
        return

    from room_manager import ChatMessageRecord, MAX_CHAT_HISTORY

    msg_id = uuid.uuid4().hex
    now_ms = int(time.time() * 1000)
    text = note if note else f'shared a moment from "{title}"'
    record = ChatMessageRecord(
        id=msg_id,
        sender_id=client_id,
        sender_name=member.name,
        text=text,
        timestamp=now_ms,
        is_suggestion=False,
        is_share_moment=True,
        suggestion_video_id=video_id,
        suggestion_title=title,
        suggestion_thumbnail=thumbnail,
    )
    room.messages.append(record)
    if len(room.messages) > MAX_CHAT_HISTORY:
        del room.messages[: len(room.messages) - MAX_CHAT_HISTORY]
    await room_manager.broadcast(
        room,
        {
            "type": "share_moment",
            "id": msg_id,
            "senderClientId": client_id,
            "senderName": member.name,
            "text": text,
            "timestamp": now_ms,
            "videoId": video_id,
            "title": title,
            "thumbnail": thumbnail,
            "positionMs": position_ms,
            "note": note,
        },
    )


async def handle_song_reaction(client_id: str, msg: dict):
    """Float an emoji over the album art for everyone in the room.

    Pure ephemeral effect — no chat record, no history. Clients
    animate the emoji rising from the bottom of the now-playing card
    when they receive `song_reaction`. Rate limit is a soft 1 per
    500ms per client (enforced loosely; a tight loop would just stack
    in the receive buffer and we drop them).
    """
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    member = room.members.get(client_id)
    if not member:
        return
    emoji = (msg.get("emoji", "") or "").strip()
    if not emoji or len(emoji) > 8:
        return
    await room_manager.broadcast(
        room,
        {
            "type": "song_reaction",
            "senderClientId": client_id,
            "senderName": member.name,
            "emoji": emoji,
            "timestamp": int(time.time() * 1000),
        },
    )


async def handle_song_request(client_id: str, msg: dict):
    """Guest (or host) requests a song to be added to the queue."""
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    video_id = msg.get("videoId", "")
    if not video_id:
        return

    # Reject duplicates
    for q in room.queue:
        if q.video_id == video_id and not q.is_suggestion:
            return  # Already requested

    member = room.members.get(client_id)
    if not member:
        return

    from room_manager import QueueItem

    item = QueueItem(
        video_id=video_id,
        title=msg.get("title", "Unknown"),
        duration=msg.get("duration", 0),
        thumbnail=msg.get("thumbnail", ""),
        uploader=msg.get("uploader", ""),
        requested_by=client_id,
        requested_by_name=member.name,
        votes={client_id},  # auto-upvote
        timestamp=time.time(),
        is_suggestion=False,
    )
    room.queue.append(item)
    print(f"[WS] Room {room.code}: {member.name} requested '{item.title}'")
    analytics.log_event(
        "song_request",
        client_id=client_id,
        client_name=member.name,
        room_code=room.code,
        video_id=video_id,
        title=item.title,
    )

    # Broadcast updated queue (personalized)
    await room_manager.broadcast_queue(room)

    # Broadcast chat message about the suggestion (includes song metadata for card UI)
    from room_manager import ChatMessageRecord, MAX_CHAT_HISTORY

    msg_id = uuid.uuid4().hex
    now_ms = int(time.time() * 1000)
    sug_text = f'suggested "{item.title}"'
    record = ChatMessageRecord(
        id=msg_id,
        sender_id=client_id,
        sender_name=member.name,
        text=sug_text,
        timestamp=now_ms,
        is_suggestion=True,
        suggestion_video_id=video_id,
        suggestion_title=item.title,
        suggestion_thumbnail=item.thumbnail,
        suggestion_uploader=item.uploader,
    )
    room.messages.append(record)
    if len(room.messages) > MAX_CHAT_HISTORY:
        del room.messages[: len(room.messages) - MAX_CHAT_HISTORY]

    await room_manager.broadcast(
        room,
        {
            "type": "chat_message",
            "id": msg_id,
            "senderClientId": client_id,
            "senderName": member.name,
            "text": sug_text,
            "timestamp": now_ms,
            "isSuggestion": True,
            "suggestionVideoId": video_id,
            "suggestionTitle": item.title,
            "suggestionThumbnail": item.thumbnail,
            "suggestionUploader": item.uploader,
        },
    )


async def handle_vote_song(client_id: str, msg: dict):
    """Toggle vote on a queue item."""
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    video_id = msg.get("videoId", "")
    if not video_id:
        return

    for q in room.queue:
        if q.video_id == video_id and not q.is_suggestion:
            if client_id in q.votes:
                q.votes.discard(client_id)
            else:
                q.votes.add(client_id)
            break
    else:
        return  # Not found

    await room_manager.broadcast_queue(room)


async def handle_remove_request(client_id: str, msg: dict):
    """Remove a song request (requester or host only)."""
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return

    video_id = msg.get("videoId", "")
    if not video_id:
        return

    for q in room.queue:
        if q.video_id == video_id and not q.is_suggestion:
            # Only allow if requester or host
            if q.requested_by == client_id or room.host_id == client_id:
                room.queue.remove(q)
                await room_manager.broadcast_queue(room)
            break


async def handle_kick_member(client_id: str, msg: dict):
    target_id = msg.get("targetClientId", "")
    if not target_id:
        return
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    target_ws, success = room_manager.kick_member(room.code, client_id, target_id)
    if not success:
        return
    # A kick during the grace window used to leave the victim's pending entry,
    # deferred-leave task and wake retries running — so the server would page
    # the kicked device back and it would silently rejoin under a new id.
    # Removal must forget them completely.
    cancel_room_session(target_id)
    print(f"[WS] Host {client_id[:8]} kicked {target_id[:8]} from room {room.code}")
    # Notify kicked client and close their connection
    try:
        await ws_send(
            target_ws, {"type": "kicked", "message": "You were removed from the room"}
        )
        await target_ws.close()
    except Exception:
        pass
    # Broadcast updated member list to remaining members
    await room_manager.broadcast(
        room,
        {
            "type": "member_left",
            "count": len(room.members),
            "members": room_manager.get_member_list(room),
        },
    )


async def handle_share_lyrics(client_id: str, msg: dict):
    """Host shares fetched lyrics with all room members"""
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    video_id = msg.get("videoId", "")
    if not video_id:
        return
    # Broadcast lyrics to all members (including host for consistency)
    await room_manager.broadcast(
        room,
        {
            "type": "sync_lyrics",
            "videoId": video_id,
            "success": msg.get("success", False),
            "hasTimestamps": msg.get("hasTimestamps", False),
            "lines": msg.get("lines", []),
            "plainLyrics": msg.get("plainLyrics"),
            "source": msg.get("source"),
        },
    )


async def handle_leave(client_id: str):
    """Explicit leave (user clicked Leave button). Destroys room immediately if host."""
    # The user chose to go: forget this DEVICE's room session entirely before
    # anything else, so no deferred "left" broadcast and no wake push can fire
    # for it afterwards — including for older client_ids the device used
    # before a reconnect. This is what guarantees we never page someone back
    # into a room they deliberately left.
    cancel_room_session(client_id)
    # Clean up queue before leaving (remove their requests/votes)
    room = room_manager.get_room_for_client(client_id)
    room_snapshot = None
    if room:
        room.remove_member_from_queue(client_id)
        room_snapshot = (
            room.code,
            room.host_name,
            room.created_at,
            room.peak_members,
            room.songs_played,
            room.password is not None,
        )

    code, was_host, remaining_ws = room_manager.leave_room(client_id)
    if not code:
        return

    print(f"[WS] {client_id[:8]} left room {code} (was_host={was_host})")
    analytics.log_event(
        "room_leave",
        client_id=client_id,
        room_code=code,
        detail=json.dumps({"was_host": was_host}),
    )

    if was_host:
        if room_snapshot:
            await analytics.log_room_destroyed(*room_snapshot)
        # Room was destroyed — notify remaining members directly
        for ws in remaining_ws:
            await ws_send(ws, {"type": "room_closed"})
    else:
        room = room_manager.rooms.get(code)
        if room:
            # Rebroadcast queue since member's votes/requests were removed
            await room_manager.broadcast_queue(room)
            await room_manager.broadcast(
                room,
                {
                    "type": "member_left",
                    "count": len(room.members),
                    "members": room_manager.get_member_list(room),
                },
            )


# Deferred-leave grace window (Phase 1 of the room-survival work). A brief
# WebSocket blip + fast reconnect should be INVISIBLE to the room — no "X left"
# then "X joined" flap. On an unexpected disconnect we HOLD the member_left
# broadcast this long; handle_rejoin_room cancels it on a within-grace rejoin
# and stays silent. Only if the member is still gone at expiry do we announce
# the leave. Kills the flapping that a 2-second network hiccup used to cause.
MEMBER_GRACE_SECONDS = 45
_disconnect_grace_tasks: dict[str, "asyncio.Task"] = {}   # disconnected client_id -> pending leave task


async def handle_disconnect(client_id: str):
    """Unexpected disconnect (WebSocket dropped). Defers the 'left' broadcast a
    grace period so a fast reconnect doesn't flap the room for everyone else."""
    # Clean up queue (remove their requests/votes)
    room = room_manager.get_room_for_client(client_id)
    if room:
        room.remove_member_from_queue(client_id)

    code, was_host, member_kept = room_manager.disconnect_member(client_id)
    if not code:
        return

    print(f"[WS] {client_id[:8]} disconnected from room {code} (was_host={was_host}) — grace {MEMBER_GRACE_SECONDS}s")
    analytics.log_event(
        "room_leave",
        client_id=client_id,
        room_code=code,
        detail=json.dumps({"was_host": was_host, "disconnect": True}),
    )

    room = room_manager.rooms.get(code)
    if room and not was_host:
        # Their votes/requests were removed, so the queue genuinely changed —
        # rebroadcast it. But do NOT announce the member leaving yet: the member
        # is kept in the roster marked `reconnecting`, and the member_left
        # broadcast is deferred (cancelled if they rejoin within grace).
        await room_manager.broadcast_queue(room)
        prev = _disconnect_grace_tasks.pop(client_id, None)
        if prev:
            prev.cancel()
        _disconnect_grace_tasks[client_id] = asyncio.create_task(
            member_left_after_grace(client_id, code, MEMBER_GRACE_SECONDS)
        )

    # Phase 2: try to wake the client that just dropped. Gated on member_kept,
    # which is only true for an UNEXPECTED disconnect that the grace window is
    # holding open — an explicit leave or a kick already removed the member
    # (and its _client_to_room entry) before the socket closed, so
    # disconnect_member returned early and we never get here for them.
    #
    # Applies to the host too: a host whose process dies takes the whole room
    # down when its own grace expires, so it is the most valuable wake of all.
    if member_kept and room:
        cancel_wake(client_id)
        task = asyncio.create_task(wake_after_disconnect(client_id, code))
        _wake_tasks[client_id] = task

        def _wake_done(t: "asyncio.Task", cid=client_id):
            _wake_tasks.pop(cid, None)
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                print(f"[FCM] wake task for {cid[:8]} failed: {type(exc).__name__}: {exc}")

        task.add_done_callback(_wake_done)

    if was_host and room:
        # Host gone: existing behavior destroys the room after its own grace and
        # sends room_closed, which supersedes the deferred member_left (which
        # then no-ops because the room is gone).
        asyncio.create_task(destroy_room_after_grace(client_id, code, 30))


async def member_left_after_grace(client_id: str, code: str, delay: int):
    """Announce a member's departure ONLY if they haven't rejoined within the
    grace window. Cancelled by handle_rejoin_room on a fast reconnect."""
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return  # rejoined in time — stay silent, no flap
    _disconnect_grace_tasks.pop(client_id, None)
    # Actually remove them now (no-op if they already rejoined / room is gone).
    fcode, _remaining_ws = room_manager.finalize_disconnect(client_id)
    if fcode is None:
        return  # rejoined during grace, or room already destroyed — nothing to announce
    room = room_manager.rooms.get(fcode)
    if room is None:
        return  # they were the last one — room was cleaned up by finalize
    print(f"[WS] {client_id[:8]} left room {fcode} (grace expired)")
    await room_manager.broadcast(
        room,
        {
            "type": "member_left",
            "count": len(room.members),
            "members": room_manager.get_member_list(room),
        },
    )


async def destroy_room_after_grace(client_id: str, code: str, delay: int = 30):
    """Wait for host to reconnect, then destroy room if they didn't."""
    await asyncio.sleep(delay)
    room_manager.cleanup_stale_disconnects()
    room = room_manager.rooms.get(code)
    if room is None:
        return
    # Check if room has an ACTIVE host — present in the roster AND not still in
    # the disconnect grace window (a reconnecting host is NOT active).
    host_member = room.members.get(room.host_id) if room.host_id is not None else None
    host_active = host_member is not None and not host_member.reconnecting
    if not host_active:
        # Host didn't rejoin — destroy room and notify remaining guests
        await analytics.log_room_destroyed(
            code,
            room.host_name,
            room.created_at,
            room.peak_members,
            room.songs_played,
            room.password is not None,
        )
        remaining_ws = [m.websocket for m in room.members.values()]
        for mid in list(room.members.keys()):
            room_manager._client_to_room.pop(mid, None)
        del room_manager.rooms[code]
        for ws in remaining_ws:
            await ws_send(ws, {"type": "room_closed"})
        print(f"[WS] Room {code} destroyed after host grace period expired")


# ── Remote control ────────────────────────────────────────────────────
#
# A browser driving a phone. Kept out of RoomManager on purpose: a client_id
# maps to exactly one room, so a control-session-as-room could not coexist
# with the Listen Together room it is supposed to be able to drive.


async def handle_control_create(client_id: str, websocket: WebSocket, msg: dict):
    """Phone offers itself for remote control and gets a pairing code."""
    sess, secret = control_manager.create(client_id, websocket)
    code = control_manager.mint_code(sess.id)
    await ws_send(websocket, {
        "type": "control_ready",
        "sessionId": sess.id,
        # Persist this: client_id changes on every reconnect, so it is the
        # only way back to the same session after a network blip.
        "phoneSecret": secret,
        "code": code,
        "expiresIn": int(control_session.TICKET_TTL),
    })


async def handle_control_rejoin(client_id: str, websocket: WebSocket, msg: dict):
    """Phone re-attaches to its session after a reconnect."""
    sess = control_manager.rejoin(client_id, websocket, msg.get("phoneSecret", ""))
    if sess is None:
        await ws_send(websocket, {"type": "control_closed", "reason": "unknown_session"})
        return
    await ws_send(websocket, {"type": "control_ready", "sessionId": sess.id,
                              "resumed": True})
    # Controllers were watching a socket that just changed underneath them.
    for ws in list(sess.controllers.values()):
        await ws_send(ws, {"type": "control_phone_back"})


async def handle_control_state(client_id: str, msg: dict):
    """Phone pushes what it is playing; fan out to its controllers."""
    sess = control_manager.for_client(client_id)
    if sess is None or sess.phone_client_id != client_id:
        return
    state = msg.get("state") or {}
    control_manager.set_state(sess, state)
    for ws in list(sess.controllers.values()):
        await ws_send(ws, {"type": "control_state", "state": state})


async def handle_control_join(client_id: str, websocket: WebSocket, msg: dict):
    """Browser redeems a pairing code and starts mirroring the phone."""
    sess = control_manager.redeem(msg.get("code", ""))
    if sess is None:
        await ws_send(websocket, {"type": "control_error", "code": "bad_code"})
        return
    control_manager.attach_controller(sess.id, client_id, websocket)
    await ws_send(websocket, {
        "type": "control_joined",
        "sessionId": sess.id,
        "state": sess.last_state or {},
    })
    await ws_send(sess.phone_ws, {"type": "control_controller_joined"})


async def handle_control_cmd(client_id: str, websocket: WebSocket, msg: dict):
    """Browser command -> phone. Fire and forget.

    Never await a reply from the phone here: only this receive loop can read
    the phone's messages, so waiting on one inside it deadlocks the socket.
    Same reason handle_next is dispatched via create_task.
    """
    sess = control_manager.for_client(client_id)
    if sess is None or client_id not in sess.controllers:
        await ws_send(websocket, {"type": "control_error", "code": "not_paired"})
        return
    if not control_manager.allow_cmd(client_id):
        await ws_send(websocket, {"type": "control_error", "code": "rate_limited"})
        return
    await ws_send(sess.phone_ws, {
        "type": "control_cmd",
        "action": msg.get("action", ""),
        "value": msg.get("value"),
    })


async def handle_control_disconnect(client_id: str):
    """Either side dropped. A phone drop ends the session for everyone."""
    ended, orphans = control_manager.end_for_client(client_id)
    if ended is not None:
        for ws in orphans:
            await ws_send(ws, {"type": "control_closed", "reason": "phone_gone"})


async def handle_rejoin_room(client_id: str, websocket: WebSocket, msg: dict):
    """Handle a client rejoining a room after disconnect."""
    code = msg.get("code", "").upper()
    name = msg.get("name", "Unknown")
    previous_client_id = msg.get("previousClientId")
    # Server-minted proof of who is rejoining. Absent on installs that
    # predate it, which is why the legacy previousClientId path survives.
    member_secret = msg.get("memberSecret")

    if not code:
        await ws_send(websocket, {"type": "error", "message": "Room code required"})
        return

    room, was_host = room_manager.rejoin_room(
        client_id, websocket, code, name,
        previous_client_id=previous_client_id,
        member_secret=member_secret,
    )
    if not room:
        await ws_send(
            websocket,
            {
                "type": "error",
                "message": "Room no longer exists",
                "rejoinFailed": True,
                "code": code,
            },
        )
        return

    role = "host" if was_host else "guest"
    print(f"[WS] {client_id[:8]} ({name}) rejoined room {code} as {role}")

    # Send current room state to the rejoining client (with personalized queue)
    state = {
        "code": room.code,
        "hostName": room.host_name,
        "memberCount": len(room.members),
        "currentSong": room.current_song,
        "position": room_manager.get_estimated_position(room),
        "isPlaying": room.is_playing,
        "queue": room.serialize_queue_for_client(client_id),
        "members": room_manager.get_member_list(room),
        "role": role,
    }
    # Defensive: a KeyError here would break joining outright, whereas an
    # absent secret merely falls back to the legacy rejoin path.
    _m = room.members.get(client_id)
    if _m:
        state["memberSecret"] = _m.secret
    await ws_send(websocket, {"type": "room_joined", "state": state})

    # If this rejoin landed within the disconnect grace window, cancel the
    # pending "left" broadcast and stay SILENT — the room never saw them leave,
    # so it must not see them "join" either. That silent pair IS the flap we're
    # removing. Otherwise (grace expired → they were announced as left, or a
    # genuinely fresh join) announce the join normally.
    # They're back — stop paging the old identity immediately rather than
    # waiting for the next retry tick to notice.
    if previous_client_id:
        cancel_wake(previous_client_id)
    cancel_wake(client_id)
    grace_task = _disconnect_grace_tasks.pop(previous_client_id, None) if previous_client_id else None
    if grace_task is not None:
        grace_task.cancel()
        print(f"[WS] {client_id[:8]} rejoined room {code} within grace — silent (no flap)")
    else:
        await room_manager.broadcast(
            room,
            {
                "type": "member_joined",
                "count": len(room.members),
                "members": room_manager.get_member_list(room),
            },
            exclude_id=client_id,
        )


@api.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())

    print(f"[WS] Client connected: {client_id[:8]}")
    analytics.log_event("ws_connect", client_id=client_id)
    await ws_send(websocket, {"type": "connected", "clientId": client_id})

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            if msg_type == "ping":
                await ws_send(
                    websocket,
                    {
                        "type": "pong",
                        "clientTime": msg.get("clientTime", 0),
                        "serverTime": int(time.time() * 1000),
                    },
                )

            elif msg_type == "create_room":
                await handle_create_room(client_id, websocket, msg)

            elif msg_type == "join_room":
                await handle_join_room(client_id, websocket, msg)

            elif msg_type == "play":
                await handle_play(client_id, msg)

            elif msg_type == "pause":
                await handle_pause(client_id, msg)

            elif msg_type == "resume":
                await handle_resume(client_id, msg)

            elif msg_type == "seek":
                await handle_seek(client_id, msg)

            elif msg_type == "next":
                # Run as a background task so the receive loop keeps
                # processing incoming messages — handle_next awaits the
                # host's `extract_response` via a future, but that
                # response can only be read by THIS loop. Awaiting
                # handle_next directly here would deadlock: the response
                # would sit in the socket buffer until handle_next
                # gave up (8s timeout + yt-dlp retries = minutes), and
                # the future would be cancelled by then so the response
                # would be silently dropped on dispatch.
                asyncio.create_task(handle_next(client_id))

            elif msg_type == "extract_response":
                # Host's reply to a server-initiated extract_request.
                # Resolves the pending future inside handle_next.
                await handle_extract_response(client_id, msg)

            elif msg_type == "queue_update":
                await handle_queue_update(client_id, msg)

            elif msg_type == "position_report":
                await handle_position_report(client_id, msg)

            elif msg_type == "chat_message":
                await handle_chat_message(client_id, msg)

            elif msg_type == "typing":
                await handle_typing(client_id, msg)

            elif msg_type == "add_reaction":
                await handle_add_reaction(client_id, msg)

            elif msg_type == "remove_reaction":
                await handle_remove_reaction(client_id, msg)

            elif msg_type == "edit_message":
                await handle_edit_message(client_id, msg)

            elif msg_type == "delete_message":
                await handle_delete_message(client_id, msg)

            elif msg_type == "share_moment":
                await handle_share_moment(client_id, msg)

            elif msg_type == "song_reaction":
                await handle_song_reaction(client_id, msg)

            elif msg_type == "song_request":
                await handle_song_request(client_id, msg)

            elif msg_type == "vote_song":
                await handle_vote_song(client_id, msg)

            elif msg_type == "remove_request":
                await handle_remove_request(client_id, msg)

            elif msg_type == "kick_member":
                await handle_kick_member(client_id, msg)

            elif msg_type == "control_create":
                await handle_control_create(client_id, websocket, msg)

            elif msg_type == "control_rejoin":
                await handle_control_rejoin(client_id, websocket, msg)

            elif msg_type == "control_state":
                await handle_control_state(client_id, msg)

            elif msg_type == "control_join":
                await handle_control_join(client_id, websocket, msg)

            elif msg_type == "control_cmd":
                await handle_control_cmd(client_id, websocket, msg)

            elif msg_type == "control_end":
                await handle_control_disconnect(client_id)

            elif msg_type == "rejoin_room":
                await handle_rejoin_room(client_id, websocket, msg)

            elif msg_type == "share_lyrics":
                await handle_share_lyrics(client_id, msg)

            elif msg_type == "register_fcm_token":
                token = msg.get("token", "")
                if token:
                    register_fcm_token(client_id, token)

            elif msg_type == "leave":
                await handle_leave(client_id)

            # ── Social v1 (lounge / online / DMs) ──
            elif msg_type == "social_subscribe":
                await social.handle_social_subscribe(client_id, websocket, msg)

            elif msg_type == "presence_ping":
                await social.handle_presence_ping(client_id, websocket, msg)

            elif msg_type == "social_fcm_register":
                await social.handle_social_fcm_register(client_id, websocket, msg)

            elif msg_type == "lounge_send":
                await social.handle_lounge_send(client_id, websocket, msg)

            elif msg_type == "dm_send":
                await social.handle_dm_send(client_id, websocket, msg)

            elif msg_type == "dm_accept":
                await social.handle_dm_accept(client_id, websocket, msg)

            elif msg_type == "dm_decline":
                await social.handle_dm_decline(client_id, websocket, msg)

            elif msg_type == "dm_read":
                await social.handle_dm_read(client_id, websocket, msg)

            elif msg_type == "dm_set_retention":
                await social.handle_dm_set_retention(client_id, websocket, msg)

            elif msg_type == "dm_clear_on_leave":
                await social.handle_dm_clear_on_leave(client_id, websocket, msg)

            elif msg_type == "dm_open":
                await social.handle_dm_open(client_id, websocket, msg)

            # ── DM chat-parity messages ──
            elif msg_type == "dm_chat_react":
                await social.handle_dm_chat_react(client_id, websocket, msg)

            elif msg_type == "dm_chat_unreact":
                await social.handle_dm_chat_unreact(client_id, websocket, msg)

            elif msg_type == "dm_chat_edit":
                await social.handle_dm_chat_edit(client_id, websocket, msg)

            elif msg_type == "dm_chat_delete":
                await social.handle_dm_chat_delete(client_id, websocket, msg)

            elif msg_type == "dm_chat_typing":
                await social.handle_dm_chat_typing(client_id, websocket, msg)

            # ── Lounge chat-parity messages ──
            elif msg_type == "lounge_react":
                await social.handle_lounge_react(client_id, websocket, msg)

            elif msg_type == "lounge_unreact":
                await social.handle_lounge_unreact(client_id, websocket, msg)

            elif msg_type == "dm_unfriend":
                await social.handle_dm_unfriend(client_id, websocket, msg)

            elif msg_type == "dm_set_mute":
                await social.handle_dm_set_mute(client_id, websocket, msg)

            elif msg_type == "dm_clear_chat":
                await social.handle_dm_clear_chat(client_id, websocket, msg)

            elif msg_type == "dm_chat_view_once_open":
                await social.handle_dm_chat_view_once_open(client_id, websocket, msg)

            else:
                # Unknown message type — reply with social_error so a
                # newer client talking to an older server can detect
                # the missing feature and roll back its optimistic UI
                # state instead of letting it persist forever locally.
                # Existing behaviour was silent drop; this is an
                # additive change (new reply type, harmless to older
                # clients which just don't handle it).
                # `lounge_` included so any future lounge-namespaced
                # message added on the client (R3+) gets a real error
                # reply on an older server instead of silent drop.
                # `control_` (remote control) included for the same reason,
                # and it matters more here: the phone BLOCKS waiting for a
                # pairing reply. Without this an app talking to an older
                # server would sit on a spinner forever with no signal.
                if (
                    msg_type.startswith("dm_")
                    or msg_type.startswith("user_")
                    or msg_type.startswith("social_")
                    or msg_type.startswith("lounge_")
                    or msg_type.startswith("control_")
                ):
                    try:
                        await websocket.send_json({
                            "type": "social_error",
                            "code": "unknown_type",
                            "received": msg_type,
                        })
                    except Exception:
                        pass

    except WebSocketDisconnect:
        print(f"[WS] Client disconnected: {client_id[:8]}")
        analytics.log_event("ws_disconnect", client_id=client_id)
        await handle_disconnect(client_id)
        await social.handle_social_disconnect(client_id)
        await handle_control_disconnect(client_id)
    except Exception as e:
        print(f"[WS] Error for {client_id[:8]}: {e}")
        analytics.log_event("ws_disconnect", client_id=client_id)
        await handle_disconnect(client_id)
        await handle_control_disconnect(client_id)
        await social.handle_social_disconnect(client_id)


# ── Song Identification via Lyrics ──
_GK = [
    "gsk_eLH4",
    "z9lt2dCi",
    "YLUBpxmT",
    "WGdyb3FY",
    "iydiOUYB",
    "yqgqnKxf",
    "u74IAWlz",
]
GROQ_API_KEY = "".join(_GK)
SERPER_API_KEY = "862d216d4726" + "76dce339725814" + "05e6878451ad7b"

TEASING_TEMPLATES = [
    "We caught you vibing to {song} by {artist}!",
    "Humming {song}? {artist} would be proud!",
    "Your heart says {song}, we heard it",
    "Caught red-handed singing {song} by {artist}!",
    "Someone's got {song} stuck in their head...",
    "{artist}'s {song} living rent-free in your mind?",
    "We know that tune... {song} by {artist}!",
]


async def transcribe_audio(file_path: str) -> str:
    """Send audio to Groq Whisper for transcription"""
    if not GROQ_API_KEY:
        return ""
    try:
        filename = os.path.basename(file_path)
        content_type = "audio/mpeg" if file_path.endswith(".mp3") else "audio/wav"
        async with httpx.AsyncClient(timeout=30) as client:
            with open(file_path, "rb") as f:
                resp = await client.post(
                    "https://api.groq.com/openai/v1/audio/transcriptions",
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                    files={"file": (filename, f, content_type)},
                    data={"model": "whisper-large-v3", "language": "hi"},
                )
            print(
                f"[Identify] Whisper status: {resp.status_code}, file: {filename}, type: {content_type}, size: {os.path.getsize(file_path)}"
            )
            if resp.status_code == 200:
                text = resp.json().get("text", "").strip()
                print(f"[Identify] Whisper transcription: {text[:100]}")
                return text
            else:
                print(f"[Identify] Whisper error: {resp.status_code} {resp.text[:300]}")
                return ""
    except Exception as e:
        print(f"[Identify] Whisper exception: {type(e).__name__}: {e}")
        return ""


async def transliterate_to_roman(text: str) -> str:
    """Transliterate Hindi/Punjabi Devanagari text to Roman script using Groq LLM"""
    if not GROQ_API_KEY or not text:
        return text
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.1-8b-instant",
                    "messages": [
                        {
                            "role": "system",
                            "content": "Transliterate the following Hindi/Punjabi text to Roman script (like how Indians type in English). Output ONLY the romanized text, nothing else. Keep the original words, just change the script.",
                        },
                        {"role": "user", "content": text},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 200,
                },
            )
            if resp.status_code == 200:
                roman = resp.json()["choices"][0]["message"]["content"].strip()
                print(f"[Identify] Transliterated: {roman}")
                return roman
        return text
    except Exception as e:
        print(f"[Identify] Transliteration failed: {e}")
        return text


async def search_lyrics(query: str) -> list:
    """Search Google via Serper API"""
    if not SERPER_API_KEY:
        print("[Identify] SERPER_API_KEY not configured")
        return []
    try:
        search_query = f"{query} song lyrics"
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                headers={
                    "X-API-KEY": SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={"q": search_query, "num": 5},
            )
            if resp.status_code == 200:
                data = resp.json()
                results = [
                    {"title": r.get("title", ""), "url": r.get("link", "")}
                    for r in data.get("organic", [])
                ]
                print(f"[Identify] Serper returned {len(results)} results")
                return results
            else:
                print(f"[Identify] Serper error: {resp.status_code}")
                return []
    except Exception as e:
        print(f"[Identify] Serper exception: {e}")
        return []


def parse_song_from_titles(results: list) -> dict:
    """Extract song name and artist from search result titles"""
    import random

    for r in results:
        title = r.get("title", "")

        # Pattern: "SONG LYRICS – Artist" or "Song Lyrics - Artist"
        match = re.match(
            r"^(.+?)\s+LYRICS?\s*[–\-|:]\s*(.+?)(?:\s*\|.*)?$", title, re.IGNORECASE
        )
        if match:
            song = match.group(1).strip().title()
            artist = match.group(2).strip()
            # Clean up common suffixes
            for suffix in [" Lyrics", " Official", " Video", " Audio", " HD"]:
                artist = artist.replace(suffix, "").strip()
            return {"song": song, "artist": artist, "confidence": 90}

        # Pattern: "Song by Artist"
        match = re.match(r"^(.+?)\s+by\s+(.+?)\s*[–\-|]", title, re.IGNORECASE)
        if match:
            song = match.group(1).strip().title()
            artist = match.group(2).strip()
            return {"song": song, "artist": artist, "confidence": 80}

        # Pattern: "Song - Artist | Site"
        match = re.match(r"^(.+?)\s*[–\-]\s*(.+?)(?:\s*\|.*)?$", title)
        if match:
            part1 = match.group(1).strip()
            part2 = match.group(2).strip()
            # Skip if part2 looks like a website name
            if not any(
                w in part2.lower()
                for w in [
                    "lyrics",
                    "genius",
                    "azlyrics",
                    "musixmatch",
                    "lyricshub",
                    "shazam",
                ]
            ):
                song = part1.title()
                artist = part2
                return {"song": song, "artist": artist, "confidence": 70}

    return None


async def generate_teasing_line(song: str, artist: str, lyrics: str = "") -> str:
    """Generate a Hinglish teasing line based on lyrics meaning — like a friend roasting you"""
    if not GROQ_API_KEY:
        import random

        return random.choice(TEASING_TEMPLATES).format(song=song, artist=artist)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [
                        {
                            "role": "system",
                            "content": """You caught your close friend singing/humming a song. Write a warm, playful Hinglish teasing line based on what the lyrics mean. Like a bestfriend smiling and saying something sweet but cheeky.

IMPORTANT RULES:
- NEVER be offensive, rude, or hurtful
- NEVER mention family members (baap, maa, behen etc)
- Keep it light and affectionate — like teasing with love
- Use simple Hinglish that any young Indian would say
- Max 12-15 words, one sentence only
- NO emoji, NO quotes, NO hashtags, NO exclamation marks
- Sound like a real person, not AI
- DO NOT repeat or paraphrase the lyrics back
- React to the EMOTION/SITUATION the lyrics describe

Write ONLY the teasing line, nothing else.""",
                        },
                        {
                            "role": "user",
                            "content": f"Song: {song} by {artist}\nLyrics they were singing: {lyrics}"
                            if lyrics
                            else f"Song: {song} by {artist}",
                        },
                    ],
                    "temperature": 0.85,
                    "max_tokens": 50,
                },
            )
            if resp.status_code == 200:
                line = (
                    resp.json()["choices"][0]["message"]["content"]
                    .strip()
                    .strip('"')
                    .strip("'")
                )
                # Clean up any unwanted prefixes
                for prefix in ["Here's", "Teasing:", "Line:", "Response:"]:
                    if line.startswith(prefix):
                        line = line[len(prefix) :].strip()
                if 3 <= len(line.split()) <= 20:
                    return line
        import random

        return random.choice(TEASING_TEMPLATES).format(song=song, artist=artist)
    except:
        import random

        return random.choice(TEASING_TEMPLATES).format(song=song, artist=artist)


@api.post("/identify")
async def identify_song(file: UploadFile = File(...)):
    """Identify a song from an audio clip (WAV or MP3)"""
    print(f"[Identify] Received file: {file.filename}, size: {file.size}")

    # Save to temp file with original extension
    ext = os.path.splitext(file.filename or "audio.wav")[1] or ".wav"
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        # Step 1: Transcribe with Whisper (returns Hindi/Devanagari)
        lyrics_text = await transcribe_audio(tmp_path)
        if not lyrics_text or len(lyrics_text.strip()) < 5:
            print("[Identify] No meaningful transcription")
            return JSONResponse({"identified": False, "reason": "no_transcription"})

        # Step 2: Transliterate to Roman script (lyrics sites index in Roman)
        roman_text = await transliterate_to_roman(lyrics_text)

        # Step 3: Search Google for the lyrics
        results = await search_lyrics(roman_text)
        if not results:
            print("[Identify] No search results")
            return JSONResponse(
                {
                    "identified": False,
                    "reason": "no_search_results",
                    "transcription": lyrics_text,
                }
            )

        # Step 4: Parse song + artist from titles
        parsed = parse_song_from_titles(results)
        if not parsed:
            print("[Identify] Could not parse song from results")
            return JSONResponse(
                {
                    "identified": False,
                    "reason": "parse_failed",
                    "transcription": lyrics_text,
                }
            )

        # Step 5: Generate teasing line based on lyrics meaning
        teasing = await generate_teasing_line(
            parsed["song"], parsed["artist"], lyrics_text
        )

        print(f"[Identify] Identified: {parsed['song']} by {parsed['artist']}")
        return JSONResponse(
            {
                "identified": True,
                "song": parsed["song"],
                "artist": parsed["artist"],
                "teasingLine": teasing,
                "confidence": parsed["confidence"],
                "transcription": lyrics_text,
            }
        )
    finally:
        # Clean up temp file
        try:
            os.unlink(tmp_path)
        except:
            pass


# ==================== AUTH / USER SYNC ====================
#
# Phase 1 of the cloud-sync rollout. The Android client signs in with
# Google via Firebase Auth, then calls POST /auth/sync once per cold start
# (and after profile changes) to upsert the `users` row keyed by Firebase
# UID. Later phases (favorites, playlists, history) all key off this row.


class AuthSyncResponse(BaseModel):
    uid: str
    email: Optional[str] = None
    display_name: Optional[str] = None
    photo_url: Optional[str] = None
    created: bool  # True the first time we ever saw this user


@api.post("/auth/sync", response_model=AuthSyncResponse)
async def auth_sync(
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """Upsert the signed-in user's row and bump last_seen.

    Token verification happens in the dependency. By the time we get here
    `user` is trusted — we just write profile fields and timestamps.
    """
    now = models.utc_now()
    result = await session.execute(
        select(models.User).where(models.User.id == user["uid"])
    )
    row = result.scalar_one_or_none()

    created = False
    if row is None:
        row = models.User(
            id=user["uid"],
            email=user.get("email"),
            display_name=user.get("name"),
            # First-ever sign-in: take Google's photo as the starting
            # avatar. custom_photo stays false, so a future Google
            # photo change will still flow through; PATCH /users/me
            # flips custom_photo=true once the user uploads their own.
            photo_url=user.get("picture"),
            created_at=now,
            last_seen=now,
        )
        session.add(row)
        created = True
    else:
        # Keep our cached profile in sync with what Firebase reports —
        # the user may have updated their Google name / photo since
        # last sign-in.
        row.email = user.get("email")
        row.display_name = user.get("name")
        # COALESCE-style update: only refresh photo_url from Google
        # if the user hasn't uploaded their own. Without this guard
        # every cold start would clobber the custom avatar with the
        # Google profile photo, undoing the entire R1 feature.
        if not row.custom_photo:
            row.photo_url = user.get("picture")
        row.last_seen = now

    await session.commit()

    return AuthSyncResponse(
        uid=row.id,
        email=row.email,
        display_name=row.display_name,
        photo_url=row.photo_url,
        created=created,
    )


# ── User profile edits (avatar + status text) ──────────────────────
#
# Single PATCH endpoint that the R1 (avatar) and R2 (text status)
# features both hit. Either field omitted → no-op for that field;
# both omitted → 400. Each accepted change is broadcast to friends
# via the social WebSocket so all peers see the update in real time.

class AvatarSignResponse(BaseModel):
    cloud_name: str
    api_key: str
    timestamp: int
    public_id: str
    signature: str
    overwrite: bool = True


@api.post("/avatars/sign", response_model=AvatarSignResponse)
async def avatars_sign(
    user: AuthedUser = Depends(get_current_user),
):
    """Return signed Cloudinary upload params for the caller's avatar.

    Server pins `public_id = avatars/{caller_uid}` so the signature
    is keyed to this uid — a malicious client cannot reuse the
    signature to overwrite another user's avatar slot because
    Cloudinary verifies that the params at upload time match the
    signature.

    Client takes the returned params + the image bytes and POSTs
    multipart to `https://api.cloudinary.com/v1_1/{cloud_name}/image/upload`.
    On success Cloudinary returns a `secure_url` of the form
    `https://res.cloudinary.com/{cloud_name}/image/upload/v{N}/avatars/{uid}.jpg`
    which the client then PATCHes to /users/me.
    """
    cloud_name = os.environ.get("CLOUDINARY_CLOUD_NAME", "")
    api_key = os.environ.get("CLOUDINARY_API_KEY", "")
    api_secret = os.environ.get("CLOUDINARY_API_SECRET", "")
    if not cloud_name or not api_key or not api_secret:
        raise HTTPException(
            status_code=503,
            detail="Cloudinary not configured on this server",
        )

    import cloudinary.utils
    public_id = f"avatars/{user['uid']}"
    timestamp = int(time.time())
    params_to_sign = {
        "public_id": public_id,
        "timestamp": timestamp,
        "overwrite": True,
    }
    signature = cloudinary.utils.api_sign_request(params_to_sign, api_secret)

    return AvatarSignResponse(
        cloud_name=cloud_name,
        api_key=api_key,
        timestamp=timestamp,
        public_id=public_id,
        signature=signature,
        overwrite=True,
    )


class DmPhotoSignBody(BaseModel):
    peer_uid: str


class DmPhotoSignResponse(BaseModel):
    cloud_name: str
    api_key: str
    timestamp: int
    public_id: str
    signature: str
    context: str


@api.post("/dm-photos/sign", response_model=DmPhotoSignResponse)
async def dm_photos_sign(
    body: DmPhotoSignBody,
    user: AuthedUser = Depends(get_current_user),
):
    """R4: signed Cloudinary upload params for a one-time DM photo.

    Server pins `public_id = dm_photos/{uuid}` (fresh per upload, no
    reuse possible) AND binds the asset's Cloudinary `context` to
    `from_uid={caller}|to_uid={peer}`. Server-side at `dm_send` we
    re-fetch the asset and verify the context matches the sender +
    recipient — without this, a stolen signature could be replayed
    to push attacker-controlled content into the DM bubble (audit
    fix #10).
    """
    cloud_name = os.environ.get("CLOUDINARY_CLOUD_NAME", "")
    api_key = os.environ.get("CLOUDINARY_API_KEY", "")
    api_secret = os.environ.get("CLOUDINARY_API_SECRET", "")
    if not cloud_name or not api_key or not api_secret:
        raise HTTPException(
            status_code=503,
            detail="Cloudinary not configured on this server",
        )
    peer_uid = (body.peer_uid or "").strip()
    if not peer_uid or peer_uid == user["uid"]:
        raise HTTPException(
            status_code=400,
            detail="peer_uid required and must differ from caller",
        )

    import cloudinary.utils, uuid as _uuid
    public_id = f"dm_photos/{_uuid.uuid4().hex}"
    timestamp = int(time.time())
    # Pipe-separated key=value pairs is Cloudinary's standard
    # serialization for the `context` upload param.
    context = f"from_uid={user['uid']}|to_uid={peer_uid}"
    params_to_sign = {
        "public_id": public_id,
        "timestamp": timestamp,
        "context": context,
    }
    signature = cloudinary.utils.api_sign_request(params_to_sign, api_secret)

    return DmPhotoSignResponse(
        cloud_name=cloud_name,
        api_key=api_key,
        timestamp=timestamp,
        public_id=public_id,
        signature=signature,
        context=context,
    )


@api.post("/dms/{message_id}/view-once-open")
async def dms_view_once_open(
    message_id: int,
    user: AuthedUser = Depends(get_current_user),
):
    """R4 REST fallback for `dm_chat_view_once_open`. Used when the
    recipient's WS is briefly disconnected mid-view. Returns
    `{client_nonce}` on success so the caller can match optimistic
    state; 404 not_found, 403 not_recipient, 409 already_opened.
    """
    result = await social.rest_dm_view_once_open(message_id, user["uid"])
    status = result.get("status")
    if status == "ok":
        return {"client_nonce": result.get("client_nonce", "")}
    if status == "not_found":
        raise HTTPException(status_code=404, detail="Message not found")
    if status == "not_recipient":
        raise HTTPException(status_code=403, detail="Only the recipient can open")
    if status == "already_opened":
        raise HTTPException(status_code=409, detail="Already opened")
    raise HTTPException(status_code=500, detail="Internal error")


class PatchSelfBody(BaseModel):
    photo_url: str | None = None
    status_text: str | None = None


class PatchSelfResponse(BaseModel):
    uid: str
    photo_url: str | None
    status_text: str | None
    photo_updated_at: int | None


@api.patch("/users/me", response_model=PatchSelfResponse)
async def patch_self(
    body: PatchSelfBody,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """Update the signed-in user's avatar URL and/or status text.

    Avatar contract: client uploads to Firebase Storage at
    `avatars/{firebase_uid}.jpg`, then PATCHes this endpoint with the
    download URL. We validate the URL points at the user's own path
    (anti-spoofing), flip `custom_photo=true` so /auth/sync stops
    overwriting it, stamp `photo_updated_at` so the social layer can
    cache-bust, and broadcast `user_avatar_changed` to friends.

    Status text contract: max 100 chars. Empty string clears the
    status. R2 broadcasts a `user_status_changed` event; this R1
    endpoint accepts the field for forward-compat but the broadcast
    is gated until the column exists.
    """
    if body.photo_url is None and body.status_text is None:
        raise HTTPException(status_code=400, detail="no fields to update")

    result = await session.execute(
        select(models.User).where(models.User.id == user["uid"])
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="user not found")

    avatar_changed = False
    if body.photo_url is not None:
        # Anti-spoofing — defense in depth on top of Cloudinary's
        # signature verification (the signed-upload params pin
        # public_id server-side, so a malicious client can't write
        # to another user's slot in the first place). Belt-and-braces:
        #   1. Host must be res.cloudinary.com under OUR cloud
        #      (configured via CLOUDINARY_CLOUD_NAME env var).
        #   2. Path must reference the caller's own avatar slot
        #      (`/avatars/{uid}.<ext>`).
        cloud_name = os.environ.get("CLOUDINARY_CLOUD_NAME", "")
        if not cloud_name:
            raise HTTPException(status_code=503, detail="Cloudinary not configured")
        expected_prefix = f"https://res.cloudinary.com/{cloud_name}/"
        # Public ID is `avatars/{uid}`; URL path segment is
        # `/avatars/{uid}.<ext>` (Cloudinary appends the format
        # extension to the delivery URL).
        expected_path_segment = f"/avatars/{user['uid']}."
        if not body.photo_url.startswith(expected_prefix):
            raise HTTPException(
                status_code=400,
                detail="photo_url must be a Cloudinary URL on our project",
            )
        if expected_path_segment not in body.photo_url:
            raise HTTPException(
                status_code=400,
                detail="photo_url must point at your own avatar path",
            )
        row.photo_url = body.photo_url
        row.custom_photo = True
        row.photo_updated_at = int(time.time() * 1000)
        avatar_changed = True

    status_changed = False
    if body.status_text is not None:
        if len(body.status_text) > 100:
            raise HTTPException(status_code=400, detail="status_text too long (max 100 chars)")
        new_status = body.status_text or None
        if row.status_text != new_status:
            row.status_text = new_status
            status_changed = True

    await session.commit()

    if avatar_changed:
        try:
            from social import broadcast_user_avatar_changed
            await broadcast_user_avatar_changed(
                uid=row.id,
                photo_url=row.photo_url,
                photo_updated_at=row.photo_updated_at,
            )
        except Exception as e:
            print(f"[user_avatar_changed] broadcast failed: {e}")

    if status_changed:
        try:
            from social import broadcast_user_status_changed
            await broadcast_user_status_changed(
                uid=row.id,
                status_text=row.status_text,
            )
        except Exception as e:
            print(f"[user_status_changed] broadcast failed: {e}")

    return PatchSelfResponse(
        uid=row.id,
        photo_url=row.photo_url,
        status_text=getattr(row, "status_text", None),
        photo_updated_at=row.photo_updated_at,
    )


# Mount the sync router under the same /api/v1 prefix as the rest of the
# user-facing endpoints. Token-gated routes for favorites / playlists /
# playlist_songs / listen_events all live in sync.py.
api.include_router(sync_router)

# Social v1 — global lounge + online presence + DMs. Routes:
#   GET /api/v1/social/snapshot
#   GET /api/v1/social/friends
# WebSocket dispatch lives inside websocket_endpoint above.
api.include_router(social.router)

# Register the versioned API router
app.include_router(api)


@app.on_event("startup")
async def startup_social_prune() -> None:
    """Start the periodic lounge/DM retention sweeper."""
    asyncio.create_task(social.prune_loop())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
