from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
import yt_dlp
import time
import asyncio
import httpx
import os
import threading
import json
import uuid
import re
from room_manager import RoomManager
from ytmusicapi import YTMusic

app = FastAPI(title="AudioSync API")

# Serve release APKs from /releases directory
os.makedirs(os.path.join(os.path.dirname(__file__), "releases"), exist_ok=True)
app.mount("/releases", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "releases")), name="releases")

# Allow all origins for mobile app access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== PIPED CONFIGURATION ====================
# Self-hosted Piped instance (NewPipe Extractor on server)
# Deploy with: docker run -d -p 8080:8080 1337kavin/piped-backend
# Or use docker-compose up -d for both Piped and API
PIPED_URL = os.getenv("PIPED_URL", "http://localhost:8080")
PIPED_TIMEOUT = int(os.getenv("PIPED_TIMEOUT", "5"))
PIPED_ENABLED = os.getenv("PIPED_ENABLED", "false").lower() == "true"  # Disabled by default until Piped is fixed

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
    "latestVersion": "3.0",
    "latestVersionCode": 13,
    "apkUrl": "https://semidefensive-soledad-unimpeachably.ngrok-free.dev/releases/audiosync.apk",
    "releaseNotes": "Major update: Time-synced lyrics overlay, room stability fixes, UI improvements",
    # List of version codes that MUST update (mandatory)
    "mandatoryBelow": 9,  # All versions below this must update
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
                        related.append({
                            "videoId": vid,
                            "title": item.get("title", "Unknown"),
                            "duration": item.get("duration"),
                            "thumbnail": item.get("thumbnail"),
                            "uploader": item.get("uploaderName", "Unknown")
                        })

                return {
                    "success": True,
                    "source": "piped",
                    "url": best_audio.get("url"),
                    "title": data.get("title"),
                    "uploader": data.get("uploader"),
                    "duration": data.get("duration"),
                    "thumbnail": data.get("thumbnailUrl"),
                    "related": related
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
YTDLP_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".ytdlp_cache")
os.makedirs(YTDLP_CACHE_DIR, exist_ok=True)

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

    return {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "cachedir": YTDLP_CACHE_DIR,
        "extractor_args": {
            "youtube": {
                "js_runtimes": [js_runtime],
            }
        },
    }


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

        print(f"[yt-dlp timing] {video_id}: lock_wait={t_got_lock-t_lock:.2f}s, instance={t_instance-t_got_lock:.2f}s, extract={t_extract-t_instance:.2f}s, total={t_extract-t_lock:.2f}s")

        if not info:
            return AudioResponse(
                success=False,
                videoId=video_id,
                error="Failed to extract video info"
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
                success=False,
                videoId=video_id,
                error="No audio URL found"
            )

        return AudioResponse(
            success=True,
            videoId=video_id,
            url=audio_url,
            title=info.get("title", "Unknown"),
            duration=info.get("duration", 0),
            thumbnail=info.get("thumbnail", ""),
            uploader=info.get("uploader", "Unknown"),
            source="ytdlp"
        )

    except Exception as e:
        # If instance is broken, reset it for next request
        _reset_audio_ydl()
        return AudioResponse(
            success=False,
            videoId=video_id,
            error=str(e)
        )


async def get_audio_ytdlp(video_id: str) -> AudioResponse:
    """Truly async: runs yt-dlp in thread pool"""
    return await asyncio.to_thread(_extract_audio_ytdlp, video_id)


def _extract_related_ytdlp(video_id: str, limit: int = 25) -> RelatedResponse:
    """Sync yt-dlp related extraction (runs in thread pool)"""

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "skip_download": True,
        "playlist_items": f"1-{limit + 1}",
    }

    try:
        mix_url = f"https://www.youtube.com/watch?v={video_id}&list=RD{video_id}"

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(mix_url, download=False)

            if not info or "entries" not in info:
                return RelatedResponse(
                    success=False,
                    videoId=video_id,
                    error="No related songs found"
                )

            related = []
            for entry in info["entries"]:
                if not entry:
                    continue

                vid = entry.get("id", "")
                if not vid or vid == video_id:
                    continue

                related.append(SearchResult(
                    videoId=vid,
                    title=entry.get("title", "Unknown"),
                    duration=entry.get("duration"),
                    thumbnail=entry.get("thumbnail") or entry.get("thumbnails", [{}])[-1].get("url", ""),
                    uploader=entry.get("uploader") or entry.get("channel", "Unknown"),
                ))

                if len(related) >= limit:
                    break

            return RelatedResponse(
                success=True,
                videoId=video_id,
                related=related,
                source="ytdlp"
            )

    except Exception as e:
        return RelatedResponse(
            success=False,
            videoId=video_id,
            error=str(e)
        )


async def get_related_ytdlp(video_id: str, limit: int = 25) -> RelatedResponse:
    """Truly async: runs yt-dlp in thread pool"""
    return await asyncio.to_thread(_extract_related_ytdlp, video_id, limit)


# ==================== API ENDPOINTS ====================

@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "AudioSync API",
        "piped_enabled": PIPED_ENABLED,
        "piped_url": PIPED_URL if PIPED_ENABLED else None
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "piped_enabled": PIPED_ENABLED}


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
                ydl.extract_info("https://www.youtube.com/watch?v=jNQXAC9IVRw", download=False)
            print(f"[Startup] Pre-warm complete! ({time.time()-t:.2f}s) - subsequent requests will be faster")
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
            print(f"[Startup] Browse: moods cached ({time.time()-t:.2f}s)")

            # Pre-warm charts (fetch playlist tracks)
            t2 = time.time()
            raw_charts = await asyncio.to_thread(_ytmusic.get_charts, "ZZ")
            videos = raw_charts.get("videos", [])
            if videos and isinstance(videos, list):
                playlist_id = videos[0].get("playlistId", "")
                if playlist_id:
                    playlist = await asyncio.to_thread(_ytmusic.get_playlist, playlist_id, 50)
                    songs = []
                    for idx, track in enumerate(playlist.get("tracks", [])):
                        vid = track.get("videoId")
                        if not vid:
                            continue
                        thumbs = track.get("thumbnails", [])
                        thumb = thumbs[-1].get("url") if thumbs and isinstance(thumbs[-1], dict) else None
                        artists = track.get("artists", [])
                        artist = artists[0].get("name") if artists and isinstance(artists[0], dict) else None
                        songs.append(BrowseChartTrack(
                            videoId=vid, title=track.get("title", "Unknown"),
                            duration=track.get("duration_seconds"), thumbnail=thumb,
                            uploader=artist, rank=idx + 1,
                        ))
                    response = BrowseChartsResponse(success=True, country="ZZ", songs=songs)
                    set_browse_cache("charts_ZZ", response)
                    print(f"[Startup] Browse: charts cached ({len(songs)} songs, {time.time()-t2:.2f}s)")

            print(f"[Startup] Browse cache pre-warmed ({time.time()-t:.2f}s total)")
        except Exception as e:
            print(f"[Startup] Browse pre-warm failed (non-critical): {e}")
    asyncio.create_task(_warmup_browse())


# ==================== BROWSE ENDPOINTS (ytmusicapi) ====================

@app.get("/browse/moods", response_model=BrowseMoodsResponse)
async def browse_moods():
    """Get mood/genre categories from YouTube Music"""
    start = time.time()
    print("[/browse/moods] Request")

    cached = get_browse_cached("moods")
    if cached:
        # Parse cached dict into response
        sections = []
        for section_title, cats in cached.items():
            categories = [BrowseMoodCategory(title=c["title"], params=c["params"]) for c in cats]
            sections.append(BrowseMoodSection(title=section_title, categories=categories))
        print(f"[/browse/moods] CACHE HIT ({time.time()-start:.2f}s)")
        return BrowseMoodsResponse(success=True, sections=sections)

    try:
        raw = await asyncio.to_thread(_ytmusic.get_mood_categories)
        set_browse_cache("moods", raw)

        sections = []
        for section_title, cats in raw.items():
            categories = [BrowseMoodCategory(title=c["title"], params=c["params"]) for c in cats]
            sections.append(BrowseMoodSection(title=section_title, categories=categories))

        print(f"[/browse/moods] {sum(len(s.categories) for s in sections)} categories ({time.time()-start:.2f}s)")
        return BrowseMoodsResponse(success=True, sections=sections)
    except Exception as e:
        print(f"[/browse/moods] ERROR: {e}")
        return BrowseMoodsResponse(success=False)


@app.get("/browse/mood_playlists", response_model=BrowseMoodPlaylistsResponse)
async def browse_mood_playlists(params: str):
    """Get playlists for a specific mood/genre category"""
    start = time.time()
    cache_key = f"mood_playlists_{params}"
    print(f"[/browse/mood_playlists] Request: {params[:20]}...")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/browse/mood_playlists] CACHE HIT ({time.time()-start:.2f}s)")
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
            playlists.append(BrowsePlaylistItem(
                playlistId=playlist_id,
                title=p.get("title", "Unknown"),
                thumbnails=thumbs,
                description=p.get("description"),
                count=p.get("count"),
                author=p.get("author"),
            ))

        response = BrowseMoodPlaylistsResponse(success=True, title="", playlists=playlists)
        set_browse_cache(cache_key, response)
        print(f"[/browse/mood_playlists] {len(playlists)} playlists ({time.time()-start:.2f}s)")
        return response
    except Exception as e:
        print(f"[/browse/mood_playlists] ERROR: {e}")
        return BrowseMoodPlaylistsResponse(success=False)


@app.get("/browse/playlist/{playlist_id}", response_model=BrowsePlaylistDetailResponse)
async def browse_playlist_detail(playlist_id: str, limit: int = 50):
    """Get playlist tracks"""
    start = time.time()
    cache_key = f"playlist_{playlist_id}_{limit}"
    print(f"[/browse/playlist] Request: {playlist_id}")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/browse/playlist] CACHE HIT ({time.time()-start:.2f}s)")
        return cached

    try:
        raw = await asyncio.to_thread(_ytmusic.get_playlist, playlist_id, limit)

        tracks = []
        for t in raw.get("tracks", []):
            video_id = t.get("videoId")
            if not video_id:
                continue

            # Get thumbnail
            thumbnail = None
            thumbs = t.get("thumbnails", [])
            if thumbs and isinstance(thumbs, list):
                thumbnail = thumbs[-1].get("url") if isinstance(thumbs[-1], dict) else None

            # Get artist
            uploader = None
            artists = t.get("artists", [])
            if artists and isinstance(artists, list) and isinstance(artists[0], dict):
                uploader = artists[0].get("name")

            # Duration
            duration = t.get("duration_seconds")

            tracks.append(BrowsePlaylistTrack(
                videoId=video_id,
                title=t.get("title", "Unknown"),
                duration=duration,
                thumbnail=thumbnail,
                uploader=uploader,
            ))

        # Playlist thumbnail
        pl_thumb = None
        pl_thumbs = raw.get("thumbnails", [])
        if pl_thumbs and isinstance(pl_thumbs, list):
            pl_thumb = pl_thumbs[-1].get("url") if isinstance(pl_thumbs[-1], dict) else None

        response = BrowsePlaylistDetailResponse(
            success=True,
            title=raw.get("title", ""),
            description=raw.get("description"),
            thumbnail=pl_thumb,
            trackCount=raw.get("trackCount"),
            tracks=tracks,
        )
        set_browse_cache(cache_key, response)
        print(f"[/browse/playlist] {len(tracks)} tracks ({time.time()-start:.2f}s)")
        return response
    except Exception as e:
        print(f"[/browse/playlist] ERROR: {e}")
        return BrowsePlaylistDetailResponse(success=False)


@app.get("/browse/charts", response_model=BrowseChartsResponse)
async def browse_charts(country: str = "ZZ"):
    """Get music charts (top songs) — fetches first chart playlist tracks"""
    start = time.time()
    cache_key = f"charts_{country}"
    print(f"[/browse/charts] Request: country={country}")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/browse/charts] CACHE HIT ({time.time()-start:.2f}s)")
        return cached

    try:
        # get_charts returns playlist references, not individual songs
        raw = await asyncio.to_thread(_ytmusic.get_charts, country)

        videos = raw.get("videos", [])
        if not videos or not isinstance(videos, list):
            return BrowseChartsResponse(success=False, country=country)

        # Fetch the first chart playlist (e.g. "Top 100 Music Videos Global")
        playlist_id = videos[0].get("playlistId", "")
        if not playlist_id:
            return BrowseChartsResponse(success=False, country=country)

        print(f"[/browse/charts] Fetching chart playlist: {playlist_id}")
        playlist = await asyncio.to_thread(_ytmusic.get_playlist, playlist_id, 50)

        songs = []
        for idx, track in enumerate(playlist.get("tracks", [])):
            video_id = track.get("videoId")
            if not video_id:
                continue

            # Get thumbnail
            thumbnail = None
            thumbs = track.get("thumbnails", [])
            if thumbs and isinstance(thumbs, list):
                thumbnail = thumbs[-1].get("url") if isinstance(thumbs[-1], dict) else None

            # Get artist
            uploader = None
            artists = track.get("artists", [])
            if artists and isinstance(artists, list) and isinstance(artists[0], dict):
                uploader = artists[0].get("name")

            songs.append(BrowseChartTrack(
                videoId=video_id,
                title=track.get("title", "Unknown"),
                duration=track.get("duration_seconds"),
                thumbnail=thumbnail,
                uploader=uploader,
                rank=idx + 1,
            ))

        response = BrowseChartsResponse(success=True, country=country, songs=songs)
        set_browse_cache(cache_key, response)
        print(f"[/browse/charts] {len(songs)} songs ({time.time()-start:.2f}s)")
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
        end_ms = entries[i + 1]["startMs"] if i + 1 < len(entries) else entry["startMs"] + 5000
        result.append(LyricsLine(text=entry["text"], startMs=entry["startMs"], endMs=end_ms))
    return result


async def _lrclib_request(client: httpx.AsyncClient, url: str, params: dict, retries: int = 2) -> Optional[httpx.Response]:
    """Make an LRCLIB request with retries"""
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url, params=params, headers={"User-Agent": "AudioSync/1.0"})
            if resp.status_code == 200:
                return resp
        except Exception:
            if attempt < retries:
                await asyncio.sleep(0.5)
            else:
                return None
    return None


async def _fetch_lrclib(title: str, artist: str, duration_secs: int = 0) -> Optional[dict]:
    """Fetch synced lyrics from LRCLIB as fallback"""
    # Strip parenthetical suffixes like (From "Movie") for cleaner matching
    clean_title = re.sub(r'\s*\(From\s+"[^"]*"\)', '', title, flags=re.IGNORECASE).strip()
    # Handle pipe-separated titles like "SONG NAME | VIDEO SONG | ARTIST"
    if "|" in clean_title:
        clean_title = clean_title.split("|")[0].strip()
    titles = [title, clean_title] if clean_title != title else [title]

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Try exact match first if we have duration
            if duration_secs > 0:
                for t in titles:
                    resp = await _lrclib_request(client, "https://lrclib.net/api/get",
                        {"track_name": t, "artist_name": artist, "duration": duration_secs})
                    if resp:
                        data = resp.json()
                        if data.get("syncedLyrics"):
                            return data

            # Search with artist
            for t in titles:
                resp = await _lrclib_request(client, "https://lrclib.net/api/search",
                    {"track_name": t, "artist_name": artist})
                if resp:
                    for r in resp.json():
                        if r.get("syncedLyrics"):
                            return r

            # Fallback: search with title only (no artist) for better matching
            for t in titles:
                resp = await _lrclib_request(client, "https://lrclib.net/api/search",
                    {"track_name": t})
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


@app.get("/lyrics/{video_id}", response_model=LyricsResponse)
async def get_lyrics_endpoint(video_id: str):
    """Get time-synced lyrics for a song via ytmusicapi with LRCLIB fallback"""
    start = time.time()
    cache_key = f"lyrics_{video_id}"
    print(f"[/lyrics] Request: {video_id}")

    cached = get_browse_cached(cache_key)
    if cached is not None:
        print(f"[/lyrics] CACHE HIT ({time.time()-start:.2f}s)")
        return cached

    try:
        # Step 1: Get watch playlist for lyrics browseId + track info
        watch = await asyncio.to_thread(_ytmusic.get_watch_playlist, video_id)
        lyrics_browse_id = watch.get("lyrics") if watch else None

        # Extract track info for LRCLIB fallback
        track_title = ""
        track_artist = ""
        track_duration_secs = 0
        if watch and watch.get("tracks"):
            track = watch["tracks"][0]
            track_title = track.get("title", "")
            artists = track.get("artists", [])
            if artists:
                track_artist = artists[0].get("name", "")
            # Parse "m:ss" length to seconds
            length_str = track.get("length", "")
            if ":" in length_str:
                parts = length_str.split(":")
                try:
                    track_duration_secs = int(parts[0]) * 60 + int(parts[1])
                except ValueError:
                    pass

        # Step 2: Try YouTube Music lyrics
        lines = []
        plain_lyrics = None
        source = ""

        if lyrics_browse_id:
            try:
                raw_lyrics = await asyncio.to_thread(_ytmusic.get_lyrics, lyrics_browse_id, True)
                if raw_lyrics and raw_lyrics.get("lyrics"):
                    source = raw_lyrics.get("source", "")
                    has_timestamps = raw_lyrics.get("hasTimestamps", False)
                    lyrics_data = raw_lyrics.get("lyrics")

                    if has_timestamps and isinstance(lyrics_data, list):
                        for entry in lyrics_data:
                            lines.append(LyricsLine(
                                text=getattr(entry, "text", ""),
                                startMs=int(getattr(entry, "start_time", 0)),
                                endMs=int(getattr(entry, "end_time", 0)),
                            ))
                    elif isinstance(lyrics_data, str):
                        plain_lyrics = lyrics_data
            except Exception as yt_err:
                print(f"[/lyrics] YTMusic get_lyrics error: {yt_err}, falling back to LRCLIB")

        # Step 3: Fallback to LRCLIB if no timed lyrics from YouTube Music
        if not lines and track_title:
            print(f"[/lyrics] YTMusic no timed lyrics, trying LRCLIB for '{track_title}' - '{track_artist}'")
            lrclib_data = await _fetch_lrclib(track_title, track_artist, track_duration_secs)
            if lrclib_data:
                synced = lrclib_data.get("syncedLyrics")
                if synced:
                    lines = _parse_lrc(synced)
                    source = "LRCLIB"
                    plain_lyrics = None
                    print(f"[/lyrics] LRCLIB: {len(lines)} synced lines")
                elif not plain_lyrics:
                    plain_text = lrclib_data.get("plainLyrics")
                    if plain_text:
                        plain_lyrics = plain_text
                        source = "LRCLIB"
                        print(f"[/lyrics] LRCLIB: plain lyrics ({len(plain_text)} chars)")

        if not lines and not plain_lyrics:
            print(f"[/lyrics] No lyrics found ({time.time()-start:.2f}s)")
            return LyricsResponse(
                success=False,
                videoId=video_id,
                error="No lyrics available for this song"
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
        print(f"[/lyrics] {len(lines)} timed lines, source={source} ({time.time()-start:.2f}s)")
        return response

    except Exception as e:
        print(f"[/lyrics] ERROR: {e}")
        return LyricsResponse(success=False, videoId=video_id, error=str(e))


@app.get("/cache/stats")
async def cache_stats():
    """Get cache statistics"""
    now = time.time()
    valid_urls = sum(1 for v in _cache.values() if now - v["timestamp"] < CACHE_TTL)
    valid_suggestions = sum(1 for v in _suggestions_cache.values() if now - v["timestamp"] < CACHE_TTL)
    return {
        "url_cache": {"total": len(_cache), "valid": valid_urls},
        "suggestions_cache": {"total": len(_suggestions_cache), "valid": valid_suggestions},
        "cache_ttl_hours": CACHE_TTL / 3600,
        "cached_video_ids": list(_cache.keys())[:20]
    }


@app.delete("/cache/clear")
async def cache_clear():
    """Clear all cache"""
    url_count = len(_cache)
    sug_count = len(_suggestions_cache)
    _cache.clear()
    _suggestions_cache.clear()
    return {"cleared_urls": url_count, "cleared_suggestions": sug_count}


@app.post("/ytdlp/reset")
async def ytdlp_reset():
    """Reset the reusable yt-dlp instance (forces re-download of player JS)"""
    def _reset():
        with _ydl_audio_lock:
            _reset_audio_ydl()
    await asyncio.to_thread(_reset)
    return {"status": "reset", "message": "yt-dlp instance reset. Next request will re-download player JS."}


@app.get("/update/check", response_model=UpdateResponse)
async def check_update(versionCode: int, versionName: str = ""):
    """Check if app update is available"""
    latest_code = APP_UPDATE_CONFIG["latestVersionCode"]
    latest_version = APP_UPDATE_CONFIG["latestVersion"]
    mandatory_below = APP_UPDATE_CONFIG["mandatoryBelow"]

    update_available = versionCode < latest_code
    is_mandatory = versionCode < mandatory_below

    return UpdateResponse(
        updateAvailable=update_available,
        mandatory=is_mandatory if update_available else False,
        latestVersion=latest_version,
        latestVersionCode=latest_code,
        currentVersion=versionName,
        currentVersionCode=versionCode,
        apkUrl=APP_UPDATE_CONFIG["apkUrl"] if update_available else None,
        releaseNotes=APP_UPDATE_CONFIG["releaseNotes"] if update_available else None,
    )


@app.get("/rooms", response_model=RoomListResponse)
async def list_rooms():
    """List all active rooms for discovery"""
    rooms = room_manager.list_rooms()
    return RoomListResponse(
        success=True,
        rooms=[RoomListItem(**r) for r in rooms]
    )


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
            "source": "piped"
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
            "source": "ytdlp"
        }
        set_cache(video_id, result)
        return result

    return {
        "success": False,
        "videoId": video_id,
        "error": ytdlp_result.error
    }


@app.get("/audio/{video_id}", response_model=AudioResponse)
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
    print(f"[/audio] {video_id} → {source} ({time.time()-start:.2f}s)")

    return AudioResponse(**result)


@app.get("/stream/{video_id}", response_model=StreamResponse)
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
                suggestions = [
                    StreamSuggestion(**r) for r in cached_sug[:15]
                ]
                print(f"[/stream] {video_id} → FULL CACHE HIT ({time.time()-start:.2f}s)")
            else:
                t1 = time.time()
                related_response = await get_related_ytdlp(video_id, limit=15)
                print(f"[/stream] {video_id} → suggestions fetch: {time.time()-t1:.2f}s")
                if related_response.success:
                    suggestions = [
                        StreamSuggestion(
                            videoId=r.videoId, title=r.title, duration=r.duration,
                            thumbnail=r.thumbnail, uploader=r.uploader
                        ) for r in related_response.related
                    ]
                    set_suggestions_cache(video_id, [r.model_dump() for r in related_response.related])
        print(f"[/stream] {video_id} → CACHE HIT + {len(suggestions)} suggestions ({time.time()-start:.2f}s)")
        return StreamResponse(
            success=True, videoId=video_id, audioUrl=cached["url"],
            title=cached.get("title"), duration=cached.get("duration"),
            thumbnail=cached.get("thumbnail"), uploader=cached.get("uploader"),
            suggestions=suggestions
        )

    # Try Piped first (fast, includes related videos)
    piped_result = await get_from_piped(video_id)
    if piped_result and piped_result.get("url"):
        # Cache it
        set_cache(video_id, {
            "success": True,
            "videoId": video_id,
            "url": piped_result["url"],
            "title": piped_result.get("title"),
            "duration": piped_result.get("duration"),
            "thumbnail": piped_result.get("thumbnail"),
            "uploader": piped_result.get("uploader"),
            "source": "piped"
        })

        # Get suggestions from Piped response
        if include_suggestions and piped_result.get("related"):
            suggestions = [
                StreamSuggestion(
                    videoId=r["videoId"],
                    title=r["title"],
                    duration=r.get("duration"),
                    thumbnail=r.get("thumbnail"),
                    uploader=r.get("uploader")
                ) for r in piped_result["related"][:15]
            ]

        return StreamResponse(
            success=True,
            videoId=video_id,
            audioUrl=piped_result["url"],
            title=piped_result.get("title"),
            duration=piped_result.get("duration"),
            thumbnail=piped_result.get("thumbnail"),
            uploader=piped_result.get("uploader"),
            suggestions=suggestions
        )

    # Fallback to yt-dlp - fetch audio + suggestions IN PARALLEL
    t1 = time.time()
    if include_suggestions:
        ytdlp_result, related_response = await asyncio.gather(
            get_audio_ytdlp(video_id),
            get_related_ytdlp(video_id, limit=15)
        )
    else:
        ytdlp_result = await get_audio_ytdlp(video_id)
        related_response = None
    print(f"[/stream] {video_id} → yt-dlp parallel fetch: {time.time()-t1:.2f}s")

    if ytdlp_result.success and ytdlp_result.url:
        # Cache yt-dlp result
        set_cache(video_id, {
            "success": True,
            "videoId": video_id,
            "url": ytdlp_result.url,
            "title": ytdlp_result.title,
            "duration": ytdlp_result.duration,
            "thumbnail": ytdlp_result.thumbnail,
            "uploader": ytdlp_result.uploader,
            "source": "ytdlp"
        })

        # Use suggestions from parallel fetch + cache them
        if related_response and related_response.success:
            suggestions = [
                StreamSuggestion(
                    videoId=r.videoId,
                    title=r.title,
                    duration=r.duration,
                    thumbnail=r.thumbnail,
                    uploader=r.uploader
                ) for r in related_response.related
            ]
            set_suggestions_cache(video_id, [r.model_dump() for r in related_response.related])

        print(f"[/stream] {video_id} → yt-dlp OK + {len(suggestions)} suggestions ({time.time()-start:.2f}s total)")
        return StreamResponse(
            success=True,
            videoId=video_id,
            audioUrl=ytdlp_result.url,
            title=ytdlp_result.title,
            duration=ytdlp_result.duration,
            thumbnail=ytdlp_result.thumbnail,
            uploader=ytdlp_result.uploader,
            suggestions=suggestions
        )

    print(f"[/stream] {video_id} → FAILED ({time.time()-start:.2f}s)")
    return StreamResponse(
        success=False,
        videoId=video_id,
        error="Failed to get stream URL"
    )


@app.get("/related/{video_id}", response_model=RelatedResponse)
async def get_related(video_id: str, limit: int = 25):
    """
    Get related songs (suggestions).
    Checks cache first, then Piped, then yt-dlp.
    """
    start = time.time()
    print(f"[/related] Request: {video_id}")

    # Check suggestions cache first
    cached = get_cached_suggestions(video_id)
    if cached:
        print(f"[/related] {video_id} → CACHE HIT ({time.time()-start:.2f}s)")
        return RelatedResponse(
            success=True,
            videoId=video_id,
            related=[SearchResult(**r) for r in cached[:limit]],
            source="cache"
        )

    # Try Piped first (fast, includes related)
    piped_result = await get_from_piped(video_id)
    if piped_result and piped_result.get("related"):
        # Cache suggestions
        set_suggestions_cache(video_id, piped_result["related"])
        print(f"[/related] {video_id} → PIPED ({time.time()-start:.2f}s)")
        return RelatedResponse(
            success=True,
            videoId=video_id,
            related=[SearchResult(**r) for r in piped_result["related"][:limit]],
            source="piped"
        )

    # Fallback to yt-dlp
    result = await get_related_ytdlp(video_id, limit)
    if result.success and result.related:
        # Cache suggestions
        set_suggestions_cache(video_id, [r.model_dump() for r in result.related])
    print(f"[/related] {video_id} → yt-dlp: {len(result.related)} results ({time.time()-start:.2f}s)")
    return result


@app.get("/prefetch", response_model=PrefetchResponse)
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
            set_cache(vid, {
                "success": True,
                "videoId": vid,
                "url": result["url"],
                "title": result.get("title"),
                "duration": result.get("duration"),
                "thumbnail": result.get("thumbnail"),
                "uploader": result.get("uploader"),
                "source": "piped"
            })
            return PrefetchResult(videoId=vid, audioUrl=result["url"], success=True)

        # Don't fallback to yt-dlp for prefetch (too slow)
        # Return failure, app will fetch individually if needed
        return PrefetchResult(videoId=vid, audioUrl=None, success=False)

    # Fetch all in parallel
    results = await asyncio.gather(*[fetch_one(vid) for vid in ids])

    return PrefetchResponse(
        success=True,
        results=list(results)
    )


@app.get("/search", response_model=SearchResponse)
async def search(q: str, limit: int = 10):
    """Search YouTube videos using yt-dlp (no Piped equivalent)"""
    start = time.time()
    print(f"[/search] Request: '{q}'")

    if not q or len(q.strip()) == 0:
        return SearchResponse(
            success=False,
            query=q,
            error="Query cannot be empty"
        )

    def _search_ytdlp():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }

        try:
            search_query = f"ytsearch{limit}:{q}"

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(search_query, download=False)

                if not info or "entries" not in info:
                    return SearchResponse(
                        success=False,
                        query=q,
                        error="No results found"
                    )

                results = []
                for entry in info["entries"]:
                    if entry:
                        results.append(SearchResult(
                            videoId=entry.get("id", ""),
                            title=entry.get("title", "Unknown"),
                            duration=entry.get("duration"),
                            thumbnail=entry.get("thumbnail") or entry.get("thumbnails", [{}])[0].get("url", ""),
                            uploader=entry.get("uploader") or entry.get("channel", "Unknown"),
                        ))

                return SearchResponse(
                    success=True,
                    query=q,
                    results=results
                )

        except Exception as e:
            return SearchResponse(
                success=False,
                query=q,
                error=str(e)
            )

    result = await asyncio.to_thread(_search_ytdlp)
    print(f"[/search] '{q}' → {len(result.results)} results ({time.time()-start:.2f}s)")
    return result


@app.get("/next/{video_id}", response_model=NextResponse)
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
                        title=next_piped.get("title", first_related.get("title", "Unknown")),
                        duration=next_piped.get("duration", first_related.get("duration")),
                        thumbnail=next_piped.get("thumbnail", first_related.get("thumbnail")),
                        uploader=next_piped.get("uploader", first_related.get("uploader")),
                        audioUrl=next_piped["url"]
                    ),
                    suggestions=[SearchResult(**r) for r in related[1:26]]  # Skip first, next 25
                )

    # Fallback to yt-dlp
    # Step 1: Get related songs
    related_result = await get_related_ytdlp(video_id, limit=1)

    if not related_result.success or not related_result.related:
        return NextResponse(
            success=False,
            currentVideoId=video_id,
            error="No related songs found"
        )

    # Step 2: Get the first related song
    next_song_info = related_result.related[0]
    next_video_id = next_song_info.videoId

    # Step 3+4: Get audio URL AND suggestions IN PARALLEL
    audio_result, next_suggestions = await asyncio.gather(
        get_audio_ytdlp(next_video_id),
        get_related_ytdlp(next_video_id, limit=25)
    )

    if not audio_result.success or not audio_result.url:
        return NextResponse(
            success=False,
            currentVideoId=video_id,
            error=f"Failed to get audio URL: {audio_result.error}"
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
            audioUrl=audio_result.url
        ),
        suggestions=next_suggestions.related if next_suggestions.success else []
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

    room = room_manager.create_room(client_id, websocket, host_name=host_name, password=password)
    print(f"[WS] Room {room.code} created by {client_id[:8]} ({host_name}), locked={password is not None}")
    await ws_send(websocket, {
        "type": "room_created",
        "code": room.code,
        "hostName": host_name,
        "hasPassword": password is not None,
        "members": room_manager.get_member_list(room),
    })


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
            await room_manager.broadcast(old_room, {
                "type": "member_left",
                "count": len(old_room.members),
                "members": room_manager.get_member_list(old_room),
            })

    # Check room exists first (before joining) to validate password
    room = room_manager.rooms.get(code)
    if room is None:
        await ws_send(websocket, {"type": "error", "message": "Room not found"})
        return

    # Validate password if room is locked
    if room.password is not None:
        provided = msg.get("password", "")
        if provided != room.password:
            await ws_send(websocket, {"type": "error", "message": "Wrong password"})
            return

    name = msg.get("name", "Unknown")
    room, promoted = room_manager.join_room(code, client_id, websocket, name=name)
    role = "host" if promoted else "guest"
    print(f"[WS] {client_id[:8]} ({name}) joined room {code} as {role} ({len(room.members)} members)")

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
    await ws_send(websocket, {"type": "room_joined", "state": state})

    # Notify others with updated member list
    await room_manager.broadcast(room, {
        "type": "member_joined",
        "count": len(room.members),
        "members": room_manager.get_member_list(room),
    }, exclude_id=client_id)


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
                await ws_send(member.websocket, {
                    "type": "play_blocked",
                    "reason": "voted_song_pending",
                    "topVideoId": top.video_id,
                    "topTitle": top.title,
                })
            return

    print(f"[WS] Room {room.code}: host playing {video_id}")

    # Extract audio URL once for everyone
    audio_data = await extract_audio_url(video_id)
    if not audio_data.get("success") or not audio_data.get("url"):
        await room_manager.broadcast(room, {
            "type": "error",
            "message": f"Failed to extract audio: {audio_data.get('error', 'Unknown error')}"
        })
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
    await room_manager.broadcast(room, {
        "type": "sync_play",
        "videoId": video_id,
        "title": room.current_song["title"],
        "audioUrl": audio_data["url"],
        "thumbnail": room.current_song["thumbnail"],
        "uploader": room.current_song["uploader"],
        "duration": room.current_song["duration"],
        "position": 0,
        "playStartTime": play_start_time,
    })


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

    await room_manager.broadcast(room, {
        "type": "sync_pause",
        "position": position,
    })


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

    await room_manager.broadcast(room, {
        "type": "sync_resume",
        "position": position,
        "resumeTime": resume_time,
    })


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

    await room_manager.broadcast(room, {
        "type": "sync_seek",
        "position": position,
    })


async def handle_next(client_id: str):
    if not room_manager.is_host(client_id):
        return
    room = room_manager.get_room_for_client(client_id)
    if not room or not room.queue:
        return

    # Pop from sorted queue (highest-voted request first, then suggestions)
    sorted_q = room.get_sorted_queue()
    if not sorted_q:
        return
    next_item = sorted_q[0]
    room.queue.remove(next_item)
    print(f"[WS] Room {room.code}: next → {next_item.video_id} (votes={len(next_item.votes)}, suggestion={next_item.is_suggestion})")

    # Broadcast updated queue (personalized)
    await room_manager.broadcast_queue(room)

    # Play the next song (reuses handle_play logic — skip the voted check for next)
    # We call the play logic directly instead of handle_play to bypass vote-block
    video_id = next_item.video_id
    print(f"[WS] Room {room.code}: host playing {video_id}")

    audio_data = await extract_audio_url(video_id)
    if not audio_data.get("success") or not audio_data.get("url"):
        await room_manager.broadcast(room, {
            "type": "error",
            "message": f"Failed to extract audio: {audio_data.get('error', 'Unknown error')}"
        })
        return

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
    await room_manager.broadcast(room, {
        "type": "sync_play",
        "videoId": video_id,
        "title": room.current_song["title"],
        "audioUrl": audio_data["url"],
        "thumbnail": room.current_song["thumbnail"],
        "uploader": room.current_song["uploader"],
        "duration": room.current_song["duration"],
        "position": 0,
        "playStartTime": play_start_time,
    })


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
        new_suggestions.append(QueueItem(
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
        ))
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

    # Broadcast position to guests for drift correction
    await room_manager.broadcast(room, {
        "type": "sync_seek",
        "position": position,
    }, exclude_id=client_id)


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
    await room_manager.broadcast(room, {
        "type": "chat_message",
        "senderClientId": client_id,
        "senderName": member.name,
        "text": text,
        "timestamp": int(time.time() * 1000),
    })


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

    # Broadcast updated queue (personalized)
    await room_manager.broadcast_queue(room)

    # Broadcast chat message about the suggestion (includes song metadata for card UI)
    await room_manager.broadcast(room, {
        "type": "chat_message",
        "senderClientId": client_id,
        "senderName": member.name,
        "text": f"suggested \"{item.title}\"",
        "timestamp": int(time.time() * 1000),
        "isSuggestion": True,
        "suggestionTitle": item.title,
        "suggestionThumbnail": item.thumbnail,
        "suggestionUploader": item.uploader,
    })


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
    print(f"[WS] Host {client_id[:8]} kicked {target_id[:8]} from room {room.code}")
    # Notify kicked client and close their connection
    try:
        await ws_send(target_ws, {"type": "kicked", "message": "You were removed from the room"})
        await target_ws.close()
    except Exception:
        pass
    # Broadcast updated member list to remaining members
    await room_manager.broadcast(room, {
        "type": "member_left",
        "count": len(room.members),
        "members": room_manager.get_member_list(room),
    })


async def handle_share_lyrics(client_id: str, msg: dict):
    """Host shares fetched lyrics with all room members"""
    room = room_manager.get_room_for_client(client_id)
    if not room:
        return
    video_id = msg.get("videoId", "")
    if not video_id:
        return
    # Broadcast lyrics to all members (including host for consistency)
    await room_manager.broadcast(room, {
        "type": "sync_lyrics",
        "videoId": video_id,
        "success": msg.get("success", False),
        "hasTimestamps": msg.get("hasTimestamps", False),
        "lines": msg.get("lines", []),
        "plainLyrics": msg.get("plainLyrics"),
        "source": msg.get("source"),
    })


async def handle_leave(client_id: str):
    """Explicit leave (user clicked Leave button). Destroys room immediately if host."""
    # Clean up queue before leaving (remove their requests/votes)
    room = room_manager.get_room_for_client(client_id)
    if room:
        room.remove_member_from_queue(client_id)

    code, was_host, remaining_ws = room_manager.leave_room(client_id)
    if not code:
        return

    print(f"[WS] {client_id[:8]} left room {code} (was_host={was_host})")

    if was_host:
        # Room was destroyed — notify remaining members directly
        for ws in remaining_ws:
            await ws_send(ws, {"type": "room_closed"})
    else:
        room = room_manager.rooms.get(code)
        if room:
            # Rebroadcast queue since member's votes/requests were removed
            await room_manager.broadcast_queue(room)
            await room_manager.broadcast(room, {
                "type": "member_left",
                "count": len(room.members),
                "members": room_manager.get_member_list(room),
            })


async def handle_disconnect(client_id: str):
    """Unexpected disconnect (WebSocket dropped). Gives host a grace period to reconnect."""
    # Clean up queue (remove their requests/votes)
    room = room_manager.get_room_for_client(client_id)
    if room:
        room.remove_member_from_queue(client_id)

    code, was_host, remaining_ws = room_manager.disconnect_member(client_id)
    if not code:
        return

    print(f"[WS] {client_id[:8]} disconnected from room {code} (was_host={was_host})")

    room = room_manager.rooms.get(code)
    if room:
        # Rebroadcast queue since member's votes/requests were removed
        await room_manager.broadcast_queue(room)
        await room_manager.broadcast(room, {
            "type": "member_left",
            "count": len(room.members),
            "members": room_manager.get_member_list(room),
        })

    if was_host and room:
        # Schedule room destruction after grace period
        asyncio.create_task(destroy_room_after_grace(client_id, code, 30))


async def destroy_room_after_grace(client_id: str, code: str, delay: int = 30):
    """Wait for host to reconnect, then destroy room if they didn't."""
    await asyncio.sleep(delay)
    room_manager.cleanup_stale_disconnects()
    room = room_manager.rooms.get(code)
    if room is None:
        return
    # Check if room has an ACTIVE host (host_id present in room.members)
    host_active = room.host_id is not None and room.host_id in room.members
    if not host_active:
        # Host didn't rejoin — destroy room and notify remaining guests
        remaining_ws = [m.websocket for m in room.members.values()]
        for mid in list(room.members.keys()):
            room_manager._client_to_room.pop(mid, None)
        del room_manager.rooms[code]
        for ws in remaining_ws:
            await ws_send(ws, {"type": "room_closed"})
        print(f"[WS] Room {code} destroyed after host grace period expired")


async def handle_rejoin_room(client_id: str, websocket: WebSocket, msg: dict):
    """Handle a client rejoining a room after disconnect."""
    code = msg.get("code", "").upper()
    name = msg.get("name", "Unknown")
    previous_client_id = msg.get("previousClientId")

    if not code:
        await ws_send(websocket, {"type": "error", "message": "Room code required"})
        return

    room, was_host = room_manager.rejoin_room(client_id, websocket, code, name,
                                               previous_client_id=previous_client_id)
    if not room:
        await ws_send(websocket, {"type": "error", "message": "Room no longer exists", "rejoinFailed": True, "code": code})
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
    await ws_send(websocket, {"type": "room_joined", "state": state})

    # Notify other members
    await room_manager.broadcast(room, {
        "type": "member_joined",
        "count": len(room.members),
        "members": room_manager.get_member_list(room),
    }, exclude_id=client_id)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())

    print(f"[WS] Client connected: {client_id[:8]}")
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
                await ws_send(websocket, {
                    "type": "pong",
                    "clientTime": msg.get("clientTime", 0),
                    "serverTime": int(time.time() * 1000),
                })

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
                await handle_next(client_id)

            elif msg_type == "queue_update":
                await handle_queue_update(client_id, msg)

            elif msg_type == "position_report":
                await handle_position_report(client_id, msg)

            elif msg_type == "chat_message":
                await handle_chat_message(client_id, msg)

            elif msg_type == "song_request":
                await handle_song_request(client_id, msg)

            elif msg_type == "vote_song":
                await handle_vote_song(client_id, msg)

            elif msg_type == "remove_request":
                await handle_remove_request(client_id, msg)

            elif msg_type == "kick_member":
                await handle_kick_member(client_id, msg)

            elif msg_type == "rejoin_room":
                await handle_rejoin_room(client_id, websocket, msg)

            elif msg_type == "share_lyrics":
                await handle_share_lyrics(client_id, msg)

            elif msg_type == "leave":
                await handle_leave(client_id)

    except WebSocketDisconnect:
        print(f"[WS] Client disconnected: {client_id[:8]}")
        await handle_disconnect(client_id)
    except Exception as e:
        print(f"[WS] Error for {client_id[:8]}: {e}")
        await handle_disconnect(client_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
