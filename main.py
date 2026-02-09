from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import yt_dlp
import time
import asyncio
import httpx
import os
import threading

app = FastAPI(title="AudioSync API")

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


# App update configuration - modify these values to control updates
APP_UPDATE_CONFIG = {
    "latestVersion": "1.0",
    "latestVersionCode": 1,
    "apkUrl": "https://github.com/user/audiosync/releases/download/v1.0/audiosync.apk",
    "releaseNotes": "Initial release",
    # List of version codes that MUST update (mandatory)
    "mandatoryBelow": 1,  # All versions below this must update
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


@app.get("/audio/{video_id}", response_model=AudioResponse)
async def get_audio(video_id: str):
    """
    Extract audio URL for a YouTube video.
    Tries Piped first (fast), falls back to yt-dlp (reliable).
    """
    start = time.time()
    print(f"[/audio] Request: {video_id}")

    # Check cache first
    cached = get_cached(video_id)
    if cached:
        print(f"[/audio] {video_id} → CACHE HIT ({time.time()-start:.2f}s)")
        return AudioResponse(**cached)

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
        print(f"[/audio] {video_id} → PIPED ({time.time()-start:.2f}s)")
        return AudioResponse(**result)

    # Fallback to yt-dlp (slow but reliable)
    t1 = time.time()
    ytdlp_result = await get_audio_ytdlp(video_id)
    print(f"[/audio] {video_id} → yt-dlp extraction: {time.time()-t1:.2f}s")

    if ytdlp_result.success:
        # Cache the result
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

    print(f"[/audio] {video_id} → DONE ({time.time()-start:.2f}s total)")
    return ytdlp_result


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
