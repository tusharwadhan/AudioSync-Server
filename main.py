from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import yt_dlp
import time
import os
import tempfile

app = FastAPI(title="AudioSync API")

# Setup cookies file from environment variable (for YouTube bot detection bypass)
COOKIES_FILE = None
if os.environ.get("YOUTUBE_COOKIES"):
    # Write cookies to a temp file
    cookies_content = os.environ.get("YOUTUBE_COOKIES", "")
    fd, COOKIES_FILE = tempfile.mkstemp(suffix=".txt", prefix="yt_cookies_")
    with os.fdopen(fd, 'w') as f:
        f.write(cookies_content)
    print(f"Cookies file created at {COOKIES_FILE}")

# Allow all origins for mobile app access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AudioResponse(BaseModel):
    success: bool
    videoId: str
    url: Optional[str] = None
    title: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    uploader: Optional[str] = None
    error: Optional[str] = None


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


# Simple in-memory cache (for Render free tier)
_cache: dict = {}
CACHE_TTL = 3600  # 1 hour (URLs valid for ~6 hours)


def get_cached(video_id: str) -> Optional[dict]:
    """Get cached result if still valid"""
    if video_id in _cache:
        entry = _cache[video_id]
        if time.time() - entry["timestamp"] < CACHE_TTL:
            return entry["data"]
        del _cache[video_id]
    return None


def set_cache(video_id: str, data: dict):
    """Cache result"""
    _cache[video_id] = {"data": data, "timestamp": time.time()}


@app.get("/")
async def root():
    return {"status": "ok", "message": "AudioSync API"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/audio/{video_id}", response_model=AudioResponse)
async def get_audio(video_id: str):
    """Extract audio URL for a YouTube video"""

    # Check cache first
    cached = get_cached(video_id)
    if cached:
        return AudioResponse(**cached)

    # yt-dlp options - use web client and permissive format
    ydl_opts = {
        "format": "ba/b/w",  # best audio, or best, or worst (very permissive)
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extractor_args": {
            "youtube": {
                "player_client": ["web"],  # Use web client
            }
        },
    }

    # Add cookies if available
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        ydl_opts["cookiefile"] = COOKIES_FILE

    try:
        url = f"https://www.youtube.com/watch?v={video_id}"

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

            if not info:
                return AudioResponse(
                    success=False,
                    videoId=video_id,
                    error="Failed to extract video info"
                )

            # Get best audio URL from available formats
            audio_url = None
            formats = info.get("formats", [])

            if formats:
                # First try: audio-only formats (m4a, webm audio)
                audio_only = [
                    f for f in formats
                    if f.get("acodec") != "none"
                    and (f.get("vcodec") == "none" or f.get("vcodec") is None)
                    and f.get("url")
                ]
                if audio_only:
                    # Sort by audio bitrate (highest first)
                    audio_only.sort(key=lambda x: x.get("abr") or x.get("tbr") or 0, reverse=True)
                    audio_url = audio_only[0]["url"]

                # Second try: any format with audio (including video+audio)
                if not audio_url:
                    with_audio = [
                        f for f in formats
                        if f.get("acodec") != "none" and f.get("url")
                    ]
                    if with_audio:
                        # Prefer formats with lower video quality (smaller file, audio matters)
                        with_audio.sort(key=lambda x: x.get("tbr") or 0)
                        audio_url = with_audio[0]["url"]

            # Fallback to direct URL if available
            if not audio_url:
                audio_url = info.get("url")

            if not audio_url:
                return AudioResponse(
                    success=False,
                    videoId=video_id,
                    error="No audio URL found"
                )

            result = {
                "success": True,
                "videoId": video_id,
                "url": audio_url,
                "title": info.get("title", "Unknown"),
                "duration": info.get("duration", 0),
                "thumbnail": info.get("thumbnail", ""),
                "uploader": info.get("uploader", "Unknown"),
            }

            # Cache the result
            set_cache(video_id, result)

            return AudioResponse(**result)

    except Exception as e:
        return AudioResponse(
            success=False,
            videoId=video_id,
            error=str(e)
        )


@app.get("/search", response_model=SearchResponse)
async def search(q: str, limit: int = 10):
    """Search YouTube videos using yt-dlp"""

    if not q or len(q.strip()) == 0:
        return SearchResponse(
            success=False,
            query=q,
            error="Query cannot be empty"
        )

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,  # Don't download, just get metadata
        "skip_download": True,
    }

    # Add cookies if available
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        ydl_opts["cookiefile"] = COOKIES_FILE

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
