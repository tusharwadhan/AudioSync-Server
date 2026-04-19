"""
Standalone SABR branch test server.
Deploy this temporarily on Render to test if SABR protocol works from datacenter IP.

Build command:
  pip install "https://github.com/bashonly/yt-dlp/archive/refs/heads/feat/youtube/sabr.zip" fastapi uvicorn protobug

Start command:
  uvicorn test_sabr:app --host 0.0.0.0 --port $PORT
"""

from fastapi import FastAPI
import yt_dlp
import time
import json

app = FastAPI()

TEST_VIDEOS = [
    "JGwWNGJdvx8",  # Ed Sheeran
    "aJOTlE1K90k",  # AP Dhillon
    "vGJTaP6anOU",  # Arijit Singh
    "lp-EO5I60KA",  # Sidhu Moose Wala
    "dQw4w9WgXcQ",  # Rick Astley
]


def test_extraction(video_id: str, opts: dict, label: str) -> dict:
    """Try extracting audio with given opts, return result dict."""
    start = time.time()
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}", download=False
            )

        if not info:
            return {"label": label, "video_id": video_id, "success": False, "error": "No info returned", "time": round(time.time() - start, 2)}

        # Check for audio URL
        audio_url = info.get("url")
        if not audio_url and "formats" in info:
            for fmt in reversed(info["formats"]):
                if fmt.get("acodec") != "none" and fmt.get("url"):
                    audio_url = fmt["url"]
                    break

        # List available formats
        formats = []
        if "formats" in info:
            for fmt in info["formats"]:
                formats.append({
                    "id": fmt.get("format_id"),
                    "ext": fmt.get("ext"),
                    "acodec": fmt.get("acodec"),
                    "vcodec": fmt.get("vcodec"),
                    "protocol": fmt.get("protocol"),
                    "has_url": bool(fmt.get("url")),
                })

        return {
            "label": label,
            "video_id": video_id,
            "success": bool(audio_url),
            "title": info.get("title"),
            "audio_url_preview": audio_url[:100] + "..." if audio_url else None,
            "format_count": len(formats),
            "formats": formats[:20],
            "time": round(time.time() - start, 2),
        }
    except Exception as e:
        return {"label": label, "video_id": video_id, "success": False, "error": str(e), "time": round(time.time() - start, 2)}


@app.get("/")
def index():
    return {
        "message": "SABR branch test server",
        "yt_dlp_version": yt_dlp.version.__version__,
        "endpoints": {
            "/test/{video_id}": "Test all methods on one video",
            "/test-all": "Test all methods on all sample videos",
            "/health": "Health check",
        }
    }


@app.get("/health")
def health():
    return {"status": "ok", "yt_dlp_version": yt_dlp.version.__version__}


@app.get("/test/{video_id}")
def test_video(video_id: str):
    results = []

    # Test 1: SABR format with web client
    results.append(test_extraction(video_id, {
        "format": "ba[protocol=sabr]/ba/b",
        "quiet": True,
        "no_warnings": False,
        "skip_download": True,
        "extractor_args": {
            "youtube": {
                "formats": ["duplicate"],
                "player_client": ["web"],
            }
        },
    }, "sabr_web"))

    # Test 2: SABR format with default client
    results.append(test_extraction(video_id, {
        "format": "ba[protocol=sabr]/ba/b",
        "quiet": True,
        "no_warnings": False,
        "skip_download": True,
        "extractor_args": {
            "youtube": {
                "formats": ["duplicate"],
                "player_client": ["default", "-android_sdkless"],
            }
        },
    }, "sabr_default"))

    # Test 3: Regular format with android client (format 18 fallback)
    results.append(test_extraction(video_id, {
        "format": "18/ba/b",
        "quiet": True,
        "no_warnings": False,
        "skip_download": True,
        "extractor_args": {
            "youtube": {
                "player_client": ["android"],
            }
        },
    }, "android_fmt18"))

    # Test 4: No format restriction, just list what's available
    results.append(test_extraction(video_id, {
        "quiet": True,
        "no_warnings": False,
        "skip_download": True,
        "listformats": False,
        "extractor_args": {
            "youtube": {
                "formats": ["duplicate"],
                "player_client": ["default"],
            }
        },
    }, "list_all_formats"))

    summary = {
        "video_id": video_id,
        "any_success": any(r["success"] for r in results),
        "results": results,
    }
    return summary


@app.get("/test-all")
def test_all():
    all_results = {}
    for vid in TEST_VIDEOS:
        all_results[vid] = test_video(vid)
    return all_results
