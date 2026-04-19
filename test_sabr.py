"""
Standalone SABR branch test server.
Deploy this temporarily on Render to test if SABR protocol works from datacenter IP.

Build command:
  pip install "https://github.com/bashonly/yt-dlp/archive/refs/heads/feat/youtube/sabr.zip" fastapi uvicorn protobug

Start command:
  uvicorn test_sabr:app --host 0.0.0.0 --port $PORT
"""

from fastapi import FastAPI, Request
import yt_dlp
import time
import json
import os

app = FastAPI()

# Cookie file path
COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")

def _cookie_opts():
    if os.path.isfile(COOKIE_FILE):
        return {"cookiefile": COOKIE_FILE}
    return {}

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
    has_cookies = os.path.isfile(COOKIE_FILE)
    cookie_size = os.path.getsize(COOKIE_FILE) if has_cookies else 0
    return {
        "message": "SABR branch test server (with cookies support)",
        "yt_dlp_version": yt_dlp.version.__version__,
        "cookies": {"exists": has_cookies, "size_kb": round(cookie_size / 1024, 1)},
        "endpoints": {
            "/test/{video_id}": "Test all methods on one video",
            "/test-all": "Test all methods on all sample videos",
            "/upload-cookies": "POST - paste cookies.txt content as JSON {\"content\": \"...\"}",
            "/health": "Health check",
        }
    }


@app.get("/health")
def health():
    return {"status": "ok", "yt_dlp_version": yt_dlp.version.__version__, "cookies": os.path.isfile(COOKIE_FILE)}


# Cookie upload
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.post("/upload-cookies")
async def upload_cookies(request: Request):
    body = await request.json()
    content = body.get("content", "").strip()
    if not content:
        return {"success": False, "error": "Empty content"}
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        f.write(content + "\n")
    return {"success": True, "size_kb": round(os.path.getsize(COOKIE_FILE) / 1024, 1)}


@app.get("/test/{video_id}")
def test_video(video_id: str):
    cookies = _cookie_opts()
    results = []

    # Test 1: SABR + cookies + web client
    opts1 = {
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
    }
    opts1.update(cookies)
    results.append(test_extraction(video_id, opts1, "sabr_web_cookies"))

    # Test 2: SABR + cookies + default client
    opts2 = {
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
    }
    opts2.update(cookies)
    results.append(test_extraction(video_id, opts2, "sabr_default_cookies"))

    # Test 3: Regular ba/b + cookies + default client (no SABR filter)
    opts3 = {
        "format": "ba/b",
        "quiet": True,
        "no_warnings": False,
        "skip_download": True,
        "extractor_args": {
            "youtube": {
                "formats": ["duplicate"],
                "player_client": ["default", "-android_sdkless"],
            }
        },
    }
    opts3.update(cookies)
    results.append(test_extraction(video_id, opts3, "regular_default_cookies"))

    # Test 4: SABR + cookies + android_vr client
    opts4 = {
        "format": "ba[protocol=sabr]/ba/b",
        "quiet": True,
        "no_warnings": False,
        "skip_download": True,
        "extractor_args": {
            "youtube": {
                "formats": ["duplicate"],
                "player_client": ["android_vr"],
            }
        },
    }
    opts4.update(cookies)
    results.append(test_extraction(video_id, opts4, "sabr_android_vr_cookies"))

    summary = {
        "video_id": video_id,
        "cookies_loaded": bool(cookies),
        "any_success": any(r["success"] for r in results),
        "results": results,
    }
    return summary


@app.get("/test-all")
def test_all():
    all_results = {}
    for vid in TEST_VIDEOS:
        all_results[vid] = test_video(vid)
    return {
        "cookies_loaded": bool(_cookie_opts()),
        "any_success": any(r["any_success"] for r in all_results.values()),
        "results": all_results,
    }
