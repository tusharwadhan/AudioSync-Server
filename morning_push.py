"""
Morning push — the Zomato-style daily notification, personalized from
user_listen_events.

Everything operational is runtime-editable through the admin endpoints
(X-API-Key + X-Admin-Secret, same guard as the other admin routes):

    GET  /api/v1/admin/morning-push/status        scheduler + counts + today
    GET  /api/v1/admin/morning-push/config        live schedule/audience/llm cfg
    POST /api/v1/admin/morning-push/config        merge-update any subset of it
    GET  /api/v1/admin/morning-push/lines         live template pool
    POST /api/v1/admin/morning-push/lines         replace the pool ({"lines": {...}})
    GET  /api/v1/admin/morning-push/dry-run       render today's batch, send nothing
                                                  (?llm=true to render via Groq)
    POST /api/v1/admin/morning-push/generate      render + store a DRAFT for review
                                                  (body {"llm": true} for Groq lines)
    GET  /api/v1/admin/morning-push/drafts        pending drafts
    POST /api/v1/admin/morning-push/send          {"draftId": N} send a reviewed draft
                                                  or {"direct": true} manual trigger now
    POST /api/v1/admin/morning-push/discard       {"draftId": N}

App-side (ships 5.18.4):
    POST /api/v1/user/fcm-token   (Bearer Firebase ID token)
         {"token": "...", "device": "...", "morningPush": true|false}

Schedule config shape (all times IST):
    {"enabled": false, "defaultTime": "08:00",
     "days": {"15": {"enabled": false}, "20": {"time": "07:30"}},
     "audience": {"mode": "all"|"emails", "emails": [...], "exclude": [...]},
     "llm": {"enabled": false, "context": "", "model": "llama-3.3-70b-versatile"},
     "requireApproval": false, "windowDays": 30, "graceMinutes": 120}

With requireApproval=true the scheduler only GENERATES a draft at send time
and waits for an admin /send — nothing goes out unreviewed. Devotional picks
never use the LLM; they always come from the curated respectful pool.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from firebase_admin import messaging
from sqlalchemy import text as sql_text

from auth import AuthedUser, get_current_user
from db import get_session, try_session_factory

router = APIRouter(tags=["morning-push"])
# Mounted at app root (NOT /api/v1): FCM's image fetcher can't send the
# X-API-Key header, so banner URLs must be public like /share and /app.
public_router = APIRouter(tags=["morning-push-public"])

IST = ZoneInfo("Asia/Kolkata")
# Public origin for banner URLs baked into FCM messages. Render sets
# RENDER_EXTERNAL_URL automatically.
PUBLIC_ORIGIN = (os.getenv("RENDER_EXTERNAL_URL", "")
                 or "https://audiosync-server.onrender.com").rstrip("/")
# Same env var main.py uses — NOT "ADMIN_SECRET"; that name is unset on
# Render, and `if not ADMIN_SECRET` would then 403 every admin call even
# with the correct header (exactly what happened on first deploy).
ADMIN_SECRET = os.getenv("SYNCAURA_ADMIN_SECRET", "") or os.getenv("ADMIN_SECRET", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

_FCM_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4)

CONFIG_KEY = "morning_push_config"
LINES_KEY = "morning_push_lines"

DEFAULT_CONFIG: dict = {
    "enabled": False,          # master switch — nothing fires until flipped
    "defaultTime": "08:00",    # IST
    "days": {},                # "1".."31" → {"enabled": bool, "time": "HH:MM"}
    "audience": {"mode": "all", "emails": [], "exclude": []},
    "llm": {"enabled": False, "context": "",
            "model": "openai/gpt-oss-120b"},
    "requireApproval": False,
    # Per-user hand-picked song: {email(lower): {song, artist, videoId,
    # image}}. Set/cleared from the console; null clears on merge.
    "overrides": {},
    # Per-user note fed to Groq alongside the global style context
    # ("she loves bhajans, keep it respectful"). {email(lower): "text"}.
    "userContext": {},
    "windowDays": 30,
    "graceMinutes": 120,       # late-boot catch-up window after send time
    # Android notification channel. MUST stay "" until an app release has
    # created the channel — Android 8+ silently DROPS notifications aimed
    # at a channel the app never created. "" = FCM SDK fallback channel,
    # which displays on every current install.
    "channelId": "",
}

# Factory-default line pool. Seeded into app_config on first read; the live
# Postgres copy is what renders — edit via POST /admin/morning-push/lines.
DEFAULT_LINES: dict = {
    "morning_ritual": {
        "regular": [
            {"title": "Good morning, {name} ☀️",
             "body": "{song} — {artist} is waiting right where your mornings start"},
            {"title": "Subah ho gayi 🌅",
             "body": "{song} laga ke din shuru karein?"},
            {"title": "Your morning track is ready 🎧",
             "body": "{song} — {artist}, same time as always"},
            {"title": "Rise and play ▶️",
             "body": "Mornings just sound better with {song}"},
        ],
        "devotional": [
            {"title": "शुभ प्रभात 🙏",
             "body": "आपका सुबह का भजन तैयार है — {song}"},
            {"title": "Shubh prabhat 🪔",
             "body": "Start your morning with {song}"},
            {"title": "मंगलमय प्रभात 🙏",
             "body": "{song} के साथ दिन की शुभ शुरुआत करें"},
        ],
    },
    "overall_top": {
        "regular": [
            {"title": "Morning, {name} 👋",
             "body": "Your current favourite {song} could use a morning run"},
            {"title": "Aaj ka pehla gaana? 🎵",
             "body": "{song} — {artist}, obviously"},
            {"title": "Start where you left off 🎧",
             "body": "{song} has been your soundtrack lately"},
        ],
        "devotional": [
            {"title": "शुभ प्रभात 🙏",
             "body": "{song} — आपकी पसंदीदा, सुबह के लिए"},
        ],
    },
    "generic": {
        "trending": [
            {"title": "Good morning ☀️",
             "body": "Everyone's playing {song} this morning — join in?"},
            {"title": "Aaj subah ka hit 🔥",
             "body": "{song} — {artist} is trending right now"},
        ],
        "plain": [
            {"title": "Good morning ☀️",
             "body": "Din ki shuruaat ek acche gaane se? Your music is waiting 🎧"},
            {"title": "Morning, {name} 🌅",
             "body": "Pick a song, set the mood for the day"},
        ],
    },
}

DEVOTIONAL_RE = re.compile(
    r"bhajan|aarti|chalisa|hanuman|ganpati|ganesh|krishna|radha|shiv|bholenath"
    r"|mata rani|durga|devi |waheguru|gurbani|shabad|kirtan|mantra|vandana"
    r"|bhakti|prabhu|yeshu|masih|worship|hallelujah"
    r"|भजन|आरती|चालीसा|हनुमान|गणपति|गणेश|कृष्ण|राधा|शिव|दुर्गा|देवी|मंत्र|प्रभु|येसु|मसीह",
    re.IGNORECASE,
)

# Bracketed junk YouTube titles carry: (Official Video), [4K Remaster], …
_BRACKET_JUNK = re.compile(
    r"[(\[][^)\]]*(official|video|music|lyric|lyrical|audio|visuali[sz]er|full"
    r"|song|4k|hd|remaster|version|feat|ft\.|prod)[^)\]]*[)\]]",
    re.IGNORECASE,
)


def _log(msg: str) -> None:
    print(f"[MorningPush] {msg}", flush=True)


def _require_admin(request: Request) -> None:
    if not ADMIN_SECRET or request.headers.get("X-Admin-Secret") != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")


# ── title / artist cleanup ───────────────────────────────────────────────

def clean_artist(uploader: str | None) -> str:
    a = (uploader or "").strip()
    a = re.sub(r"\s+and\s+\d+\s+more$", "", a, flags=re.IGNORECASE)
    a = re.sub(r"\s*-\s*Topic$", "", a, flags=re.IGNORECASE)
    return a.strip() or "your artist"


_TITLE_JUNK_SEG = re.compile(
    r"latest|new (punjabi|hindi|haryanvi|song)|official|full (video|song|album)"
    r"|\b20\d\d\b|lyrical|whatsapp status|trending",
    re.IGNORECASE,
)

_TAIL_JUNK_WORD = re.compile(
    r"\s*\b(latest|new|official|full|video|song|songs|hit|hits|punjabi|hindi"
    r"|haryanvi|lyrical|hd|4k|audio|music|20\d\d|no\.? ?\d+)\s*$",
    re.IGNORECASE,
)


def clean_title(raw: str | None, uploader: str | None) -> str:
    """Dig the song name out of a YouTube title. Walks the pipe-separated
    segments, strips bracket junk + artist halves of 'Artist - Song' /
    'Song : Artist' shapes, and takes the first segment that survives and
    isn't marketing junk ('Latest Punjabi Songs 2025')."""
    original = (raw or "").strip()
    t = _BRACKET_JUNK.sub(" ", original)
    up = (uploader or "").lower()
    artist = clean_artist(uploader)

    def strip_artist(seg: str) -> str:
        if len(artist) >= 4 and artist.lower() != "your artist":
            seg = re.sub(r"(\bby\s+)?" + re.escape(artist), " ", seg,
                         flags=re.IGNORECASE)
        seg = re.sub(r"\bby\s*$", "", seg, flags=re.IGNORECASE)
        return re.sub(r"\s{2,}", " ", seg).strip(" -–—:|,")

    def trim_tail(seg: str) -> str:
        # Peel marketing words off the END ('… Full Video Song' → '…').
        prev = None
        while prev != seg:
            prev = seg
            seg = _TAIL_JUNK_WORD.sub("", seg).strip(" -–—:|,")
        return seg

    candidates = []
    for seg in t.split("|"):
        seg = seg.strip()
        if not seg:
            continue
        # 'Artist - Song' / 'Artist : Song' → song half; and the mirror,
        # 'Song : Artist' → song half.
        for sep in (" - ", " : "):
            if sep in seg:
                left, right = seg.split(sep, 1)
                l, r = left.strip().lower(), right.strip().lower()
                if up and (l in up or up in l):
                    seg = right
                elif up and (r in up or up in r):
                    seg = left
                elif sep == " : " and len(left.split()) <= 3:
                    # 'Babbu Maan : Song…' — a short colon-prefix is almost
                    # always the artist even when the uploader is a label.
                    seg = right
                break
        seg = trim_tail(strip_artist(seg))
        if len(seg) >= 2:
            candidates.append(seg)

    best = next((c for c in candidates if not _TITLE_JUNK_SEG.search(c)), None)
    t = best or (candidates[0] if candidates else "")
    if len(t) < 2:
        t = original[:60]
    return t[:60].strip()


def is_devotional(title: str | None, uploader: str | None) -> bool:
    return bool(DEVOTIONAL_RE.search(f"{title or ''} {uploader or ''}"))


def _stable_idx(seed: str, n: int) -> int:
    if n <= 0:
        return 0
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % n


# ── notification banner (Zomato-style wide card) ─────────────────────────
# The raw YouTube thumbnail looks like a video frame in the tray. This
# composes a 1200x600 banner: the album art blur-filled across the whole
# card, with the sharp art floating on it — rendered once per videoId and
# cached in tmp. On any failure the endpoint redirects to the raw
# thumbnail so a notification never ships without an image.

_BANNER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,20}$")


def _compose_banner(art_bytes: bytes) -> bytes:
    """Zomato-style FULL-BLEED card, never a framed thumbnail: wide video
    frames cover-crop edge-to-edge; square album art fills the full height
    with bright blurred wings of the same artwork — no margins, no rounded
    inner corners, no dark padding."""
    from io import BytesIO

    from PIL import Image, ImageEnhance, ImageFilter

    W, H = 1200, 600
    art = Image.open(BytesIO(art_bytes)).convert("RGB")

    # YouTube pads square album art to 16:9 with black bars — trim any
    # near-black margins so we work with the real artwork.
    gray = art.convert("L")
    bbox = gray.point(lambda p: 255 if p > 16 else 0).getbbox()
    if bbox:
        art = art.crop(bbox)

    def cover(img, w, h):
        s = max(w / img.width, h / img.height)
        img = img.resize((int(img.width * s) + 1, int(img.height * s) + 1))
        return img.crop(((img.width - w) // 2, (img.height - h) // 2,
                         (img.width - w) // 2 + w, (img.height - h) // 2 + h))

    if art.width / art.height >= 1.4:
        # Cinematic video frame — full-bleed crop with a vivid grade so the
        # card pops like food photography instead of a dim paused frame.
        canvas = cover(art, W, H)
        canvas = ImageEnhance.Color(canvas).enhance(1.28)
        canvas = ImageEnhance.Brightness(canvas).enhance(1.07)
        canvas = ImageEnhance.Contrast(canvas).enhance(1.05)
    else:
        # Square-ish album art: full-height sharp art, bright colorful
        # blur of the same art filling the wings seamlessly.
        canvas = cover(art, W, H).filter(ImageFilter.GaussianBlur(28))
        canvas = ImageEnhance.Brightness(canvas).enhance(0.85)
        fw = int(art.width * (H / art.height))
        fg = art.resize((min(fw, W), H))
        canvas.paste(fg, ((W - fg.width) // 2, 0))

    out = BytesIO()
    canvas.save(out, "JPEG", quality=84)
    return out.getvalue()


async def _banner_bytes(video_id: str) -> bytes | None:
    import tempfile

    # v3 cache key — vivid grade added, old cached banners must not serve.
    cache = os.path.join(tempfile.gettempdir(), f"mpb3_{video_id}.jpg")
    if os.path.exists(cache):
        with open(cache, "rb") as f:
            return f.read()
    art = None
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for variant in ("maxresdefault", "hq720", "hqdefault"):
            try:
                r = await client.get(
                    f"https://i.ytimg.com/vi/{video_id}/{variant}.jpg")
                if r.status_code == 200 and len(r.content) > 2000:
                    art = r.content
                    break
            except Exception:
                continue
    if art is None:
        return None
    loop = asyncio.get_running_loop()
    try:
        data = await loop.run_in_executor(None, _compose_banner, art)
    except Exception as e:
        _log(f"banner compose failed for {video_id}: {type(e).__name__}: {e}")
        return None
    try:
        with open(cache, "wb") as f:
            f.write(data)
    except Exception:
        pass
    return data


@public_router.get("/mp-banner/{video_id}.jpg")
async def mp_banner(video_id: str):
    from fastapi.responses import RedirectResponse, Response

    if not _BANNER_ID_RE.match(video_id):
        raise HTTPException(status_code=400, detail="bad id")
    data = await _banner_bytes(video_id)
    if data is None:
        return RedirectResponse(
            f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg")
    return Response(content=data, media_type="image/jpeg",
                    headers={"Cache-Control": "public, max-age=86400"})


def banner_url(video_id: str | None) -> str:
    if video_id and _BANNER_ID_RE.match(video_id):
        return f"{PUBLIC_ORIGIN}/mp-banner/{video_id}.jpg"
    return ""


# ── app_config storage ───────────────────────────────────────────────────

async def _kv_get(session, key: str) -> dict | None:
    row = (
        await session.execute(
            sql_text("SELECT value FROM app_config WHERE key = :k"), {"k": key}
        )
    ).scalar()
    if not row:
        return None
    try:
        return json.loads(row)
    except Exception:
        return None


async def _kv_set(session, key: str, value: dict) -> None:
    await session.execute(
        sql_text(
            "INSERT INTO app_config (key, value, updated_at) "
            "VALUES (:k, :v, now()) "
            "ON CONFLICT (key) DO UPDATE SET value = :v, updated_at = now()"
        ),
        {"k": key, "v": json.dumps(value, ensure_ascii=False)},
    )
    await session.commit()


def _merge_config(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = {**out[k], **v}
        else:
            out[k] = v
    return out


async def load_config(session) -> dict:
    stored = await _kv_get(session, CONFIG_KEY)
    return _merge_config(DEFAULT_CONFIG, stored or {})


async def load_lines(session) -> dict:
    stored = await _kv_get(session, LINES_KEY)
    if stored:
        return stored
    await _kv_set(session, LINES_KEY, DEFAULT_LINES)
    return DEFAULT_LINES


# ── picker ───────────────────────────────────────────────────────────────

async def _audience(session, cfg: dict) -> list[dict]:
    # Reachable = has the social-path token (users.fcm_token — registered on
    # every signed-in app open since the DM-push feature) OR a row in the new
    # authed store (5.18.4+, carries the in-app opt-out). Legacy users
    # opt out via audience.exclude until their app has the toggle.
    rows = (
        await session.execute(
            sql_text(
                "SELECT DISTINCT u.id, u.email, u.display_name FROM users u "
                "WHERE u.fcm_token IS NOT NULL "
                "   OR EXISTS (SELECT 1 FROM user_fcm_tokens t "
                "              WHERE t.user_id = u.id AND t.enabled)"
            )
        )
    ).all()
    aud = cfg.get("audience") or {}
    emails = {e.lower() for e in aud.get("emails") or []}
    exclude = {e.lower() for e in aud.get("exclude") or []}
    out = []
    for r in rows:
        em = (r.email or "").lower()
        if em in exclude:
            continue
        if aud.get("mode") == "emails" and em not in emails:
            continue
        # First name, normalized ("SRISHTY BHALLA" → "Srishty",
        # "priya rani" → "Priya") so greetings read naturally.
        first = (r.display_name or "").split(" ")[0]
        out.append({"uid": r.id, "email": r.email or "",
                    "name": (first.capitalize() if first.isupper() or first.islower()
                             else first) or "friend"})
    return out


async def _track_stats(session, days: int) -> dict[str, list]:
    rows = (
        await session.execute(
            sql_text(
                """
                SELECT user_id, video_id,
                       MAX(title) AS title, MAX(uploader) AS uploader,
                       MAX(thumbnail) AS thumbnail, COUNT(*) AS plays,
                       COUNT(*) FILTER (
                           WHERE EXTRACT(HOUR FROM played_at AT TIME ZONE 'Asia/Kolkata')
                                 BETWEEN 6 AND 10
                       ) AS morning_plays
                FROM user_listen_events
                WHERE played_at >= now() - make_interval(days => :days)
                GROUP BY user_id, video_id
                """
            ),
            {"days": days},
        )
    ).all()
    by_user: dict[str, list] = {}
    for r in rows:
        by_user.setdefault(r.user_id, []).append(r)
    return by_user


def _merge_tracks(rows: list) -> list[dict]:
    """Collapse the same song under several videoIds (official video vs
    audio) by cleaned (title, artist)."""
    merged: dict[tuple, dict] = {}
    for r in rows:
        song = clean_title(r.title, r.uploader)
        artist = clean_artist(r.uploader)
        key = (song.lower(), artist.lower())
        m = merged.get(key)
        if m is None:
            merged[key] = {
                "song": song, "artist": artist, "videoId": r.video_id,
                "thumbnail": r.thumbnail or "", "plays": r.plays,
                "morning": r.morning_plays, "bestPlays": r.plays,
                "listeners": getattr(r, "listeners", 0),
                "devotional": is_devotional(r.title, r.uploader),
            }
        else:
            m["plays"] += r.plays
            m["morning"] += r.morning_plays
            # Same user may sit in both videoId variants — max, not sum.
            m["listeners"] = max(m["listeners"], getattr(r, "listeners", 0))
            if r.plays > m["bestPlays"]:
                m["bestPlays"] = r.plays
                m["videoId"] = r.video_id
                m["thumbnail"] = r.thumbnail or m["thumbnail"]
    return list(merged.values())


async def _trending(session, days: int) -> dict | None:
    """Trending = most DISTINCT listeners, then plays. Raw play counts let
    one superfan looping a song make it 'trending' for everyone (11K was
    the global pick off a single user's 28 plays)."""
    rows = (
        await session.execute(
            sql_text(
                """
                SELECT video_id, MAX(title) AS title, MAX(uploader) AS uploader,
                       MAX(thumbnail) AS thumbnail, COUNT(*) AS plays,
                       COUNT(DISTINCT user_id) AS listeners,
                       0 AS morning_plays
                FROM user_listen_events
                WHERE played_at >= now() - make_interval(days => :days)
                GROUP BY video_id
                ORDER BY COUNT(DISTINCT user_id) DESC, COUNT(*) DESC
                LIMIT 8
                """
            ),
            {"days": days},
        )
    ).all()
    merged = _merge_tracks(rows)
    merged.sort(key=lambda t: (-t.get("listeners", 0), -t["plays"]))
    return merged[0] if merged else None


async def _yesterday_video(session, uid: str, today_ist: str) -> str | None:
    yday = (datetime.strptime(today_ist, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    return (
        await session.execute(
            sql_text(
                "SELECT video_id FROM morning_push_log "
                "WHERE user_id = :u AND sent_date = :d "
                "ORDER BY id DESC LIMIT 1"
            ),
            {"u": uid, "d": yday},
        )
    ).scalar()


def _pick_for_user(tracks: list[dict], yesterday: str | None,
                   uid: str, date_str: str) -> tuple[str, dict | None]:
    """Returns (bucket, track|None). Rotates among the top picks and avoids
    repeating yesterday's song when an alternative exists."""
    morning = sorted([t for t in tracks if t["morning"] >= 2],
                     key=lambda t: -t["morning"])[:3]
    overall = sorted([t for t in tracks if t["plays"] >= 2],
                     key=lambda t: -t["plays"])[:3]

    def rotate(cands: list[dict]) -> dict:
        i = _stable_idx(f"{uid}:{date_str}:song", len(cands))
        pick = cands[i]
        if yesterday and pick["videoId"] == yesterday and len(cands) > 1:
            pick = cands[(i + 1) % len(cands)]
        return pick

    if morning:
        return "morning_ritual", rotate(morning)
    if overall:
        return "overall_top", rotate(overall)
    return "generic", None


def _render_line(lines: dict, bucket: str, tone: str,
                 uid: str, date_str: str, slots: dict) -> dict:
    pool = (lines.get(bucket) or {}).get(tone) or []
    if not pool:  # fall back through tones/buckets rather than crash
        pool = (lines.get(bucket) or {}).get("regular") or \
               (lines.get("generic") or {}).get("plain") or \
               [{"title": "Good morning ☀️", "body": "Your music is waiting 🎧"}]
    line = pool[_stable_idx(f"{uid}:{date_str}:line", len(pool))]
    safe = {"song": slots.get("song", ""), "artist": slots.get("artist", ""),
            "name": slots.get("name", "friend")}
    try:
        return {"title": line["title"].format(**safe)[:64],
                "body": line["body"].format(**safe)[:120]}
    except Exception:
        return {"title": "Good morning ☀️", "body": "Your music is waiting 🎧"}


# ── Groq line generation ─────────────────────────────────────────────────

# Current Groq catalog default (llama-3.x chat models 404 since ~2026-10).
DEFAULT_GROQ_MODEL = "openai/gpt-oss-120b"


def _parse_line_json(content: str) -> dict | None:
    content = re.sub(r"^```(json)?|```$", "", content.strip(),
                     flags=re.MULTILINE).strip()
    try:
        return json.loads(content)
    except Exception:
        m = re.search(r"\{.*\}", content, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return None


async def _groq_line(cfg_llm: dict, name: str, song: str, artist: str,
                     bucket: str, user_ctx: str = "") -> dict | None:
    if not GROQ_API_KEY:
        return None
    context = (cfg_llm.get("context") or "").strip()
    system = (
        "You write ONE short morning push notification for SyncAura, an Indian "
        "music app. Reply with ONLY a JSON object: "
        '{"title": "...", "body": "..."}. Title max 38 characters, body max 90. '
        "Warm, playful; light Hinglish or simple English. Greet the user by "
        "their first name in the title (e.g. 'Good morning, Tushar ☀️'). "
        "Mention the song naturally. No hashtags, no quotes around the song name."
        + (f" Extra context from the admin: {context}" if context else "")
    )
    user = (
        f"User first name: {name}. This morning's pick: '{song}' by {artist}. "
        f"Bucket: {bucket} (morning_ritual = they play this most mornings; "
        f"overall_top = their recent favourite; generic = trending today; "
        f"custom = hand-picked specially for them today)."
        + (f" Note about this user from the admin: {user_ctx.strip()}"
           if user_ctx.strip() else "")
    )
    model = cfg_llm.get("model") or DEFAULT_GROQ_MODEL
    for attempt_model in dict.fromkeys([model, DEFAULT_GROQ_MODEL]):
        try:
            payload = {
                "model": attempt_model,
                "messages": [{"role": "system", "content": system},
                             {"role": "user", "content": user}],
                "temperature": 0.9,
                # Reasoning models (gpt-oss) spend tokens thinking before
                # the answer; a tight cap starves them into empty content.
                "max_tokens": 400,
            }
            if attempt_model.startswith("openai/gpt-oss"):
                payload["reasoning_effort"] = "low"
            async with httpx.AsyncClient(timeout=25) as client:
                resp = await client.post(
                    GROQ_URL,
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                    json=payload,
                )
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"] or ""
                out = _parse_line_json(content)
                if out:
                    title = str(out.get("title") or "").strip()[:64]
                    body = str(out.get("body") or "").strip()[:120]
                    if title and body:
                        return {"title": title, "body": body}
                _log(f"groq returned unparseable content via {attempt_model}")
        except httpx.HTTPStatusError as e:
            _log(f"groq generation failed ({attempt_model}): "
                 f"{e.response.status_code} — "
                 f"{'retrying with default model' if attempt_model != DEFAULT_GROQ_MODEL else 'giving up'}")
            continue  # 404 = retired model; next loop tries the default
        except Exception as e:
            _log(f"groq generation failed: {type(e).__name__}: {e}")
            break
    return None


# ── batch building + sending ─────────────────────────────────────────────

async def build_batch(session, cfg: dict, use_llm: bool,
                      only_email: str | None = None) -> list[dict]:
    days = int(cfg.get("windowDays") or 30)
    date_str = datetime.now(IST).strftime("%Y-%m-%d")
    lines = await load_lines(session)
    audience = await _audience(session, cfg)
    if only_email:
        audience = [u for u in audience
                    if (u["email"] or "").lower() == only_email.lower()]
    stats = await _track_stats(session, days)
    trending = await _trending(session, days)

    overrides = {k.lower(): v for k, v in (cfg.get("overrides") or {}).items() if v}

    items = []
    for u in audience:
        ov = overrides.get((u["email"] or "").lower())
        if ov and (ov.get("song") or ov.get("videoId")):
            # Admin hand-picked this user's song in the console. The stored
            # title is the raw search result — clean it like any other pick.
            track = {"song": clean_title(ov.get("song"), ov.get("artist")),
                     "artist": clean_artist(ov.get("artist")),
                     "videoId": ov.get("videoId") or "",
                     "thumbnail": ov.get("image") or "",
                     "devotional": is_devotional(ov.get("song"), ov.get("artist"))}
            bucket = "custom"
            tone = "devotional" if track["devotional"] else "regular"
        else:
            tracks = _merge_tracks(stats.get(u["uid"], []))
            yday = await _yesterday_video(session, u["uid"], date_str)
            bucket, track = _pick_for_user(tracks, yday, u["uid"], date_str)

            if bucket == "generic":
                track = trending
                tone = "trending" if track else "plain"
            else:
                tone = "devotional" if track and track["devotional"] else "regular"

        slots = {"name": u["name"],
                 "song": track["song"] if track else "",
                 "artist": track["artist"] if track else ""}

        line = None
        llm_used = False
        # Devotional picks never go through the LLM — curated pool only.
        if use_llm and track and tone not in ("devotional",):
            user_ctx = ((cfg.get("userContext") or {})
                        .get((u["email"] or "").lower()) or "")
            line = await _groq_line(cfg.get("llm") or {}, u["name"],
                                    track["song"], track["artist"], bucket,
                                    user_ctx)
            llm_used = line is not None
        if line is None:
            # Custom picks render from the most personal pool.
            render_bucket = "morning_ritual" if bucket == "custom" else bucket
            line = _render_line(lines, render_bucket, tone, u["uid"], date_str, slots)

        vid = track["videoId"] if track else ""
        items.append({
            "uid": u["uid"], "email": u["email"], "name": u["name"],
            "bucket": bucket, "tone": tone, "llm": llm_used,
            "videoId": vid,
            "song": slots["song"], "artist": slots["artist"],
            # Composed wide banner (Zomato-style card); raw thumbnail only
            # when there's no videoId to compose from.
            "image": banner_url(vid)
                     or ((track.get("thumbnail") or "") if track else ""),
            "title": line["title"], "body": line["body"],
            "date": date_str,
        })
    return items


# App versionCode that ships the custom Zomato-style renderer (5.19.1).
CUSTOM_RENDER_MIN_CODE = 88


def _fcm_message(token: str, item: dict, channel_id: str | None,
                 custom_render: bool = False) -> messaging.Message:
    image = item.get("image") or None
    if image and not image.startswith("https://"):
        image = None
    if custom_render:
        # DATA-ONLY: the app (5.19.1+) draws the notification itself —
        # full-bleed banner with the text overlaid, true Zomato style.
        # A notification block here would double-notify.
        return messaging.Message(
            token=token,
            data={
                "type": "morning_push",
                "title": item["title"],
                "body": item["body"],
                "image": image or "",
                "play_video_id": item.get("videoId") or "",
                "play_title": item.get("song") or "",
                "play_uploader": item.get("artist") or "",
                "play_thumbnail": (
                    f"https://i.ytimg.com/vi/{item['videoId']}/hqdefault.jpg"
                    if item.get("videoId") else ""
                ),
            },
            android=messaging.AndroidConfig(priority="high"),
        )
    android_notif = messaging.AndroidNotification(image=image)
    if channel_id:
        android_notif = messaging.AndroidNotification(
            channel_id=channel_id, image=image,
        )
    return messaging.Message(
        token=token,
        notification=messaging.Notification(
            title=item["title"], body=item["body"], image=image,
        ),
        data={
            "type": "morning_push",
            # Keys match MainActivity's existing play-intent extras
            # (OfflineBackupActivity path), so tapping the notification
            # starts playback with ZERO new routing code — on every app
            # version back to when that path shipped.
            "play_video_id": item.get("videoId") or "",
            "play_title": item.get("song") or "",
            "play_uploader": item.get("artist") or "",
            "play_thumbnail": (
                f"https://i.ytimg.com/vi/{item['videoId']}/hqdefault.jpg"
                if item.get("videoId") else ""
            ),
        },
        android=messaging.AndroidConfig(
            priority="high",
            notification=android_notif,
        ),
    )


async def send_batch(session, items: list[dict], force: bool = False) -> dict:
    """`force` skips the once-per-user-per-day guard — admin testing only;
    the scheduler never passes it."""
    loop = asyncio.get_running_loop()
    cfg = await load_config(session)
    channel_id = (cfg.get("channelId") or "").strip() or None
    # Guard + log on the date the send actually HAPPENS, not the date the
    # draft was generated — a stale draft sent this morning must count as
    # today's push, or the 8am scheduler would double-ping everyone.
    send_date = datetime.now(IST).strftime("%Y-%m-%d")
    sent = failed = skipped = 0
    for item in items:
        # Per-day dedupe at send time too (draft may be approved late).
        if not force:
            already = (
                await session.execute(
                    sql_text(
                        "SELECT 1 FROM morning_push_log "
                        "WHERE user_id = :u AND sent_date = :d AND status = 'sent' "
                        "LIMIT 1"
                    ),
                    {"u": item["uid"], "d": send_date},
                )
            ).scalar()
            if already:
                skipped += 1
                continue

        # New authed store (5.19.0+, carries opt-out + app version) ∪ the
        # social-path token every signed-in install already registers.
        rows = (
            await session.execute(
                sql_text(
                    "SELECT token, app_version_code FROM user_fcm_tokens "
                    "WHERE user_id = :u AND enabled"
                ),
                {"u": item["uid"]},
            )
        ).all()
        tokens: dict[str, int] = {r.token: r.app_version_code for r in rows}
        legacy = (
            await session.execute(
                sql_text("SELECT fcm_token FROM users WHERE id = :u"),
                {"u": item["uid"]},
            )
        ).scalar()
        if legacy and legacy not in tokens:
            tokens[legacy] = 0
        if not tokens:
            skipped += 1
            continue

        ok = 0
        last_err = ""
        for token, app_code in tokens.items():
            try:
                msg = _fcm_message(
                    token, item, channel_id,
                    custom_render=app_code >= CUSTOM_RENDER_MIN_CODE,
                )
                await loop.run_in_executor(_FCM_POOL, messaging.send, msg)
                ok += 1
            except Exception as e:
                last_err = f"{type(e).__name__}"
                dead = isinstance(e, messaging.UnregisteredError) or isinstance(
                    e, (messaging.SenderIdMismatchError, ValueError)
                )
                if dead:
                    await session.execute(
                        sql_text("DELETE FROM user_fcm_tokens WHERE token = :t"),
                        {"t": token},
                    )
                    await session.execute(
                        sql_text(
                            "UPDATE users SET fcm_token = NULL, "
                            "fcm_token_updated_at = NULL "
                            "WHERE id = :u AND fcm_token = :t"
                        ),
                        {"u": item["uid"], "t": token},
                    )

        status = "sent" if ok else f"failed:{last_err or 'no-delivery'}"
        await session.execute(
            sql_text(
                "INSERT INTO morning_push_log "
                "(user_id, sent_date, video_id, title, body, status, created_at) "
                "VALUES (:u, :d, :v, :t, :b, :s, now())"
            ),
            {"u": item["uid"], "d": send_date,
             "v": item.get("videoId") or None,
             "t": item["title"], "b": item["body"], "s": status},
        )
        if ok:
            sent += 1
        else:
            failed += 1
    await session.commit()
    summary = {"sent": sent, "failed": failed, "skipped": skipped,
               "total": len(items)}
    _log(f"batch done: {summary}")
    return summary


# ── scheduler ────────────────────────────────────────────────────────────

_last_action_date: str | None = None  # in-process guard; DB is the real dedupe
_last_error: str = ""


async def _tick() -> None:
    global _last_action_date, _last_error
    factory = try_session_factory()
    if factory is None:
        return
    async with factory() as session:
        cfg = await load_config(session)
        if not cfg.get("enabled"):
            return

        now = datetime.now(IST)
        date_str = now.strftime("%Y-%m-%d")
        if _last_action_date == date_str:
            return

        day_cfg = (cfg.get("days") or {}).get(str(now.day), {})
        if day_cfg.get("enabled") is False:
            return
        time_str = day_cfg.get("time") or cfg.get("defaultTime") or "08:00"
        try:
            hh, mm = (int(x) for x in time_str.split(":"))
        except Exception:
            hh, mm = 8, 0
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        grace = timedelta(minutes=int(cfg.get("graceMinutes") or 120))
        if now < target or now > target + grace:
            return

        if cfg.get("requireApproval"):
            # One draft per day; the admin reviews and /send-s it.
            existing = (
                await session.execute(
                    sql_text(
                        "SELECT 1 FROM morning_push_drafts "
                        "WHERE created_at >= :start LIMIT 1"
                    ),
                    {"start": target.astimezone(ZoneInfo("UTC"))},
                )
            ).scalar()
            if existing:
                _last_action_date = date_str
                return
            items = await build_batch(
                session, cfg, use_llm=bool((cfg.get("llm") or {}).get("enabled"))
            )
            await session.execute(
                sql_text(
                    "INSERT INTO morning_push_drafts (status, payload, created_at) "
                    "VALUES ('pending', :p, now())"
                ),
                {"p": json.dumps(items, ensure_ascii=False)},
            )
            await session.commit()
            _last_action_date = date_str
            _log(f"draft created for {date_str} ({len(items)} users) — awaiting approval")
            return

        # Direct mode: already sent today? (survives process restarts)
        already = (
            await session.execute(
                sql_text(
                    "SELECT 1 FROM morning_push_log WHERE sent_date = :d LIMIT 1"
                ),
                {"d": date_str},
            )
        ).scalar()
        if already:
            _last_action_date = date_str
            return

        items = await build_batch(
            session, cfg, use_llm=bool((cfg.get("llm") or {}).get("enabled"))
        )
        await send_batch(session, items)
        _last_action_date = date_str


async def scheduler_loop() -> None:
    _log("scheduler started (30s tick, IST)")
    global _last_error
    while True:
        try:
            await _tick()
            _last_error = ""
        except Exception as e:
            _last_error = f"{type(e).__name__}: {e}"
            _log(f"tick failed: {_last_error}")
        await asyncio.sleep(30)


# ── user endpoint (app half, ships 5.18.4) ───────────────────────────────

@router.post("/user/fcm-token")
async def register_user_fcm_token(
    request: Request,
    user: AuthedUser = Depends(get_current_user),
    session=Depends(get_session),
):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="bad json")
    token = (body.get("token") or "").strip()
    if not token or len(token) > 512:
        raise HTTPException(status_code=400, detail="missing/invalid token")
    device = (body.get("device") or "").strip()[:128] or None
    enabled = bool(body.get("morningPush", True))
    try:
        app_version = int(body.get("appVersionCode") or 0)
    except Exception:
        app_version = 0

    # The user row may not exist yet (fresh sign-in before first sync).
    await session.execute(
        sql_text(
            "INSERT INTO users (id, email, display_name, created_at, last_seen) "
            "VALUES (:id, :email, :name, now(), now()) "
            "ON CONFLICT (id) DO NOTHING"
        ),
        {"id": user["uid"], "email": user.get("email"),
         "name": user.get("name")},
    )
    await session.execute(
        sql_text(
            "INSERT INTO user_fcm_tokens "
            "(token, user_id, device, enabled, app_version_code, updated_at) "
            "VALUES (:t, :u, :d, :e, :v, now()) "
            "ON CONFLICT (token) DO UPDATE SET "
            "user_id = :u, device = :d, enabled = :e, "
            "app_version_code = :v, updated_at = now()"
        ),
        {"t": token, "u": user["uid"], "d": device, "e": enabled,
         "v": app_version},
    )
    await session.commit()
    return {"ok": True, "morningPush": enabled}


# ── admin endpoints ──────────────────────────────────────────────────────

@router.get("/admin/morning-push/status")
async def mp_status(request: Request, session=Depends(get_session)):
    _require_admin(request)
    cfg = await load_config(session)
    date_str = datetime.now(IST).strftime("%Y-%m-%d")
    tokens = (await session.execute(
        sql_text("SELECT COUNT(*) FROM user_fcm_tokens WHERE enabled")
    )).scalar()
    legacy = (await session.execute(
        sql_text("SELECT COUNT(*) FROM users WHERE fcm_token IS NOT NULL")
    )).scalar()
    users = (await session.execute(
        sql_text(
            "SELECT COUNT(*) FROM users u "
            "WHERE u.fcm_token IS NOT NULL "
            "   OR EXISTS (SELECT 1 FROM user_fcm_tokens t "
            "              WHERE t.user_id = u.id AND t.enabled)"
        )
    )).scalar()
    today = (await session.execute(
        sql_text("SELECT COUNT(*) FROM morning_push_log WHERE sent_date = :d"),
        {"d": date_str},
    )).scalar()
    pending = (await session.execute(
        sql_text("SELECT COUNT(*) FROM morning_push_drafts WHERE status = 'pending'")
    )).scalar()
    return {"config": cfg, "istNow": datetime.now(IST).isoformat(),
            "enabledTokens": tokens, "legacySocialTokens": legacy,
            "reachableUsers": users,
            "sentToday": today, "pendingDrafts": pending,
            "groqConfigured": bool(GROQ_API_KEY), "lastError": _last_error}


@router.get("/admin/morning-push/config")
async def mp_get_config(request: Request, session=Depends(get_session)):
    _require_admin(request)
    return await load_config(session)


@router.post("/admin/morning-push/config")
async def mp_set_config(request: Request, session=Depends(get_session)):
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="bad json")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="expected object")
    current = await load_config(session)
    merged = _merge_config(current, body)
    # days and audience arrive as COMPLETE snapshots from the console —
    # the nested merge resurrected removed entries (a re-enabled day could
    # never be deleted: its absence kept the old {enabled:false}, so days
    # could only ever accumulate disables). Replace them wholesale.
    if isinstance(body.get("days"), dict):
        merged["days"] = body["days"]
    if isinstance(body.get("audience"), dict):
        merged["audience"] = body["audience"]
    # A null override/note means "clear it for this user".
    if isinstance(merged.get("overrides"), dict):
        merged["overrides"] = {k: v for k, v in merged["overrides"].items() if v}
    if isinstance(merged.get("userContext"), dict):
        merged["userContext"] = {k: v for k, v in merged["userContext"].items() if v}
    await _kv_set(session, CONFIG_KEY, merged)
    return {"ok": True, "config": merged}


@router.get("/admin/morning-push/lines")
async def mp_get_lines(request: Request, session=Depends(get_session)):
    _require_admin(request)
    return await load_lines(session)


@router.post("/admin/morning-push/lines")
async def mp_set_lines(request: Request, session=Depends(get_session)):
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="bad json")
    lines = body.get("lines") if isinstance(body, dict) else None
    if not isinstance(lines, dict) or not lines:
        raise HTTPException(status_code=400, detail="expected {\"lines\": {...}}")
    for bucket, tones in lines.items():
        if not isinstance(tones, dict):
            raise HTTPException(status_code=400,
                                detail=f"bucket '{bucket}' must map tones to lists")
        for tone, pool in tones.items():
            if not isinstance(pool, list) or not all(
                isinstance(l, dict) and l.get("title") and l.get("body")
                for l in pool
            ):
                raise HTTPException(
                    status_code=400,
                    detail=f"'{bucket}.{tone}' must be a list of "
                           "{title, body} objects",
                )
    await _kv_set(session, LINES_KEY, lines)
    return {"ok": True, "lines": lines}


@router.get("/admin/morning-push/dry-run")
async def mp_dry_run(request: Request, llm: bool = False,
                     session=Depends(get_session)):
    _require_admin(request)
    cfg = await load_config(session)
    items = await build_batch(session, cfg, use_llm=llm)
    return {"date": datetime.now(IST).strftime("%Y-%m-%d"),
            "llm": llm, "count": len(items), "items": items}


@router.post("/admin/morning-push/generate")
async def mp_generate(request: Request, session=Depends(get_session)):
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    use_llm = bool(body.get("llm"))
    cfg = await load_config(session)
    items = await build_batch(session, cfg, use_llm=use_llm)
    draft_id = (
        await session.execute(
            sql_text(
                "INSERT INTO morning_push_drafts (status, payload, created_at) "
                "VALUES ('pending', :p, now()) RETURNING id"
            ),
            {"p": json.dumps(items, ensure_ascii=False)},
        )
    ).scalar()
    await session.commit()
    return {"ok": True, "draftId": draft_id, "llm": use_llm,
            "count": len(items), "items": items}


@router.get("/admin/morning-push/user-songs")
async def mp_user_songs(request: Request, email: str, days: int = 365,
                        session=Depends(get_session)):
    """One user's listening library, merged by cleaned title+artist and
    sorted by play count — feeds the console's custom-song picker."""
    _require_admin(request)
    days = max(1, min(days, 730))
    rows = (
        await session.execute(
            sql_text(
                """
                SELECT e.video_id,
                       MAX(e.title) AS title, MAX(e.uploader) AS uploader,
                       MAX(e.thumbnail) AS thumbnail, COUNT(*) AS plays,
                       COUNT(*) FILTER (
                           WHERE EXTRACT(HOUR FROM e.played_at AT TIME ZONE 'Asia/Kolkata')
                                 BETWEEN 6 AND 10
                       ) AS morning_plays
                FROM user_listen_events e
                JOIN users u ON u.id = e.user_id
                WHERE lower(u.email) = lower(:email)
                  AND e.played_at >= now() - make_interval(days => :days)
                GROUP BY e.video_id
                """
            ),
            {"email": email, "days": days},
        )
    ).all()
    merged = _merge_tracks(rows)
    merged.sort(key=lambda t: -t["plays"])
    return {"items": [
        {"song": t["song"], "artist": t["artist"], "videoId": t["videoId"],
         "image": t["thumbnail"], "plays": t["plays"], "morning": t["morning"]}
        for t in merged[:60]
    ]}


@router.post("/admin/morning-push/draft")
async def mp_create_draft(request: Request, session=Depends(get_session)):
    """Store an already-rendered batch (e.g. a dry-run the admin liked) as a
    sendable draft — what was previewed is exactly what goes out."""
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="bad json")
    items = body.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="expected {\"items\": [...]}")
    date_str = datetime.now(IST).strftime("%Y-%m-%d")
    for it in items:
        if not (isinstance(it, dict) and it.get("uid")
                and it.get("title") and it.get("body")):
            raise HTTPException(
                status_code=400,
                detail="each item needs at least uid, title, body",
            )
        it.setdefault("date", date_str)
    draft_id = (
        await session.execute(
            sql_text(
                "INSERT INTO morning_push_drafts (status, payload, created_at) "
                "VALUES ('pending', :p, now()) RETURNING id"
            ),
            {"p": json.dumps(items, ensure_ascii=False)},
        )
    ).scalar()
    await session.commit()
    return {"ok": True, "draftId": draft_id, "count": len(items)}


@router.post("/admin/morning-push/render-one")
async def mp_render_one(request: Request, session=Depends(get_session)):
    """Re-render a single user's pick + line (fresh Groq roll when llm=true)
    without touching the rest of the batch — the console swaps the row in."""
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="bad json")
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="missing email")
    cfg = await load_config(session)
    items = await build_batch(session, cfg, use_llm=bool(body.get("llm")),
                              only_email=email)
    if not items:
        raise HTTPException(status_code=404,
                            detail="user not in the current audience")
    return {"ok": True, "item": items[0]}


@router.get("/admin/morning-push/drafts")
async def mp_drafts(request: Request, session=Depends(get_session)):
    _require_admin(request)
    rows = (
        await session.execute(
            sql_text(
                "SELECT id, status, payload, created_at FROM morning_push_drafts "
                "ORDER BY id DESC LIMIT 10"
            )
        )
    ).all()
    out = []
    for r in rows:
        try:
            items = json.loads(r.payload or "[]")
        except Exception:
            items = []
        out.append({"draftId": r.id, "status": r.status,
                    "createdAt": r.created_at.isoformat(),
                    "count": len(items), "items": items})
    return {"drafts": out}


@router.post("/admin/morning-push/send")
async def mp_send(request: Request, session=Depends(get_session)):
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="bad json")

    force = bool(body.get("force"))
    if body.get("direct"):
        cfg = await load_config(session)
        items = await build_batch(session, cfg, use_llm=bool(body.get("llm")))
        summary = await send_batch(session, items, force=force)
        return {"ok": True, "mode": "direct", "force": force, **summary}

    draft_id = body.get("draftId")
    if not draft_id:
        raise HTTPException(status_code=400,
                            detail="pass {\"draftId\": N} or {\"direct\": true}")
    row = (
        await session.execute(
            sql_text(
                "SELECT status, payload FROM morning_push_drafts WHERE id = :i"
            ),
            {"i": int(draft_id)},
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="draft not found")
    if row.status != "pending":
        raise HTTPException(status_code=409, detail=f"draft is '{row.status}'")
    items = json.loads(row.payload or "[]")
    summary = await send_batch(session, items, force=force)
    await session.execute(
        sql_text("UPDATE morning_push_drafts SET status = 'sent' WHERE id = :i"),
        {"i": int(draft_id)},
    )
    await session.commit()
    return {"ok": True, "mode": "draft", "draftId": int(draft_id), **summary}


@router.get("/admin/morning-push/users")
async def mp_users(request: Request, session=Depends(get_session)):
    """Every user the push can reach, for the console's audience picker."""
    _require_admin(request)
    rows = (
        await session.execute(
            sql_text(
                """
                SELECT u.id, u.email, u.display_name, u.last_seen,
                       (u.fcm_token IS NOT NULL) AS social_token,
                       (SELECT COUNT(*) FROM user_fcm_tokens t
                         WHERE t.user_id = u.id AND t.enabled) AS app_tokens,
                       (SELECT COALESCE(MAX(t.app_version_code), 0)
                          FROM user_fcm_tokens t
                         WHERE t.user_id = u.id) AS app_version,
                       (SELECT COUNT(*) FROM user_listen_events e
                         WHERE e.user_id = u.id) AS events
                FROM users u
                WHERE u.fcm_token IS NOT NULL
                   OR EXISTS (SELECT 1 FROM user_fcm_tokens t
                              WHERE t.user_id = u.id AND t.enabled)
                ORDER BY u.last_seen DESC NULLS LAST
                """
            )
        )
    ).all()
    return {"items": [
        {"uid": r.id, "email": r.email or "", "name": r.display_name or "",
         "lastSeen": r.last_seen.isoformat() if r.last_seen else "",
         "socialToken": bool(r.social_token), "appTokens": r.app_tokens,
         "appVersion": r.app_version, "events": r.events}
        for r in rows
    ]}


@router.get("/admin/morning-push/log")
async def mp_log(request: Request, limit: int = 60,
                 session=Depends(get_session)):
    _require_admin(request)
    limit = max(1, min(limit, 300))
    rows = (
        await session.execute(
            sql_text(
                "SELECT l.user_id, u.email, l.sent_date, l.video_id, "
                "       l.title, l.body, l.status, l.created_at "
                "FROM morning_push_log l LEFT JOIN users u ON u.id = l.user_id "
                "ORDER BY l.id DESC LIMIT :lim"
            ),
            {"lim": limit},
        )
    ).all()
    return {"items": [
        {"userId": r.user_id, "email": r.email or "", "sentDate": r.sent_date,
         "videoId": r.video_id or "", "title": r.title or "",
         "body": r.body or "", "status": r.status or "",
         "createdAt": r.created_at.isoformat()}
        for r in rows
    ]}


@router.post("/admin/morning-push/discard")
async def mp_discard(request: Request, session=Depends(get_session)):
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="bad json")
    draft_id = body.get("draftId")
    if not draft_id:
        raise HTTPException(status_code=400, detail="pass {\"draftId\": N}")
    await session.execute(
        sql_text(
            "UPDATE morning_push_drafts SET status = 'discarded' "
            "WHERE id = :i AND status = 'pending'"
        ),
        {"i": int(draft_id)},
    )
    await session.commit()
    return {"ok": True}
