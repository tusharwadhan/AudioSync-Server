"""Moments outside the app: a short public link to a window of a song.

A "moment" is a POINTER (video_id + [start_ms, end_ms] + optional lyric lines
+ note), never audio. The app creates one with POST /api/v1/moment and shares
the returned https link; opening it either lands in the app (Android App Links,
newer builds) or in the public page below, which plays just that window in the
YouTube embedded player.

Routes
  POST /api/v1/moment          (signed-in)  -> {id, url}
  GET  /api/v1/moment/{id}     (API-key)    -> the stored pointer, for the app
  GET  /share/moment/{id}      (public)     -> the web page + link-preview tags
"""
from __future__ import annotations

import html
import json
import re
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import models
import social
from auth import AuthedUser, get_current_user
from db import get_session

router = APIRouter(tags=["moment-share"])
public_router = APIRouter(tags=["moment-share-public"])

PUBLIC_BASE = "https://audiosync-server.onrender.com"
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,16}$")
_MAX_PER_HOUR = 30


def _stored(row: models.MomentShare) -> dict:
    p = dict(row.payload)
    p["id"] = row.id
    return p


@router.post("/moment")
async def create_moment(
    body: dict,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """Store a moment and hand back its public link.

    The body is the same shape a DM dedication carries (video_id, start_ms,
    end_ms, lines, note, title, artist) and goes through the SAME sanitiser,
    so nothing the client sends reaches the page unvalidated."""
    raw = dict(body) if isinstance(body, dict) else {}
    raw["kind"] = "dedication"  # the sanitiser's one accepted kind
    clean = social._sanitize_dedication(raw)
    if clean is None:
        raise HTTPException(status_code=400, detail="bad_moment")

    since = datetime.now(timezone.utc) - timedelta(hours=1)
    recent = (
        await session.execute(
            select(func.count())
            .select_from(models.MomentShare)
            .where(
                models.MomentShare.user_id == user["uid"],
                models.MomentShare.created_at > since,
            )
        )
    ).scalar_one()
    if recent >= _MAX_PER_HOUR:
        raise HTTPException(status_code=429, detail="too_many_moments")

    # 6 random bytes -> 8 url-safe chars; a collision is retried, not trusted.
    for _ in range(5):
        mid = secrets.token_urlsafe(6)
        if await session.get(models.MomentShare, mid) is None:
            break
    else:
        raise HTTPException(status_code=503, detail="try_again")

    session.add(models.MomentShare(id=mid, user_id=user["uid"], payload=clean))
    try:
        await session.commit()
    except IntegrityError:
        # The FK to users.id fails when this account has not synced yet
        # (/auth/sync creates the row). A clean 409, not a 500.
        await session.rollback()
        raise HTTPException(status_code=409, detail="sync_account_first")
    return {"id": mid, "url": f"{PUBLIC_BASE}/share/moment/{mid}"}


@router.get("/moment/{moment_id}")
async def get_moment(
    moment_id: str, session: AsyncSession = Depends(get_session)
):
    if not _ID_RE.fullmatch(moment_id):
        raise HTTPException(status_code=404, detail="not_found")
    row = await session.get(models.MomentShare, moment_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not_found")
    return _stored(row)


_NOT_FOUND_PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Moment not found - SyncAura</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0b0a10;
color:#fff;font-family:system-ui,sans-serif;text-align:center;padding:24px}
a{color:#ffd6aa}</style></head><body><div><h2>This moment isn't here any more</h2>
<p style="color:#ffffffa0">The link may be mistyped.</p>
<p><a href="/app">Get SyncAura</a></p></div></body></html>"""


_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__ - a moment on SyncAura</title>
<meta property="og:title" content="__OG_TITLE__" />
<meta property="og:description" content="__OG_DESC__" />
<meta property="og:image" content="__THUMB__" />
<meta property="og:url" content="__URL__" />
<meta property="og:type" content="website" />
<meta name="twitter:card" content="summary_large_image" />
<meta name="theme-color" content="#0b0a10" />
<meta name="robots" content="noindex" />
<style>
*{box-sizing:border-box;margin:0}
html,body{min-height:100%}
body{background:#0b0a10;color:#fff;font-family:Roboto,"Segoe UI",system-ui,-apple-system,sans-serif;
 display:flex;justify-content:center;position:relative;overflow-x:hidden}
.ember{position:fixed;inset:0;opacity:.4;pointer-events:none;
 background:radial-gradient(ellipse 80% 55% at 50% 42%,#e8782a 0,#8a3a14 45%,transparent 80%),
 radial-gradient(ellipse 120% 100% at 50% 50%,transparent 45%,#000 100%)}
main{position:relative;z-index:1;width:100%;max-width:440px;min-height:100vh;padding:22px 20px 24px;
 display:flex;flex-direction:column}
.brand{display:flex;align-items:center;gap:8px;font-weight:700;font-size:15px;letter-spacing:.02em}
.logo{width:24px;height:24px;border-radius:7px;background:linear-gradient(135deg,#ffb064,#ff7828)}
.hdr{margin-top:22px;font-size:11px;letter-spacing:.08em;color:#e8a56a;font-weight:700}
.from{font-size:17px;font-weight:600;margin-top:3px}
.video{margin-top:16px;width:100%;aspect-ratio:16/9;border-radius:26px;overflow:hidden;
 background:#000;box-shadow:0 20px 50px #0009;position:relative}
.video iframe,.video #yt{position:absolute;inset:0;width:100%;height:100%;border:0}
.tag{position:absolute;left:12px;top:10px;z-index:2;font-size:11px;font-weight:600;padding:4px 9px;
 border-radius:99px;background:#00000099;border:1px solid #ffffff30;pointer-events:none}
.title{margin-top:16px;font-size:21px;font-weight:600;text-align:center}
.artist{margin-top:3px;font-size:14px;color:#ffffffb3;text-align:center}
.note{margin-top:10px;font-size:14px;font-style:italic;color:#ecdff2;text-align:center;line-height:1.4}
.lyr{margin-top:14px;text-align:center;display:flex;flex-direction:column;gap:5px;min-height:70px}
.lyr .l{font-size:13px;color:#ffffff59;min-height:17px}
.lyr .n{font-size:17px;font-weight:700;min-height:22px}
.sp{flex:1;min-height:18px}
.glass{width:100%;border-radius:26px;padding:14px;background:linear-gradient(135deg,#ffffff24,#ffffff0a);
 border:1px solid #ffffff33;box-shadow:inset 0 1px 0 #ffffff40;backdrop-filter:blur(22px);-webkit-backdrop-filter:blur(22px)}
.btn{height:50px;border-radius:25px;display:flex;align-items:center;justify-content:center;
 font-weight:600;font-size:15px;color:#fff;text-decoration:none;
 background:linear-gradient(135deg,#ffb064e6,#ff782899);border:1px solid #ffd6aaa6;
 box-shadow:inset 0 1px 0 #ffffff59}
.btn2{margin-top:9px;text-align:center;font-size:12.5px;color:#ffffffb0}
.btn2 a{color:#ffd6aa;text-underline-offset:3px}
.embedfail{display:none;margin-top:10px;font-size:12.5px;color:#ffd6aa;text-align:center}
.hint{margin-top:10px;font-size:12px;color:#ffffff99;text-align:center;line-height:1.4;display:none}
</style>
</head>
<body>
<div class="ember"></div>
<main>
 <div class="brand"><div class="logo"></div>SyncAura</div>
 <div class="hdr">A MOMENT FOR YOU</div>
 <div class="from">__FROM__</div>
 <div class="video"><div class="tag">__SECS__ s moment</div><div id="yt"></div></div>
 <div class="title">__TITLE__</div>
 <div class="artist">__ARTIST__</div>
 __NOTE__
 <div class="lyr" id="lyr"><div class="l" id="lp"></div><div class="n" id="ln"></div><div class="l" id="lx"></div></div>
 <div class="embedfail" id="embedfail">This video can't play here. Open it in SyncAura to hear the moment.</div>
 <div class="sp"></div>
 <div class="glass">
  <a class="btn" id="open" href="__DEEP__">Open in SyncAura</a>
  <div class="btn2">No app yet? <a href="/app">Get SyncAura</a></div>
  <div class="hint" id="hint">Nothing opened. Your SyncAura may be out of date, or not installed yet. <a href="/app" style="color:#ffd6aa">Get the latest</a></div>
 </div>
</main>
<script id="moment" type="application/json">__DATA__</script>
<script>
(function(){
 var m = JSON.parse(document.getElementById('moment').textContent);
 var startS = Math.floor(m.start_ms/1000), endS = Math.ceil(m.end_ms/1000);
 // "Open in SyncAura": try the app, and if the page is still visible after a
 // moment, say so - a browser cannot tell whether an app is installed.
 var open = document.getElementById('open'), hint = document.getElementById('hint');
 open.addEventListener('click', function(){
   var t = setTimeout(function(){ if(!document.hidden) hint.style.display='block'; }, 1600);
   var onVis = function(){ if(document.hidden){ clearTimeout(t); document.removeEventListener('visibilitychange', onVis); } };
   document.addEventListener('visibilitychange', onVis);
 });
 var lines = m.lines || [];
 function show(i){
   document.getElementById('lp').textContent = i>0 ? lines[i-1].text : '';
   document.getElementById('ln').textContent = i>=0 ? lines[i].text : '';
   document.getElementById('lx').textContent = (i+1<lines.length) ? lines[i+1].text : '';
 }
 var player, timer, last = -2;
 function tick(){
   if(!player || !player.getCurrentTime || !lines.length) return;
   var pos = player.getCurrentTime()*1000, idx = -1;
   for(var i=0;i<lines.length;i++){ if(lines[i].t <= pos) idx=i; else break; }
   if(idx!==last){ last=idx; show(idx); }
 }
 window.onYouTubeIframeAPIReady = function(){
   player = new YT.Player('yt', {
     videoId: m.video_id,
     playerVars: { start:startS, end:endS, playsinline:1, rel:0, modestbranding:1 },
     events: {
       onReady:function(){ timer=setInterval(tick,200); },
       onError:function(){ document.getElementById('embedfail').style.display='block'; }
     }
   });
 };
 var s = document.createElement('script'); s.src = 'https://www.youtube.com/iframe_api';
 document.head.appendChild(s);
 if(lines.length) show(-1);
})();
</script>
</body>
</html>"""


def _e(v: str | None) -> str:
    return html.escape(v or "", quote=True)


@public_router.get("/share/moment/{moment_id}", response_class=HTMLResponse)
async def moment_page(
    moment_id: str, request: Request, session: AsyncSession = Depends(get_session)
):
    if not _ID_RE.fullmatch(moment_id):
        return HTMLResponse(_NOT_FOUND_PAGE, status_code=404)
    row = await session.get(models.MomentShare, moment_id)
    if row is None:
        return HTMLResponse(_NOT_FOUND_PAGE, status_code=404)

    p = row.payload
    title = p.get("title") or "A song"
    artist = p.get("artist") or ""
    note = p.get("note") or ""
    secs = max(1, round((p["end_ms"] - p["start_ms"]) / 1000))

    sender = await session.get(models.User, row.user_id)
    # First name only: this page is public, and a full account name is more
    # than a shared link needs to say.
    full = (getattr(sender, "display_name", None) or "").strip() if sender else ""
    name = full.split()[0] if full else ""
    from_line = f"from {name}" if name else "from a friend"

    desc = f"{artist} · {secs} s moment" if artist else f"{secs} s moment"
    if note:
        desc += f" · “{note}”"
    og_title = f"{name} sent you a moment · {title}" if name else f"A moment · {title}"
    thumb = f"https://i.ytimg.com/vi/{p['video_id']}/hqdefault.jpg"

    # The JSON blob rides in a <script type=application/json>: "<" is escaped so
    # a lyric or note can never close the tag.
    data = json.dumps(
        {
            "video_id": p["video_id"],
            "start_ms": p["start_ms"],
            "end_ms": p["end_ms"],
            "lines": p.get("lines") or [],
        },
        ensure_ascii=False,
    ).replace("<", "\\u003c")

    note_html = f'<div class="note">“{_e(note)}”</div>' if note else ""
    # One pass over the template: a title or note that happens to contain
    # "__DATA__" must not be substituted a second time.
    subs = {
        "__TITLE__": _e(title),
        "__OG_TITLE__": _e(og_title),
        "__OG_DESC__": _e(desc),
        "__THUMB__": _e(thumb),
        "__URL__": _e(f"{PUBLIC_BASE}/share/moment/{moment_id}"),
        "__FROM__": _e(from_line),
        "__SECS__": str(secs),
        "__ARTIST__": _e(artist),
        "__NOTE__": note_html,
        "__DEEP__": _e(f"syncaura://moment/{moment_id}"),
        "__DATA__": data,
    }
    page = re.sub(r"__[A-Z_]+__", lambda m: subs.get(m.group(0), m.group(0)), _PAGE)
    return HTMLResponse(page, headers={"Cache-Control": "public, max-age=300"})
