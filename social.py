"""
SyncAura social v1 — global lounge + online presence + 1:1 DMs.

Architecture
------------

  * Persistent WebSocket per signed-in client. The existing /api/v1/ws
    endpoint accepts a `social_subscribe` message carrying a Firebase
    ID token; once verified, the connection is tied to a UID and
    enrolled in the in-memory `PresenceManager`.

  * Presence is RAM-only — `PresenceManager._by_uid: dict[uid, _Online]`.
    On server restart everyone is offline until they reconnect. We
    deliberately do not persist this; it's transient by definition.

  * Lounge messages and DMs persist in Postgres (see `models.py`).
    Lounge keeps a rolling last-200 via the periodic prune task; DMs
    are deleted ~1h after being read so concurrent sessions still see
    them but the table doesn't grow without bound.

  * The "chat wipes on app restart except unread messages" UX is
    enforced client-side: the snapshot endpoint only returns unread
    DMs (and the sender's own still-unread sent messages) — the
    client doesn't keep a local cache across cold starts.

  * No friend-search / add-by-uid v1. Friendships are forged the
    instant either side taps Accept on a pending DM thread; before
    that, every cross-user dm_send is gated through a one-time
    request flow.

WebSocket message types handled here
------------------------------------

  Client → Server:
    social_subscribe  {idToken}         — auth + go online
    presence_ping     {}                — heartbeat
    lounge_send       {text?, np_*}     — post to global lounge
    dm_send           {to_uid, text?, np_*}
    dm_accept         {peer_uid}        — cross the stranger gate
    dm_decline        {peer_uid}        — soft-block from this side
    dm_read           {peer_uid, up_to_message_id}

  Server → Client:
    social_snapshot   {online, friends, lounge, unread_dms, pending_requests}
    presence_join     {uid, name, avatar_url}
    presence_leave    {uid}
    lounge_message    {id, from_uid, from_name, from_avatar_url, text?, np_*, sent_at}
    dm_message        {id, from_uid, from_name, from_avatar_url, text?, np_*, sent_at, gate_state}
    dm_friendship_formed {peer_uid}
    social_error      {code, message}

REST endpoints (mounted under /api/v1/social):
    GET /snapshot       — same payload as `social_snapshot` WS message
    GET /friends        — list of friend UIDs with name/avatar
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, WebSocket
from pydantic import BaseModel
from sqlalchemy import and_, delete, or_, select, update, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from firebase_admin import auth as fb_auth

from auth import AuthedUser, get_current_user
import db
import models

# firebase_admin is initialized at app startup in main.py for the
# existing room-reconnect FCM path; importing the `messaging` module
# here is safe regardless of init ordering — we guard sends behind
# `firebase_admin._apps` so we no-op silently if the SDK isn't ready.
try:
    import firebase_admin
    from firebase_admin import messaging as _fcm_messaging
except Exception:  # pragma: no cover — local dev without firebase_admin
    firebase_admin = None
    _fcm_messaging = None


router = APIRouter(prefix="/social", tags=["social"])


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────


def _ordered_pair(uid1: str, uid2: str) -> tuple[str, str]:
    """Return (uid_a, uid_b) sorted lex-ascending — the canonical pair
    ordering used by friendships and dm_thread_state."""
    return (uid1, uid2) if uid1 < uid2 else (uid2, uid1)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _verify_id_token_sync(token: str) -> dict | None:
    """Verify a Firebase ID token. Returns the decoded dict or None on
    failure. Runs the verifier in a thread because firebase_admin is
    synchronous and we don't want to block the asyncio loop."""
    try:
        return fb_auth.verify_id_token(token)
    except Exception as e:
        print(f"[social] token verify failed: {e}")
        return None


async def _verify_id_token(token: str) -> dict | None:
    return await asyncio.to_thread(_verify_id_token_sync, token)


# ─────────────────────────────────────────────────────────────────────
# Presence (in-memory)
# ─────────────────────────────────────────────────────────────────────


@dataclass
class _Online:
    client_id: str
    websocket: WebSocket
    uid: str
    name: str
    avatar_url: str | None
    last_ping: float


class PresenceManager:
    """Tracks who's currently connected. One entry per UID — if a user
    opens the app on a second device, the new connection replaces the
    old one (`subscribe` returns the kicked entry so the caller can
    close the prior socket cleanly).
    """

    def __init__(self):
        self._by_uid: dict[str, _Online] = {}
        self._client_to_uid: dict[str, str] = {}

    def subscribe(
        self,
        ws: WebSocket,
        client_id: str,
        uid: str,
        name: str,
        avatar_url: str | None,
    ) -> _Online | None:
        """Add the connection. Returns the previously-online entry for
        this UID if there was one (so the caller can drop that socket).
        """
        kicked: _Online | None = None
        prior = self._by_uid.get(uid)
        if prior:
            self._client_to_uid.pop(prior.client_id, None)
            kicked = prior
        entry = _Online(
            client_id=client_id,
            websocket=ws,
            uid=uid,
            name=name,
            avatar_url=avatar_url,
            last_ping=time.time(),
        )
        self._by_uid[uid] = entry
        self._client_to_uid[client_id] = uid
        return kicked

    def unsubscribe_by_client(self, client_id: str) -> _Online | None:
        """Remove the connection by client_id. Returns the removed entry
        (so the caller can broadcast presence_leave)."""
        uid = self._client_to_uid.pop(client_id, None)
        if uid is None:
            return None
        entry = self._by_uid.get(uid)
        if entry and entry.client_id == client_id:
            del self._by_uid[uid]
            return entry
        return None

    def get(self, uid: str) -> _Online | None:
        return self._by_uid.get(uid)

    def is_online(self, uid: str) -> bool:
        return uid in self._by_uid

    def uid_for_client(self, client_id: str) -> str | None:
        return self._client_to_uid.get(client_id)

    def touch(self, client_id: str) -> None:
        uid = self._client_to_uid.get(client_id)
        if uid is None:
            return
        entry = self._by_uid.get(uid)
        if entry:
            entry.last_ping = time.time()

    def online_summaries(self) -> list[dict]:
        return [
            {"uid": o.uid, "name": o.name, "avatar_url": o.avatar_url}
            for o in self._by_uid.values()
        ]

    def all_entries(self) -> list[_Online]:
        return list(self._by_uid.values())


presence = PresenceManager()


# ─────────────────────────────────────────────────────────────────────
# FCM token registry (UID -> device push token)
# ─────────────────────────────────────────────────────────────────────
#
# Populated by the `social_fcm_register` WS message the client sends
# right after a successful social_subscribe. Used to push DM
# notifications to recipients whose WebSocket isn't connected at the
# moment a message lands. Cleared lazily on UnregisteredError replies
# from FCM (token rotated / app uninstalled).

_fcm_tokens_by_uid: dict[str, str] = {}


async def _push_dm_fcm(
    *,
    recipient_uid: str,
    sender_uid: str,
    sender_name: str,
    preview: str,
) -> None:
    """Send a data-only FCM message for a DM the recipient missed
    because they weren't connected. Best-effort, but every branch
    logs explicitly so the Render dashboard makes the failure mode
    obvious when a user reports "no notification while offline"."""
    print(
        f"[social] FCM attempt -> recipient={recipient_uid[:8]} "
        f"sender={sender_uid[:8]} (tokens_map_size={len(_fcm_tokens_by_uid)})"
    )
    token = _fcm_tokens_by_uid.get(recipient_uid)
    if not token:
        print(f"[social] FCM skip: no token stored for {recipient_uid[:8]}")
        return
    if firebase_admin is None or not firebase_admin._apps:
        print(
            f"[social] FCM skip: firebase_admin not initialized "
            f"(uid={recipient_uid[:8]})"
        )
        return
    try:
        message = _fcm_messaging.Message(
            data={
                "type": "dm",
                "from_uid": sender_uid,
                "from_name": sender_name[:120],
                "preview": (preview or "")[:200],
            },
            token=token,
            android=_fcm_messaging.AndroidConfig(priority="high"),
        )
        message_id = await asyncio.to_thread(_fcm_messaging.send, message)
        print(
            f"[social] FCM DM push OK -> {recipient_uid[:8]} "
            f"message_id={message_id}"
        )
    except _fcm_messaging.UnregisteredError:
        # Stale token — drop from the map so we don't keep retrying.
        _fcm_tokens_by_uid.pop(recipient_uid, None)
        print(f"[social] FCM stale token removed for {recipient_uid[:8]}")
    except Exception as e:
        print(f"[social] FCM DM push FAILED for {recipient_uid[:8]}: {e}")


# ─────────────────────────────────────────────────────────────────────
# WS send helpers
# ─────────────────────────────────────────────────────────────────────


async def _send(ws: WebSocket, msg: dict) -> None:
    try:
        await ws.send_text(json.dumps(msg))
    except Exception:
        # Connection probably dropped; cleanup happens in the receive
        # loop's WebSocketDisconnect path.
        pass


async def _send_to_uid(uid: str, msg: dict) -> bool:
    """Returns True if the message was delivered over an open WS."""
    entry = presence.get(uid)
    if entry is None:
        return False
    await _send(entry.websocket, msg)
    return True


async def _broadcast(msg: dict, exclude_uid: str | None = None) -> None:
    payload = json.dumps(msg)
    tasks = []
    for entry in presence.all_entries():
        if exclude_uid and entry.uid == exclude_uid:
            continue
        tasks.append(_safe_send_text(entry.websocket, payload))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _safe_send_text(ws: WebSocket, payload: str) -> None:
    try:
        await ws.send_text(payload)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────


async def _open_session() -> AsyncSession:
    """Mint a fresh AsyncSession for use inside a WS handler (where the
    `Depends(get_session)` machinery doesn't apply). Caller is
    responsible for closing it (`async with await _open_session() as s`
    pattern doesn't work because the returned session isn't an async
    context manager — use try/finally or `async with _session_scope():`
    below)."""
    db._ensure_engine()
    assert db._SessionLocal is not None
    return db._SessionLocal()


class _session_scope:
    """async with helper. `async with _session_scope() as session: ...`."""

    def __init__(self):
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> AsyncSession:
        db._ensure_engine()
        assert db._SessionLocal is not None
        self._session = db._SessionLocal()
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        if self._session is not None:
            await self._session.close()


async def _is_friend(session: AsyncSession, uid_a: str, uid_b: str) -> bool:
    a, b = _ordered_pair(uid_a, uid_b)
    result = await session.execute(
        select(models.Friendship).where(
            models.Friendship.uid_a == a,
            models.Friendship.uid_b == b,
        )
    )
    return result.scalar_one_or_none() is not None


async def _get_thread_state(
    session: AsyncSession, uid_a: str, uid_b: str
) -> models.DmThreadState | None:
    a, b = _ordered_pair(uid_a, uid_b)
    result = await session.execute(
        select(models.DmThreadState).where(
            models.DmThreadState.uid_a == a,
            models.DmThreadState.uid_b == b,
        )
    )
    return result.scalar_one_or_none()


async def _ensure_thread_state(
    session: AsyncSession, sender_uid: str, recipient_uid: str
) -> tuple[models.DmThreadState, bool]:
    """Fetch-or-create the thread state row for the pair, anchored on
    whoever sends the first message. Returns (row, created_new)."""
    a, b = _ordered_pair(sender_uid, recipient_uid)
    initial_state = "pending_from_a" if sender_uid == a else "pending_from_b"

    existing = await _get_thread_state(session, sender_uid, recipient_uid)
    if existing is not None:
        return existing, False

    row = models.DmThreadState(uid_a=a, uid_b=b, state=initial_state, updated_at=_now())
    session.add(row)
    await session.flush()
    return row, True


async def _friend_summaries(session: AsyncSession, uid: str) -> list[dict]:
    """Return [{uid, name, avatar_url, is_online}] for all friends of uid."""
    # JOIN against users to pick up display_name + photo_url. Friendship
    # rows store the pair with uid_a < uid_b, so the friend's uid is
    # whichever side isn't `uid`.
    other_col = func.coalesce(
        func.nullif(models.Friendship.uid_a, uid), models.Friendship.uid_b
    ).label("friend_uid")
    stmt = (
        select(
            other_col,
            models.User.display_name,
            models.User.photo_url,
        )
        .select_from(models.Friendship)
        .join(models.User, models.User.id == other_col)
        .where(
            or_(
                models.Friendship.uid_a == uid,
                models.Friendship.uid_b == uid,
            )
        )
    )
    rows = (await session.execute(stmt)).all()
    return [
        {
            "uid": friend_uid,
            "name": display_name or "",
            "avatar_url": photo_url,
            "is_online": presence.is_online(friend_uid),
        }
        for (friend_uid, display_name, photo_url) in rows
    ]


def _dm_to_dict(m: models.DmMessage, *, with_gate_state: str | None = None) -> dict:
    d = {
        "id": m.id,
        "from_uid": m.from_uid,
        "to_uid": m.to_uid,
        "text": m.text,
        "np_video_id": m.np_video_id,
        "np_title": m.np_title,
        "np_artist": m.np_artist,
        "np_thumbnail": m.np_thumbnail,
        "sent_at": _to_ms(m.sent_at),
        "read_at": _to_ms(m.read_at),
        # Chat-parity fields (migration 0005).
        "reactions": dict(m.reactions or {}),
        "reply_to_message_id": m.reply_to_message_id,
        "edited_at": _to_ms(m.edited_at),
        "deleted": bool(m.deleted),
        "share_moment": m.share_moment,
    }
    if with_gate_state is not None:
        d["gate_state"] = with_gate_state
    return d


def _lounge_to_dict(m: models.LoungeMessage) -> dict:
    return {
        "id": m.id,
        "from_uid": m.from_uid,
        "from_name": m.from_name,
        "from_avatar_url": m.from_avatar_url,
        "text": m.text,
        "np_video_id": m.np_video_id,
        "np_title": m.np_title,
        "np_artist": m.np_artist,
        "np_thumbnail": m.np_thumbnail,
        "sent_at": _to_ms(m.sent_at),
        # Chat-parity fields (migration 0006).
        "reactions": dict(m.reactions or {}),
        "reply_to_message_id": m.reply_to_message_id,
    }


async def _build_snapshot(session: AsyncSession, uid: str) -> dict:
    """Build the social_snapshot payload for the given user."""

    # Online list — strip the user themselves out so the client doesn't
    # have to filter.
    online = [o for o in presence.online_summaries() if o["uid"] != uid]

    # Friends with online flag.
    friends = await _friend_summaries(session, uid)

    # Lounge: newest 200 messages, oldest-first for direct render.
    lounge_rows = (
        await session.execute(
            select(models.LoungeMessage)
            .order_by(models.LoungeMessage.sent_at.desc())
            .limit(200)
        )
    ).scalars().all()
    lounge = [_lounge_to_dict(m) for m in reversed(lounge_rows)]

    # DMs:
    #   * unread inbox: messages where to_uid = me AND read_at IS NULL
    #   * own pending: messages where from_uid = me AND read_at IS NULL
    #
    # Split out 'pending_requests' from 'unread_dms' using the per-pair
    # thread state — a pending-from-stranger thread surfaces as a
    # request, an accepted/friend thread surfaces in unread_dms.
    incoming = (
        await session.execute(
            select(models.DmMessage)
            .where(models.DmMessage.to_uid == uid, models.DmMessage.read_at.is_(None))
            .order_by(models.DmMessage.sent_at.asc())
        )
    ).scalars().all()

    sent_pending = (
        await session.execute(
            select(models.DmMessage)
            .where(
                models.DmMessage.from_uid == uid,
                models.DmMessage.read_at.is_(None),
            )
            .order_by(models.DmMessage.sent_at.asc())
        )
    ).scalars().all()

    # Pull all thread states involving me — used to bucket incoming
    # messages into pending vs accepted.
    thread_states = (
        await session.execute(
            select(models.DmThreadState).where(
                or_(
                    models.DmThreadState.uid_a == uid,
                    models.DmThreadState.uid_b == uid,
                )
            )
        )
    ).scalars().all()
    state_by_peer: dict[str, str] = {}
    for ts in thread_states:
        peer = ts.uid_b if ts.uid_a == uid else ts.uid_a
        state_by_peer[peer] = ts.state

    # Bucket incoming. For declined threads, only the LATEST unread
    # message is returned (the user wanted older declined messages
    # suppressed on snapshot to keep their inbox from drowning in spam
    # they already chose to ignore).
    pending_requests: list[dict] = []
    unread_dms: list[dict] = []
    declined_latest: dict[str, models.DmMessage] = {}
    for m in incoming:
        peer = m.from_uid
        state = state_by_peer.get(peer)
        if state in ("accepted",) or state is None:
            # state == None should not happen if the sender went through
            # dm_send, but be defensive — treat orphans as pending.
            if state is None:
                pending_requests.append(_dm_to_dict(m, with_gate_state="pending"))
            else:
                unread_dms.append(_dm_to_dict(m, with_gate_state="accepted"))
        elif state in ("pending_from_a", "pending_from_b"):
            pending_requests.append(_dm_to_dict(m, with_gate_state="pending"))
        elif state in ("declined_by_a", "declined_by_b"):
            # Keep only the newest per peer.
            existing = declined_latest.get(peer)
            if existing is None or m.sent_at > existing.sent_at:
                declined_latest[peer] = m

    for m in declined_latest.values():
        pending_requests.append(_dm_to_dict(m, with_gate_state="declined"))

    # Outgoing-still-unread (for the sender's view of their own
    # in-flight messages). Group by peer for the client.
    sent_unread = [_dm_to_dict(m, with_gate_state="accepted") for m in sent_pending]

    return {
        "online": online,
        "friends": friends,
        "lounge": lounge,
        "unread_dms": unread_dms,
        "pending_requests": pending_requests,
        "sent_unread_dms": sent_unread,
        "server_time": _to_ms(_now()),
    }


# ─────────────────────────────────────────────────────────────────────
# WS handlers (called from main.websocket_endpoint dispatch)
# ─────────────────────────────────────────────────────────────────────


async def handle_social_subscribe(client_id: str, websocket: WebSocket, msg: dict) -> None:
    """`social_subscribe {idToken}` — authenticate the connection and
    enrol it in PresenceManager. On success, replies with
    `social_snapshot` and broadcasts `presence_join` to everyone else.
    """
    token = msg.get("idToken") or msg.get("id_token") or ""
    if not isinstance(token, str) or not token:
        await _send(websocket, _err("auth_missing", "idToken required"))
        return

    decoded = await _verify_id_token(token)
    if decoded is None:
        await _send(websocket, _err("auth_invalid", "Invalid Firebase ID token"))
        return

    uid = decoded.get("uid") or decoded.get("user_id")
    name = decoded.get("name") or ""
    avatar_url = decoded.get("picture")
    if not uid:
        await _send(websocket, _err("auth_invalid", "Token missing uid"))
        return

    # Pull display_name from DB if the token didn't include `name` (e.g.
    # the user has changed it server-side via /auth/sync since signing in).
    async with _session_scope() as session:
        user_row = (
            await session.execute(
                select(models.User).where(models.User.id == uid)
            )
        ).scalar_one_or_none()
        if user_row:
            name = user_row.display_name or name
            avatar_url = user_row.photo_url or avatar_url
            # Bump last_seen so we have a coarse "active recently" record.
            user_row.last_seen = _now()
            await session.commit()

    kicked = presence.subscribe(websocket, client_id, uid, name or "Anonymous", avatar_url)
    if kicked is not None:
        # Tell the kicked socket why they got bumped, in case it's still
        # alive (different device same UID).
        await _send(
            kicked.websocket,
            _err("session_replaced", "Signed in from another device"),
        )

    # Broadcast our arrival to everyone else.
    await _broadcast(
        {
            "type": "presence_join",
            "uid": uid,
            "name": name or "",
            "avatar_url": avatar_url,
        },
        exclude_uid=uid,
    )

    # Build + send the initial snapshot.
    async with _session_scope() as session:
        snapshot = await _build_snapshot(session, uid)
    snapshot["type"] = "social_snapshot"
    await _send(websocket, snapshot)


async def handle_social_disconnect(client_id: str) -> None:
    """Called from main.handle_disconnect for every WS that drops.
    Idempotent if the client never sent social_subscribe."""
    kicked = presence.unsubscribe_by_client(client_id)
    if kicked is None:
        return
    await _broadcast({"type": "presence_leave", "uid": kicked.uid})


async def handle_presence_ping(client_id: str, websocket: WebSocket, msg: dict) -> None:
    presence.touch(client_id)


async def handle_social_fcm_register(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    """Map the WS-authenticated UID to the device's FCM token so we
    can push DM notifications when the WS isn't connected. Client
    sends this once after social_subscribe completes."""
    uid = presence.uid_for_client(client_id)
    if uid is None:
        return
    token = (msg.get("token") or "").strip()
    if not token:
        return
    _fcm_tokens_by_uid[uid] = token
    print(f"[social] FCM token registered for uid={uid[:8]}")


async def handle_lounge_send(client_id: str, websocket: WebSocket, msg: dict) -> None:
    uid = presence.uid_for_client(client_id)
    if uid is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return

    text = (msg.get("text") or "").strip() or None
    np_video_id = (msg.get("np_video_id") or "").strip() or None
    if text is None and np_video_id is None:
        await _send(websocket, _err("empty_message", "Provide text or np_video_id"))
        return

    reply_to_id = msg.get("reply_to_message_id")
    if reply_to_id is not None and not isinstance(reply_to_id, int):
        reply_to_id = None

    entry = presence.get(uid)
    if entry is None:
        return

    async with _session_scope() as session:
        row = models.LoungeMessage(
            from_uid=uid,
            from_name=entry.name,
            from_avatar_url=entry.avatar_url,
            text=text,
            np_video_id=np_video_id,
            np_title=msg.get("np_title"),
            np_artist=msg.get("np_artist"),
            np_thumbnail=msg.get("np_thumbnail"),
            reply_to_message_id=reply_to_id,
            reactions={},
            sent_at=_now(),
        )
        session.add(row)
        await session.commit()
        await session.refresh(row)
        payload = _lounge_to_dict(row)

    payload["type"] = "lounge_message"
    await _broadcast(payload)


async def _fetch_lounge_message(
    session: AsyncSession, message_id: int
) -> models.LoungeMessage | None:
    return (
        await session.execute(
            select(models.LoungeMessage).where(models.LoungeMessage.id == message_id)
        )
    ).scalar_one_or_none()


async def handle_lounge_react(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    message_id = msg.get("message_id")
    emoji = (msg.get("emoji") or "").strip()
    if not isinstance(message_id, int) or not emoji:
        await _send(websocket, _err("bad_request", "message_id + emoji required"))
        return

    async with _session_scope() as session:
        m = await _fetch_lounge_message(session, message_id)
        if m is None:
            return
        reactions = dict(m.reactions or {})
        bucket = list(reactions.get(emoji, []))
        if me not in bucket:
            bucket.append(me)
        reactions[emoji] = bucket
        m.reactions = reactions
        await session.commit()
        updated = dict(reactions)

    await _broadcast({
        "type": "lounge_message_reactions",
        "message_id": message_id,
        "reactions": updated,
    })


async def handle_lounge_unreact(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        return
    message_id = msg.get("message_id")
    emoji = (msg.get("emoji") or "").strip()
    if not isinstance(message_id, int) or not emoji:
        return

    async with _session_scope() as session:
        m = await _fetch_lounge_message(session, message_id)
        if m is None:
            return
        reactions = dict(m.reactions or {})
        bucket = [u for u in reactions.get(emoji, []) if u != me]
        if bucket:
            reactions[emoji] = bucket
        else:
            reactions.pop(emoji, None)
        m.reactions = reactions
        await session.commit()
        updated = dict(reactions)

    await _broadcast({
        "type": "lounge_message_reactions",
        "message_id": message_id,
        "reactions": updated,
    })


async def handle_dm_send(client_id: str, websocket: WebSocket, msg: dict) -> None:
    sender_uid = presence.uid_for_client(client_id)
    if sender_uid is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return

    recipient_uid = (msg.get("to_uid") or "").strip()
    if not recipient_uid:
        await _send(websocket, _err("bad_request", "to_uid required"))
        return
    if recipient_uid == sender_uid:
        await _send(websocket, _err("bad_request", "Cannot DM yourself"))
        return

    text = (msg.get("text") or "").strip() or None
    np_video_id = (msg.get("np_video_id") or "").strip() or None
    share_moment = msg.get("share_moment")
    if not isinstance(share_moment, dict):
        share_moment = None
    if text is None and np_video_id is None and share_moment is None:
        await _send(
            websocket,
            _err("empty_message", "Provide text, np_video_id, or share_moment"),
        )
        return

    reply_to_id = msg.get("reply_to_message_id")
    if reply_to_id is not None and not isinstance(reply_to_id, int):
        reply_to_id = None

    sender = presence.get(sender_uid)
    if sender is None:
        return

    async with _session_scope() as session:
        # Verify recipient exists (FK would also catch this but a friendly
        # error is nicer than a 500).
        exists = (
            await session.execute(
                select(models.User.id).where(models.User.id == recipient_uid)
            )
        ).scalar_one_or_none()
        if exists is None:
            await _send(websocket, _err("recipient_not_found", "Unknown recipient"))
            return

        # Gate logic: if no thread state OR pending, this is a request.
        # If accepted, this is a normal DM. If declined-by-recipient,
        # we still STORE the message (sender never knows they were
        # declined — soft block) but DO NOT deliver it over WS.
        state_row, _ = await _ensure_thread_state(session, sender_uid, recipient_uid)
        gate_state = "pending"
        delivered_to_recipient = False
        if state_row.state == "accepted":
            gate_state = "accepted"
            delivered_to_recipient = True
        elif state_row.state in ("declined_by_a", "declined_by_b"):
            # Soft block — sender doesn't know.
            gate_state = "declined"
            delivered_to_recipient = False
        else:
            gate_state = "pending"
            delivered_to_recipient = True

        message = models.DmMessage(
            from_uid=sender_uid,
            to_uid=recipient_uid,
            text=text,
            np_video_id=np_video_id,
            np_title=msg.get("np_title"),
            np_artist=msg.get("np_artist"),
            np_thumbnail=msg.get("np_thumbnail"),
            reply_to_message_id=reply_to_id,
            share_moment=share_moment,
            reactions={},
            deleted=False,
            sent_at=_now(),
        )
        session.add(message)
        await session.commit()
        await session.refresh(message)

    base = _dm_to_dict(message, with_gate_state=gate_state)
    base["from_name"] = sender.name
    base["from_avatar_url"] = sender.avatar_url

    # Echo to the sender so their own client renders the outgoing message
    # without waiting for snapshot.
    sender_payload = dict(base)
    sender_payload["type"] = "dm_message"
    await _send(websocket, sender_payload)

    # Deliver to recipient if not soft-blocked. If their WS isn't
    # connected, fall back to an FCM data push so their device wakes
    # up and shows a notification.
    if delivered_to_recipient:
        recipient_payload = dict(base)
        recipient_payload["type"] = "dm_message"
        delivered_over_ws = await _send_to_uid(recipient_uid, recipient_payload)
        if not delivered_over_ws:
            await _push_dm_fcm(
                recipient_uid=recipient_uid,
                sender_uid=sender_uid,
                sender_name=sender.name,
                preview=text or "Shared a song",
            )


async def handle_dm_accept(client_id: str, websocket: WebSocket, msg: dict) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    peer = (msg.get("peer_uid") or "").strip()
    if not peer:
        await _send(websocket, _err("bad_request", "peer_uid required"))
        return

    a, b = _ordered_pair(me, peer)
    async with _session_scope() as session:
        state_row = await _get_thread_state(session, me, peer)
        if state_row is None:
            await _send(websocket, _err("no_thread", "Nothing to accept"))
            return
        if state_row.state == "accepted":
            return  # Idempotent.

        state_row.state = "accepted"
        state_row.updated_at = _now()

        # Upsert the Friendship row.
        await session.execute(
            pg_insert(models.Friendship)
            .values(uid_a=a, uid_b=b, formed_at=_now())
            .on_conflict_do_nothing(index_elements=["uid_a", "uid_b"])
        )
        await session.commit()

    # Notify both sides so their UIs move the thread out of Requests.
    await _send_to_uid(me, {"type": "dm_friendship_formed", "peer_uid": peer})
    await _send_to_uid(peer, {"type": "dm_friendship_formed", "peer_uid": me})


async def handle_dm_decline(client_id: str, websocket: WebSocket, msg: dict) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    peer = (msg.get("peer_uid") or "").strip()
    if not peer:
        await _send(websocket, _err("bad_request", "peer_uid required"))
        return

    a, b = _ordered_pair(me, peer)
    new_state = "declined_by_a" if me == a else "declined_by_b"

    async with _session_scope() as session:
        state_row = await _get_thread_state(session, me, peer)
        if state_row is None:
            await _send(websocket, _err("no_thread", "Nothing to decline"))
            return
        if state_row.state == "accepted":
            # Already friends — declining is a no-op here (the user
            # would expect "block" semantics which we don't have v1).
            return
        state_row.state = new_state
        state_row.updated_at = _now()
        await session.commit()

    # Recipient (us) sees no broadcast — the soft-block requires the
    # sender to remain in the dark. The recipient's own UI updates
    # optimistically on send.


async def handle_dm_read(client_id: str, websocket: WebSocket, msg: dict) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        return
    peer = (msg.get("peer_uid") or "").strip()
    up_to = msg.get("up_to_message_id")
    if not peer:
        return

    async with _session_scope() as session:
        q = update(models.DmMessage).where(
            models.DmMessage.to_uid == me,
            models.DmMessage.from_uid == peer,
            models.DmMessage.read_at.is_(None),
        )
        if isinstance(up_to, int):
            q = q.where(models.DmMessage.id <= up_to)
        q = q.values(read_at=_now())
        await session.execute(q)
        await session.commit()


# ─────────────────────────────────────────────────────────────────────
# DM chat-parity handlers (reactions, reply, edit, delete, share-moment,
# typing). Mirror the room-chat capability set on top of dm_messages.
# ─────────────────────────────────────────────────────────────────────


async def _fetch_dm_message(
    session: AsyncSession, message_id: int
) -> models.DmMessage | None:
    return (
        await session.execute(
            select(models.DmMessage).where(models.DmMessage.id == message_id)
        )
    ).scalar_one_or_none()


def _other_uid(m: models.DmMessage, me: str) -> str:
    return m.to_uid if m.from_uid == me else m.from_uid


async def _broadcast_to_pair(payload: dict, me: str, peer: str) -> None:
    """Send the same payload to both ends of a DM thread. Used by
    reactions / edits / deletes / typing so both views stay in sync."""
    await _send_to_uid(me, payload)
    await _send_to_uid(peer, payload)


async def handle_dm_chat_react(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    message_id = msg.get("message_id")
    emoji = (msg.get("emoji") or "").strip()
    if not isinstance(message_id, int) or not emoji:
        await _send(websocket, _err("bad_request", "message_id + emoji required"))
        return

    async with _session_scope() as session:
        m = await _fetch_dm_message(session, message_id)
        if m is None or (m.from_uid != me and m.to_uid != me):
            await _send(websocket, _err("not_found", "Message not in your thread"))
            return
        # Mutate the JSONB dict in Python then re-assign so SQLAlchemy
        # marks it dirty (it doesn't track in-place dict mutations).
        reactions = dict(m.reactions or {})
        bucket = list(reactions.get(emoji, []))
        if me not in bucket:
            bucket.append(me)
        reactions[emoji] = bucket
        m.reactions = reactions
        await session.commit()
        peer = _other_uid(m, me)
        updated = dict(reactions)

    await _broadcast_to_pair(
        {
            "type": "dm_message_reactions",
            "message_id": message_id,
            "reactions": updated,
        },
        me,
        peer,
    )


async def handle_dm_chat_unreact(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    message_id = msg.get("message_id")
    emoji = (msg.get("emoji") or "").strip()
    if not isinstance(message_id, int) or not emoji:
        return

    async with _session_scope() as session:
        m = await _fetch_dm_message(session, message_id)
        if m is None or (m.from_uid != me and m.to_uid != me):
            return
        reactions = dict(m.reactions or {})
        bucket = [u for u in reactions.get(emoji, []) if u != me]
        if bucket:
            reactions[emoji] = bucket
        else:
            reactions.pop(emoji, None)
        m.reactions = reactions
        await session.commit()
        peer = _other_uid(m, me)
        updated = dict(reactions)

    await _broadcast_to_pair(
        {
            "type": "dm_message_reactions",
            "message_id": message_id,
            "reactions": updated,
        },
        me,
        peer,
    )


async def handle_dm_chat_edit(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    message_id = msg.get("message_id")
    new_text = (msg.get("text") or "").strip()
    if not isinstance(message_id, int) or not new_text:
        await _send(websocket, _err("bad_request", "message_id + text required"))
        return

    async with _session_scope() as session:
        m = await _fetch_dm_message(session, message_id)
        if m is None or m.from_uid != me:
            await _send(websocket, _err("not_allowed", "Can only edit own messages"))
            return
        if m.deleted:
            return
        m.text = new_text
        m.edited_at = _now()
        await session.commit()
        peer = _other_uid(m, me)
        edited_at_ms = _to_ms(m.edited_at)

    await _broadcast_to_pair(
        {
            "type": "dm_message_edited",
            "message_id": message_id,
            "text": new_text,
            "edited_at": edited_at_ms,
        },
        me,
        peer,
    )


async def handle_dm_chat_delete(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    message_id = msg.get("message_id")
    if not isinstance(message_id, int):
        return

    async with _session_scope() as session:
        m = await _fetch_dm_message(session, message_id)
        if m is None or m.from_uid != me:
            await _send(websocket, _err("not_allowed", "Can only delete own messages"))
            return
        m.deleted = True
        await session.commit()
        peer = _other_uid(m, me)

    await _broadcast_to_pair(
        {"type": "dm_message_deleted", "message_id": message_id},
        me,
        peer,
    )


async def handle_dm_chat_typing(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    me = presence.uid_for_client(client_id)
    if me is None:
        return
    peer = (msg.get("peer_uid") or "").strip()
    is_typing = bool(msg.get("isTyping", True))
    if not peer:
        return
    # Typing has no DB side-effect. Just notify the peer.
    await _send_to_uid(
        peer,
        {
            "type": "dm_typing",
            "from_uid": me,
            "is_typing": is_typing,
        },
    )


# ─────────────────────────────────────────────────────────────────────
# Error envelope
# ─────────────────────────────────────────────────────────────────────


def _err(code: str, message: str) -> dict:
    return {"type": "social_error", "code": code, "message": message}


# ─────────────────────────────────────────────────────────────────────
# Periodic prune (lounge rolling-200 + DM post-read sweep)
# ─────────────────────────────────────────────────────────────────────


async def prune_loop(interval_seconds: int = 300) -> None:
    """Run forever — trims old lounge messages past the 200-message
    window and deletes DMs that have been read for >1h.

    Triggered from main.py's startup."""
    while True:
        try:
            async with _session_scope() as session:
                # Lounge: keep last 200 by sent_at. Faster + simpler than
                # a window-based trigger and runs once every 5 minutes.
                await session.execute(
                    delete(models.LoungeMessage).where(
                        models.LoungeMessage.id.notin_(
                            select(models.LoungeMessage.id)
                            .order_by(models.LoungeMessage.sent_at.desc())
                            .limit(200)
                        )
                    )
                )

                # DMs: clear out anything read > 1 hour ago.
                cutoff = _now() - timedelta(hours=1)
                await session.execute(
                    delete(models.DmMessage).where(
                        models.DmMessage.read_at.is_not(None),
                        models.DmMessage.read_at < cutoff,
                    )
                )

                await session.commit()
        except Exception as e:
            print(f"[social] prune failed: {e}")
        await asyncio.sleep(interval_seconds)


# ─────────────────────────────────────────────────────────────────────
# REST endpoints
# ─────────────────────────────────────────────────────────────────────


@router.get("/snapshot")
async def get_snapshot(
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(db.get_session),
):
    """Same payload as the `social_snapshot` WS message. Used by the
    client to paint the People/Lounge tabs on cold open before the WS
    handshake completes."""
    return await _build_snapshot(session, user["uid"])


@router.get("/friends")
async def get_friends(
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(db.get_session),
):
    """Just the friends list (no lounge / no DMs). Cheap call for the
    People tab's pinned strip when the client doesn't need the full
    snapshot."""
    return {"friends": await _friend_summaries(session, user["uid"])}
