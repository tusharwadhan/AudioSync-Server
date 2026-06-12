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
import os
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

    def set_avatar(self, uid: str, avatar_url: str | None) -> None:
        """Update the cached avatar_url for an online user. Called by
        broadcast_user_avatar_changed so the in-memory presence entry
        (which downstream sites read for lounge bake / dm_message
        from_avatar_url / online list) stays in sync with the DB
        without forcing a reconnect."""
        entry = self._by_uid.get(uid)
        if entry is not None:
            entry.avatar_url = avatar_url


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
    sender_avatar_url: str | None = None,
) -> None:
    """Send a data-only FCM message for a DM the recipient missed
    because they weren't connected. Best-effort, but every branch
    logs explicitly so the Render dashboard makes the failure mode
    obvious when a user reports "no notification while offline"."""
    print(
        f"[social] FCM attempt -> recipient={recipient_uid[:8]} "
        f"sender={sender_uid[:8]} (tokens_map_size={len(_fcm_tokens_by_uid)})"
    )
    # R2 mute gate: skip the push entirely if the recipient has
    # muted the sender. In-app delivery / unread badge are
    # unaffected — only the lock-screen notification is suppressed.
    try:
        async with _session_scope() as session:
            if await _is_muted(session, recipient_uid, sender_uid):
                print(
                    f"[social] FCM skipped (muted) -> recipient={recipient_uid[:8]} "
                    f"sender={sender_uid[:8]}"
                )
                return
    except Exception as e:
        print(f"[social] FCM mute-check failed for {recipient_uid[:8]} (proceeding): {e}")
    token = _fcm_tokens_by_uid.get(recipient_uid)
    if not token:
        # Cold in-memory cache (e.g. right after a deploy). Fall back to
        # the token persisted on the user row so offline recipients still
        # get pushed, then warm the cache for next time.
        try:
            async with _session_scope() as session:
                token = (
                    await session.execute(
                        select(models.User.fcm_token).where(
                            models.User.id == recipient_uid
                        )
                    )
                ).scalar_one_or_none()
        except Exception as e:
            print(f"[social] FCM token DB lookup failed for {recipient_uid[:8]}: {e}")
            token = None
        if token:
            _fcm_tokens_by_uid[recipient_uid] = token
            print(f"[social] FCM token loaded from DB for {recipient_uid[:8]}")
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
        data = {
            "type": "dm",
            "from_uid": sender_uid,
            "from_name": sender_name[:120],
            "preview": (preview or "")[:200],
        }
        if sender_avatar_url:
            # Carry the avatar so the recipient's notification can render
            # the sender's face (drives Android 11+ Conversation classification).
            data["from_avatar_url"] = sender_avatar_url[:512]
        message = _fcm_messaging.Message(
            data=data,
            token=token,
            android=_fcm_messaging.AndroidConfig(priority="high"),
        )
        message_id = await asyncio.to_thread(_fcm_messaging.send, message)
        print(
            f"[social] FCM DM push OK -> {recipient_uid[:8]} "
            f"message_id={message_id}"
        )
    except _fcm_messaging.UnregisteredError:
        # Stale token — drop from the map AND clear the persisted copy so
        # we don't keep retrying a dead token after the next restart.
        _fcm_tokens_by_uid.pop(recipient_uid, None)
        try:
            async with _session_scope() as session:
                await session.execute(
                    update(models.User)
                    .where(models.User.id == recipient_uid)
                    .values(fcm_token=None, fcm_token_updated_at=_now())
                )
                await session.commit()
        except Exception as e:
            print(f"[social] FCM stale token DB clear failed for {recipient_uid[:8]}: {e}")
        print(f"[social] FCM stale token removed for {recipient_uid[:8]}")
    except Exception as e:
        print(f"[social] FCM DM push FAILED for {recipient_uid[:8]}: {e}")


# ─────────────────────────────────────────────────────────────────────
# WS send helpers
# ─────────────────────────────────────────────────────────────────────


async def _send(ws: WebSocket, msg: dict) -> bool:
    """Returns True iff the WS send actually completed. False on any
    exception (audit finding #11: callers relying on the return
    value were previously fooled into thinking a stale socket
    successfully delivered, suppressing FCM fallback)."""
    try:
        await ws.send_text(json.dumps(msg))
        return True
    except Exception:
        # Connection probably dropped; cleanup happens in the receive
        # loop's WebSocketDisconnect path.
        return False


async def _send_to_uid(uid: str, msg: dict) -> bool:
    """Returns True only if the message ACTUALLY went over an open
    WS. A False return means the FCM fallback path should run."""
    entry = presence.get(uid)
    if entry is None:
        return False
    return await _send(entry.websocket, msg)


async def _broadcast(msg: dict, exclude_uid: str | None = None) -> None:
    payload = json.dumps(msg)
    tasks = []
    for entry in presence.all_entries():
        if exclude_uid and entry.uid == exclude_uid:
            continue
        tasks.append(_safe_send_text(entry.websocket, payload))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _broadcast_to_audience(uids: list[str], msg: dict) -> None:
    """Send `msg` to every online uid in the list. Offline uids are
    silently skipped — they pick up the change via their next
    snapshot fetch. Used for fan-outs that should NOT reach the
    full presence list (e.g. avatar/status updates which are
    friends-only)."""
    if not uids:
        return
    payload = json.dumps(msg)
    tasks = []
    for uid in uids:
        entry = presence.get(uid)
        if entry is not None:
            tasks.append(_safe_send_text(entry.websocket, payload))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def photo_url_with_version(url: str | None, photo_updated_at: int | None) -> str | None:
    """Append `?v={photo_updated_at}` (or `?v=0` if unset) so Coil's
    URL-keyed memory cache + DmAvatarCache's disk cache treat any
    avatar change as a cache miss. Firebase Storage doesn't rotate
    its download-URL tokens on blob overwrite, so this is the only
    reliable invalidation signal we have.

    Returns None if url is None. Idempotent: if the URL already
    contains a `v=` param we leave it alone — caller has already
    versioned it."""
    if not url:
        return None
    if "v=" in url:
        return url
    sep = "&" if "?" in url else "?"
    v = photo_updated_at or 0
    return f"{url}{sep}v={v}"


async def broadcast_user_avatar_changed(
    uid: str,
    photo_url: str | None,
    photo_updated_at: int | None,
) -> None:
    """Fan out a `user_avatar_changed` event to the user's friends
    ONLY. Audience scoping is privacy-sensitive: broadcasting to
    pending-DM requesters would leak online presence to strangers
    (they'd see WS traffic whenever the user edits their avatar,
    confirming the user is currently online). Pending requesters
    catch up on their next snapshot fetch.

    Self-broadcast is included so the user's other devices stay in
    sync.
    """
    versioned = photo_url_with_version(photo_url, photo_updated_at)
    msg = {
        "type": "user_avatar_changed",
        "uid": uid,
        "photo_url": versioned,
    }
    # Keep the in-memory presence entry consistent with the DB so
    # that future lounge messages, dm_message bake-ins, and
    # online_summaries all emit the new URL without requiring a
    # reconnect from the user.
    presence.set_avatar(uid, versioned)
    async with _session_scope() as session:
        friend_uids = await _friend_uids(session, uid)
    audience = list(friend_uids | {uid})
    print(f"[user_avatar_changed] uid={uid} audience={len(audience)}")
    await _broadcast_to_audience(audience, msg)


async def broadcast_user_status_changed(
    uid: str,
    status_text: str | None,
) -> None:
    """R2 sibling of broadcast_user_avatar_changed. Fans `user_status_changed`
    out to the user's friends ONLY (plus self for cross-device sync).
    """
    msg = {
        "type": "user_status_changed",
        "uid": uid,
        "status_text": status_text,
    }
    async with _session_scope() as session:
        friend_uids = await _friend_uids(session, uid)
    audience = list(friend_uids | {uid})
    print(f"[user_status_changed] uid={uid} audience={len(audience)}")
    await _broadcast_to_audience(audience, msg)


async def _muted_uids_for(session: AsyncSession, user_uid: str) -> set[str]:
    """Return the set of peer uids that `user_uid` has muted. Used by
    snapshot (returned as `muted_peers`) and by `_push_dm_fcm` to skip
    pushes from muted senders."""
    rows = (
        await session.execute(
            select(models.MutedPeer.peer_uid)
            .where(models.MutedPeer.user_uid == user_uid)
        )
    ).scalars().all()
    return set(rows)


async def _is_muted(
    session: AsyncSession,
    user_uid: str,
    peer_uid: str,
) -> bool:
    """True iff `user_uid` has muted `peer_uid` for DM notifications."""
    row = (
        await session.execute(
            select(models.MutedPeer.user_uid).where(
                (models.MutedPeer.user_uid == user_uid) &
                (models.MutedPeer.peer_uid == peer_uid)
            )
        )
    ).scalar_one_or_none()
    return row is not None


async def _chat_clear_cutoffs(
    session: AsyncSession,
    user_uid: str,
) -> dict[str, int]:
    """Return `{peer_uid: cleared_before_ts (epoch ms)}` for every
    thread the caller has issued a clear-chat on. Snapshot uses this
    to filter all three DM arrays."""
    rows = (
        await session.execute(
            select(models.ChatClear.peer_uid, models.ChatClear.cleared_before_ts)
            .where(models.ChatClear.user_uid == user_uid)
        )
    ).all()
    return {peer: ts for (peer, ts) in rows}


async def _friend_uids(session: AsyncSession, uid: str) -> set[str]:
    """Return uids of all accepted friends of `uid`. Used by
    broadcast helpers that need a friends-only audience.

    Returns a set so callers don't accidentally double-send a WS
    event if the friendships table ever has duplicate rows (audit
    finding #14). Callers can still take the | union with {uid} for
    self-broadcast without an extra wrap.
    """
    pair_rows = (
        await session.execute(
            select(models.Friendship.uid_a, models.Friendship.uid_b)
            .where(
                (models.Friendship.uid_a == uid) | (models.Friendship.uid_b == uid)
            )
        )
    ).all()
    out: set[str] = set()
    for (a, b) in pair_rows:
        out.add(b if a == uid else a)
    return out


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
    """Return [{uid, name, avatar_url, is_online, last_seen_ms}] for all
    friends of uid."""
    # JOIN against users to pick up display_name + photo_url + last_seen.
    # Friendship rows store the pair with uid_a < uid_b, so the friend's
    # uid is whichever side isn't `uid`.
    other_col = func.coalesce(
        func.nullif(models.Friendship.uid_a, uid), models.Friendship.uid_b
    ).label("friend_uid")
    stmt = (
        select(
            other_col,
            models.User.display_name,
            models.User.photo_url,
            models.User.photo_updated_at,
            models.User.status_text,
            models.User.last_seen,
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
            # Cache-busted so Coil + DmAvatarCache treat URL changes
            # as misses. See photo_url_with_version() docstring.
            "avatar_url": photo_url_with_version(photo_url, photo_updated_at),
            "status_text": status_text,
            "is_online": presence.is_online(friend_uid),
            "last_seen_ms": _to_ms(last_seen),
        }
        for (friend_uid, display_name, photo_url, photo_updated_at, status_text, last_seen) in rows
    ]


def _dm_to_dict(
    m: models.DmMessage,
    *,
    with_gate_state: str | None = None,
    user_lookup: dict[str, tuple[str | None, str | None, int | None, str | None]] | None = None,
) -> dict:
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
        "event_type": m.event_type,
        # R4 view-once fields (migration 0012). Both NULL for
        # plain text DMs. photo_url goes NULL atomically with
        # view_once_status='opened' (server-side delete handler).
        # Suppress for non-accepted gate (final-deep-audit fix #5):
        # if a friendship is dissolved + a new pending thread forms,
        # a historical 'sent' photo row could re-surface in the
        # stranger's snapshot. Belt-and-braces — handle_dm_send
        # already blocks new view-once across pending gates.
        "photo_url": m.photo_url if with_gate_state in (None, "accepted") else None,
        "view_once_status": m.view_once_status if with_gate_state in (None, "accepted") else None,
    }
    if with_gate_state is not None:
        d["gate_state"] = with_gate_state
    # Sender identity for the snapshot's pending_requests etc., so
    # the client doesn't have to fall back to "Message request" when
    # rendering rows for senders it can't otherwise identify.
    # Cache-buster appended via photo_url_with_version so any avatar
    # change forces Coil + DmAvatarCache to refetch.
    if user_lookup is not None:
        name, avatar, updated_at, status_text = user_lookup.get(
            m.from_uid, (None, None, None, None)
        )
        d["from_name"] = name
        d["from_avatar_url"] = photo_url_with_version(avatar, updated_at)
        # Status broadcasts are friends-only (broadcast_user_status_changed
        # restricts the audience to actual friends). The snapshot must
        # honour the same scope — a stranger sending you their first DM
        # should NOT leak their status_text in the pending-request row
        # (audit finding #3). Once the gate flips to accepted, status
        # rides along with the dm_message payload.
        if with_gate_state in (None, "accepted"):
            d["from_status_text"] = status_text
        else:
            d["from_status_text"] = None
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

    # Build a uid -> (display_name, photo_url) lookup for every
    # sender referenced in the snapshot so client rows can render
    # real names (and avatars) without their own per-message
    # JOIN. Especially important for pending stranger requests
    # where the recipient has never seen the sender otherwise.
    sender_uids = {m.from_uid for m in incoming} | {m.from_uid for m in sent_pending}
    user_lookup: dict[str, tuple[str | None, str | None, int | None, str | None]] = {}
    if sender_uids:
        user_rows = (
            await session.execute(
                select(
                    models.User.id,
                    models.User.display_name,
                    models.User.photo_url,
                    models.User.photo_updated_at,
                    models.User.status_text,
                )
                .where(models.User.id.in_(sender_uids))
            )
        ).all()
        for u_id, dname, photo, updated_at, status_text in user_rows:
            user_lookup[u_id] = (dname, photo, updated_at, status_text)

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
    mode_by_peer: dict[str, str] = {}
    for ts in thread_states:
        peer = ts.uid_b if ts.uid_a == uid else ts.uid_a
        state_by_peer[peer] = ts.state
        mode_by_peer[peer] = ts.retention_mode or "keep"

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
                pending_requests.append(
                    _dm_to_dict(m, with_gate_state="pending", user_lookup=user_lookup)
                )
            else:
                unread_dms.append(
                    _dm_to_dict(m, with_gate_state="accepted", user_lookup=user_lookup)
                )
        elif state in ("pending_from_a", "pending_from_b"):
            pending_requests.append(
                _dm_to_dict(m, with_gate_state="pending", user_lookup=user_lookup)
            )
        elif state in ("declined_by_a", "declined_by_b"):
            # Keep only the newest per peer.
            existing = declined_latest.get(peer)
            if existing is None or m.sent_at > existing.sent_at:
                declined_latest[peer] = m

    for m in declined_latest.values():
        pending_requests.append(
            _dm_to_dict(m, with_gate_state="declined", user_lookup=user_lookup)
        )

    # Outgoing-still-unread (for the sender's view of their own
    # in-flight messages). Group by peer for the client.
    sent_unread = [
        _dm_to_dict(m, with_gate_state="accepted", user_lookup=user_lookup)
        for m in sent_pending
    ]

    # Keep-mode threads: include recent READ history (both directions) so
    # the client can rebuild the full conversation on launch, not just the
    # unread tail. Disappear threads intentionally return unread only.
    keep_peers = {p for p, mode in mode_by_peer.items() if mode == "keep"}
    history: list[dict] = []
    if keep_peers:
        hist_rows = (
            await session.execute(
                select(models.DmMessage)
                .where(
                    or_(
                        models.DmMessage.from_uid == uid,
                        models.DmMessage.to_uid == uid,
                    ),
                    models.DmMessage.read_at.is_not(None),
                    or_(
                        models.DmMessage.from_uid.in_(keep_peers),
                        models.DmMessage.to_uid.in_(keep_peers),
                    ),
                )
                .order_by(models.DmMessage.sent_at.desc())
                .limit(500)
            )
        ).scalars().all()
        # Ensure sender names resolve for history rows too.
        missing = {m.from_uid for m in hist_rows} - set(user_lookup.keys())
        if missing:
            extra = (
                await session.execute(
                    select(
                        models.User.id,
                        models.User.display_name,
                        models.User.photo_url,
                        models.User.photo_updated_at,
                        models.User.status_text,
                    ).where(models.User.id.in_(missing))
                )
            ).all()
            for u_id, dname, photo, updated_at, status_text in extra:
                user_lookup[u_id] = (dname, photo, updated_at, status_text)
        history = [
            _dm_to_dict(m, with_gate_state="accepted", user_lookup=user_lookup)
            for m in reversed(hist_rows)
        ]

    # Per-peer one-sided "clear chat" cutoffs (R2). Applied to ALL
    # three DM arrays so a reconnect / re-snapshot doesn't bring back
    # messages the user has cleared from their own side. Peer's
    # snapshot is unaffected (asymmetric clear, WhatsApp-style).
    clear_cutoffs = await _chat_clear_cutoffs(session, uid)
    def _after_clear(m: dict) -> bool:
        cutoff = clear_cutoffs.get(_peer_of(m, uid))
        return cutoff is None or (m.get("sent_at") or 0) > cutoff
    unread_dms = [m for m in unread_dms if _after_clear(m)]
    sent_unread = [m for m in sent_unread if _after_clear(m)]
    history = [m for m in history if _after_clear(m)]
    # Pending requests need the same treatment (audit finding #5b) —
    # otherwise a stranger DM cleared before acceptance reappears
    # on every reconnect.
    pending_requests = [m for m in pending_requests if _after_clear(m)]

    # Set of peer uids the caller has muted for DM push notifications
    # (R2). In-app delivery + unread counts are unaffected by mute —
    # this is purely a notification suppression list. Client uses it
    # to drive the three-dot menu's Mute / Unmute toggle state.
    muted_peers = sorted(await _muted_uids_for(session, uid))

    return {
        "online": online,
        "friends": friends,
        "lounge": lounge,
        "unread_dms": unread_dms,
        "pending_requests": pending_requests,
        "sent_unread_dms": sent_unread,
        # Read history for keep-mode threads (empty for disappear threads).
        "history_dms": history,
        # Per-peer retention mode so the client header reflects it.
        "dm_modes": mode_by_peer,
        # R2: muted peers (DM notification suppression list).
        "muted_peers": muted_peers,
        # R2: per-peer clear-chat cutoffs so the client can hide local
        # echo of older messages it doesn't have in the snapshot
        # (e.g. messages that haven't been GC'd yet on the server).
        "chat_clears": clear_cutoffs,
        "server_time": _to_ms(_now()),
    }


def _peer_of(dm_dict: dict, self_uid: str) -> str:
    """Return the other party in a DM dict from `self_uid`'s POV.
    Used by the clear-chat filter — we key cutoffs by peer."""
    return dm_dict["to_uid"] if dm_dict.get("from_uid") == self_uid else dm_dict.get("from_uid", "")


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
            # Cache-busted at presence-entry time so every downstream
            # read (presence_join broadcast, online list, lounge bake,
            # dm_message from_avatar_url at send time) gets a URL Coil
            # will treat as fresh on the next avatar update.
            avatar_url = photo_url_with_version(
                user_row.photo_url or avatar_url,
                user_row.photo_updated_at,
            )
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
    # Stamp last_seen on the user row so friends' clients can show
    # "last seen X ago" in their DM header subtitles.
    try:
        async with _session_scope() as session:
            await session.execute(
                update(models.User)
                .where(models.User.id == kicked.uid)
                .values(last_seen=_now())
            )
            await session.commit()
    except Exception as e:
        print(f"[social] last_seen update failed for {kicked.uid[:8]}: {e}")
    await _broadcast({"type": "presence_leave", "uid": kicked.uid})


async def handle_dm_set_mute(client_id: str, websocket: WebSocket, msg: dict) -> None:
    """R2: toggle the caller's notification-mute on a specific peer.

    Mute is one-way + asymmetric — only the CALLER's lock-screen
    push from that peer is suppressed. In-app delivery + unread
    counts are unaffected. Peer never learns they were muted (no
    broadcast to them).

    Reply is sent ONLY to the caller (`dm_mute_changed`) so their
    other devices learn about the toggle and the local UI can flip
    the menu item without waiting for a fresh snapshot.

    Idempotent via `ON CONFLICT DO NOTHING` — safe to call twice
    from two devices without the second hitting a PK violation
    (audit finding #4).
    """
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    peer_uid = msg.get("peer_uid")
    if not isinstance(peer_uid, str) or not peer_uid or peer_uid == me:
        await _send(websocket, _err("bad_request", "peer_uid required"))
        return
    muted = bool(msg.get("muted", False))
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    async with _session_scope() as session:
        if muted:
            stmt = pg_insert(models.MutedPeer.__table__).values(
                user_uid=me, peer_uid=peer_uid, muted_at=_now(),
            ).on_conflict_do_nothing(
                index_elements=["user_uid", "peer_uid"],
            )
            await session.execute(stmt)
        else:
            await session.execute(
                models.MutedPeer.__table__.delete().where(
                    (models.MutedPeer.user_uid == me) &
                    (models.MutedPeer.peer_uid == peer_uid)
                )
            )
        await session.commit()
    print(f"[dm_set_mute] user={me[:8]} peer={peer_uid[:8]} muted={muted}")
    # Echo to all of caller's connected devices (currently we only
    # track one WS per uid, but the helper handles both cases).
    await _send_to_uid(me, {
        "type": "dm_mute_changed",
        "peer_uid": peer_uid,
        "muted": muted,
    })


async def handle_dm_clear_chat(client_id: str, websocket: WebSocket, msg: dict) -> None:
    """R2: one-sided clear-chat. Upserts a cutoff for the (caller,
    peer) pair so the caller's snapshot filters out messages with
    `sent_at <= cleared_before_ts` from ALL THREE DM arrays
    (unread_dms, sent_unread_dms, history_dms). Peer's snapshot is
    unaffected — WhatsApp-style.

    A subsequent message in the thread appears normally; the cutoff
    is a one-shot, not a kill-switch on the conversation.
    """
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    peer_uid = msg.get("peer_uid")
    if not isinstance(peer_uid, str) or not peer_uid or peer_uid == me:
        await _send(websocket, _err("bad_request", "peer_uid required"))
        return
    cleared_before_ts = int(time.time() * 1000)
    # Upsert via ON CONFLICT DO UPDATE so concurrent calls from two
    # devices don't hit a PK violation (audit finding #4). The
    # cutoff is monotonically non-decreasing per (user, peer), so
    # last-write-wins is correct semantics.
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    async with _session_scope() as session:
        stmt = pg_insert(models.ChatClear.__table__).values(
            user_uid=me, peer_uid=peer_uid, cleared_before_ts=cleared_before_ts,
        ).on_conflict_do_update(
            index_elements=["user_uid", "peer_uid"],
            set_=dict(cleared_before_ts=cleared_before_ts),
        )
        await session.execute(stmt)
        await session.commit()
    print(f"[dm_clear_chat] user={me[:8]} peer={peer_uid[:8]} cutoff={cleared_before_ts}")
    await _send_to_uid(me, {
        "type": "dm_chat_cleared",
        "peer_uid": peer_uid,
        "cleared_before_ts": cleared_before_ts,
    })


async def handle_dm_unfriend(client_id: str, websocket: WebSocket, msg: dict) -> None:
    """Drop the friendship between caller and peer. Also blows away
    the thread state so a future dm_send goes through the gate
    again. Idempotent — no-ops if there's no friendship."""
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    peer = (msg.get("peer_uid") or "").strip()
    if not peer:
        return
    a, b = _ordered_pair(me, peer)
    async with _session_scope() as session:
        await session.execute(
            delete(models.Friendship).where(
                models.Friendship.uid_a == a,
                models.Friendship.uid_b == b,
            )
        )
        await session.execute(
            delete(models.DmThreadState).where(
                models.DmThreadState.uid_a == a,
                models.DmThreadState.uid_b == b,
            )
        )
        # Wipe undelivered DMs on both directions so the peer doesn't
        # see a phantom unread after being unfriended.
        await session.execute(
            delete(models.DmMessage).where(
                or_(
                    and_(models.DmMessage.from_uid == me, models.DmMessage.to_uid == peer),
                    and_(models.DmMessage.from_uid == peer, models.DmMessage.to_uid == me),
                )
            )
        )
        await session.commit()

    # Notify both sides so their UIs drop the friend chip + thread.
    payload = {"type": "dm_unfriended", "peer_uid_for_recipient": me, "by_uid": me}
    await _send_to_uid(peer, dict(payload, peer_uid_for_recipient=me))
    await _send_to_uid(me, dict(payload, peer_uid_for_recipient=peer))


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
    # Persist so the token survives a server restart/redeploy — otherwise
    # an offline user can't be pushed a DM until they reconnect the WS.
    try:
        async with _session_scope() as session:
            await session.execute(
                update(models.User)
                .where(models.User.id == uid)
                .values(fcm_token=token, fcm_token_updated_at=_now())
            )
            await session.commit()
    except Exception as e:
        print(f"[social] FCM token DB persist failed for uid={uid[:8]}: {e}")
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

    # R4 view-once photo. Empty text + view_once=True + photo_url
    # is a valid content kind. Validation runs BEFORE the
    # empty-message check below so the existing path is not
    # accidentally relaxed.
    view_once = bool(msg.get("view_once", False))
    photo_url = msg.get("photo_url")
    if photo_url is not None and not isinstance(photo_url, str):
        photo_url = None
    if view_once or photo_url:
        if not view_once or not photo_url:
            # Both must be set together — plain photos don't exist yet.
            await _send(websocket, _err("bad_request", "view_once requires photo_url and vice versa"))
            return
        # Reject view-once across a non-accepted gate so a stranger
        # can't push a one-time photo behind their first DM
        # (audit fix #12 — closes the snapshot URL leak attack).
        async with _session_scope() as gate_session:
            state_row = await _get_thread_state(gate_session, sender_uid, recipient_uid)
            current_state = state_row.state if state_row else None
        if current_state != "accepted":
            await _send(websocket, {
                "type": "social_error",
                "code": "view_once_requires_accepted_thread",
            })
            return
        # Verify the URL is on our Cloudinary cloud + under dm_photos/.
        cloud = os.environ.get("CLOUDINARY_CLOUD_NAME", "")
        expected_prefix = f"https://res.cloudinary.com/{cloud}/"
        if not cloud or not photo_url.startswith(expected_prefix) or "/dm_photos/" not in photo_url:
            await _send(websocket, _err("bad_photo_url", "Invalid photo URL"))
            return
        # Verify the Cloudinary asset's context binds it to
        # (sender, recipient). Without this, a stolen signature can
        # be replayed — attacker uploads to a public_id we signed
        # for someone else, then convinces user to send that URL
        # (audit fix #10).
        import cloudinary.api
        public_id = _public_id_from_dm_photo_url(photo_url)
        if public_id is None:
            await _send(websocket, _err("bad_photo_url", "Cannot parse public_id"))
            return
        try:
            resource = await asyncio.to_thread(
                cloudinary.api.resource, public_id, context=True
            )
        except Exception as e:
            print(f"[dm_send view_once] context fetch failed for {public_id}: {e}")
            await _send(websocket, _err("verify_failed", "Photo verification failed"))
            return
        ctx = (resource.get("context") or {}).get("custom", {})
        if ctx.get("from_uid") != sender_uid or ctx.get("to_uid") != recipient_uid:
            await _send(websocket, _err("context_mismatch", "Photo not bound to this conversation"))
            return

    if text is None and np_video_id is None and share_moment is None and not view_once:
        await _send(
            websocket,
            _err("empty_message", "Provide text, np_video_id, share_moment, or view-once photo"),
        )
        return

    reply_to_id = msg.get("reply_to_message_id")
    if reply_to_id is not None and not isinstance(reply_to_id, int):
        reply_to_id = None

    # Client-generated correlation id for optimistic UI. We echo it back
    # (only to the sender) so their client can swap the local "Sending"
    # bubble for the persisted message instead of appending a duplicate.
    client_nonce = msg.get("client_nonce")
    if client_nonce is not None and not isinstance(client_nonce, str):
        client_nonce = None

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
            # R4: one-time photo fields. NULL for plain text DMs.
            photo_url=photo_url if view_once else None,
            view_once_status="sent" if view_once else None,
            # Audit fix #1 — persist client_nonce so the
            # dm_view_once_opened broadcast can carry it back to
            # the sender's optimistic message.
            client_nonce=client_nonce if view_once else None,
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
    if client_nonce is not None:
        sender_payload["client_nonce"] = client_nonce
    await _send(websocket, sender_payload)

    # Deliver to recipient if not soft-blocked. If their WS isn't
    # connected, fall back to an FCM data push so their device wakes
    # up and shows a notification.
    if delivered_to_recipient:
        recipient_payload = dict(base)
        recipient_payload["type"] = "dm_message"
        delivered_over_ws = await _send_to_uid(recipient_uid, recipient_payload)
        if not delivered_over_ws:
            # R4 view-once: lock-screen preview is plain "Photo"
            # (no emoji round-trip risk per audit fix #11, no
            # photo_url leak in FCM payload).
            fcm_preview = "Photo" if view_once else (text or "Shared a song")
            await _push_dm_fcm(
                recipient_uid=recipient_uid,
                sender_uid=sender_uid,
                sender_name=sender.name,
                sender_avatar_url=sender.avatar_url,
                preview=fcm_preview,
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

    read_at = _now()
    async with _session_scope() as session:
        q = update(models.DmMessage).where(
            models.DmMessage.to_uid == me,
            models.DmMessage.from_uid == peer,
            models.DmMessage.read_at.is_(None),
        )
        if isinstance(up_to, int):
            q = q.where(models.DmMessage.id <= up_to)
        q = q.values(read_at=read_at)
        result = await session.execute(q)
        await session.commit()
        rows_marked = result.rowcount or 0

    # Tell the sender (peer) which of their messages just got read so
    # their UI can flip "Sent" → "Read". Without this push the sender
    # has no way to know — they'd be stuck on "Sent" forever even
    # though the DB row has read_at set.
    if rows_marked > 0:
        await _send_to_uid(
            peer,
            {
                "type": "dm_messages_read",
                "reader_uid": me,
                "up_to_message_id": up_to if isinstance(up_to, int) else None,
                "read_at": _to_ms(read_at),
            },
        )


async def handle_dm_set_retention(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    """`dm_set_retention {peer_uid, mode}` — switch the conversation
    between 'keep' (persist history) and 'disappear' (delete on
    view+leave). Either user may switch it; last-write-wins. Both sides
    get a `dm_retention_changed` carrying an inline system notice."""
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    peer = (msg.get("peer_uid") or "").strip()
    mode = (msg.get("mode") or "").strip()
    if not peer or mode not in ("keep", "disappear"):
        await _send(websocket, _err("bad_request", "peer_uid + mode(keep|disappear)"))
        return

    sender = presence.get(me)
    by_name = sender.name if sender is not None else ""

    async with _session_scope() as session:
        state_row, _ = await _ensure_thread_state(session, me, peer)
        state_row.retention_mode = mode
        state_row.updated_at = _now()

        # Inline notice so both clients can render "<name> turned on …".
        # text carries the mode (satisfies the content CHECK); the client
        # renders from event_type. read_at is stamped so it never counts
        # as unread.
        event = models.DmMessage(
            from_uid=me,
            to_uid=peer,
            text=mode,
            event_type=f"retention_{mode}",
            reactions={},
            deleted=False,
            sent_at=_now(),
            read_at=_now(),
        )
        session.add(event)
        await session.commit()
        await session.refresh(event)

    event_dict = _dm_to_dict(event, with_gate_state="accepted")
    event_dict["from_name"] = by_name

    # Tailor peer_uid per recipient (it's always "the other side").
    await _send_to_uid(
        me,
        {
            "type": "dm_retention_changed",
            "peer_uid": peer,
            "mode": mode,
            "by_uid": me,
            "by_name": by_name,
            "message": event_dict,
        },
    )
    await _send_to_uid(
        peer,
        {
            "type": "dm_retention_changed",
            "peer_uid": me,
            "mode": mode,
            "by_uid": me,
            "by_name": by_name,
            "message": event_dict,
        },
    )


async def handle_dm_clear_on_leave(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    """`dm_clear_on_leave {peer_uid}` — the recipient just left a
    disappear-mode chat after viewing it. Delete the content messages
    they read (from this peer) and tell both ends to drop them."""
    me = presence.uid_for_client(client_id)
    if me is None:
        return
    peer = (msg.get("peer_uid") or "").strip()
    if not peer:
        return

    async with _session_scope() as session:
        state = await _get_thread_state(session, me, peer)
        if state is None or (state.retention_mode or "keep") != "disappear":
            return  # only disappear threads vanish on leave

        # All already-VIEWED content messages between me and this peer,
        # both directions: the peer's messages I read, AND my messages the
        # peer has read (read_at stamped). Unviewed messages (read_at NULL)
        # stay — they haven't been seen yet. System notices (event_type
        # set) are always kept.
        rows = (
            await session.execute(
                select(models.DmMessage.id).where(
                    models.DmMessage.read_at.is_not(None),
                    models.DmMessage.event_type.is_(None),
                    or_(
                        and_(
                            models.DmMessage.to_uid == me,
                            models.DmMessage.from_uid == peer,
                        ),
                        and_(
                            models.DmMessage.from_uid == me,
                            models.DmMessage.to_uid == peer,
                        ),
                    ),
                )
            )
        ).scalars().all()
        if not rows:
            return
        ids = [int(r) for r in rows]
        await session.execute(
            delete(models.DmMessage).where(models.DmMessage.id.in_(ids))
        )
        await session.commit()

    # Both ends drop the same ids (sender sees their sent bubbles vanish).
    await _send_to_uid(me, {"type": "dm_messages_cleared", "peer_uid": peer, "ids": ids})
    await _send_to_uid(peer, {"type": "dm_messages_cleared", "peer_uid": me, "ids": ids})


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
# R4 — one-time photo helpers + handlers
# ─────────────────────────────────────────────────────────────────────


def _public_id_from_dm_photo_url(url: str | None) -> str | None:
    """Parse `dm_photos/{uuid}` out of a Cloudinary delivery URL.

    Cloudinary delivery URLs look like
    `https://res.cloudinary.com/{cloud}/image/upload/v{N}/dm_photos/{uuid}.{ext}`.
    The destroy API needs the public_id WITHOUT extension —
    `dm_photos/{uuid}`. Returns None on any parse failure (caller
    decides whether to fail loudly).
    """
    if not url:
        return None
    idx = url.find("/dm_photos/")
    if idx < 0:
        return None
    tail = url[idx + 1:]  # "dm_photos/{uuid}.{ext}"
    # Strip extension (last segment after the last dot, only if
    # that segment doesn't contain a slash).
    dot = tail.rfind(".")
    if dot > 0 and "/" not in tail[dot:]:
        return tail[:dot]
    return tail


async def handle_dm_chat_view_once_open(
    client_id: str, websocket: WebSocket, msg: dict
) -> None:
    """R4: recipient marks a view-once photo as opened.

    Atomic state flip + Cloudinary blob destruction + broadcast to
    BOTH peers. The Cloudinary destroy is fire-and-forget on a
    background thread (audit fix #2) so the WS event loop isn't
    blocked on the ~800ms CDN purge.

    Server-side guards:
      * caller must be the recipient (audit-defense — client
        already skips firing on the sender side, but we don't
        trust that)
      * status must be 'sent' (already opened → no-op + error)
    """
    me = presence.uid_for_client(client_id)
    if me is None:
        await _send(websocket, _err("not_subscribed", "Call social_subscribe first"))
        return
    raw_id = msg.get("message_id")
    if not isinstance(raw_id, int) or raw_id <= 0:
        await _send(websocket, _err("bad_request", "message_id required"))
        return
    message_id = raw_id

    async with _session_scope() as session:
        row = (
            await session.execute(
                select(models.DmMessage).where(models.DmMessage.id == message_id)
            )
        ).scalar_one_or_none()
        if row is None:
            await _send(websocket, {
                "type": "social_error",
                "code": "not_found",
                "message_id": message_id,
            })
            return
        if row.to_uid != me:
            await _send(websocket, {
                "type": "social_error",
                "code": "not_recipient",
                "message_id": message_id,
            })
            return
        if row.view_once_status != "sent":
            await _send(websocket, {
                "type": "social_error",
                "code": "already_opened",
                "message_id": message_id,
            })
            return
        public_id = _public_id_from_dm_photo_url(row.photo_url)
        client_nonce = row.client_nonce or ""
        from_uid_for_broadcast = row.from_uid
        to_uid_for_broadcast = row.to_uid
        # Atomic state flip — the cross-column CHECK constraint
        # would block any partial state.
        row.view_once_status = "opened"
        row.photo_url = None
        await session.commit()

    # Fire-and-forget Cloudinary destroy on a worker thread so a
    # slow CDN purge doesn't stall this WS worker (audit fix #2).
    if public_id is not None:
        import cloudinary.uploader
        async def _destroy_async():
            try:
                await asyncio.to_thread(
                    cloudinary.uploader.destroy, public_id, invalidate=True,
                )
                print(f"[view_once_open] cloudinary destroy ok public_id={public_id}")
            except Exception as e:
                print(f"[view_once_open] cloudinary destroy FAILED public_id={public_id}: {e}")
        asyncio.create_task(_destroy_async())

    broadcast = {
        "type": "dm_view_once_opened",
        "message_id": message_id,
        "client_nonce": client_nonce,
    }
    await _send_to_uid(to_uid_for_broadcast, broadcast)
    await _send_to_uid(from_uid_for_broadcast, broadcast)


async def rest_dm_view_once_open(message_id: int, caller_uid: str) -> dict:
    """REST fallback for `handle_dm_chat_view_once_open` — used when
    the recipient's WS is briefly disconnected mid-view. Same
    semantics; returns a dict the REST endpoint serialises.
    Raises HTTPException-equivalent dicts the caller can adapt."""
    async with _session_scope() as session:
        row = (
            await session.execute(
                select(models.DmMessage).where(models.DmMessage.id == message_id)
            )
        ).scalar_one_or_none()
        if row is None:
            return {"status": "not_found"}
        if row.to_uid != caller_uid:
            return {"status": "not_recipient"}
        if row.view_once_status != "sent":
            return {"status": "already_opened"}
        public_id = _public_id_from_dm_photo_url(row.photo_url)
        client_nonce = row.client_nonce or ""
        from_uid_for_broadcast = row.from_uid
        to_uid_for_broadcast = row.to_uid
        row.view_once_status = "opened"
        row.photo_url = None
        await session.commit()

    if public_id is not None:
        import cloudinary.uploader
        async def _destroy_async():
            try:
                await asyncio.to_thread(
                    cloudinary.uploader.destroy, public_id, invalidate=True,
                )
                print(f"[view_once_open REST] cloudinary destroy ok public_id={public_id}")
            except Exception as e:
                print(f"[view_once_open REST] cloudinary destroy FAILED public_id={public_id}: {e}")
        asyncio.create_task(_destroy_async())

    broadcast = {
        "type": "dm_view_once_opened",
        "message_id": message_id,
        "client_nonce": client_nonce,
    }
    await _send_to_uid(to_uid_for_broadcast, broadcast)
    await _send_to_uid(from_uid_for_broadcast, broadcast)
    return {"status": "ok", "client_nonce": client_nonce}


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

                # DMs — disappear-mode safety net: delete read content
                # messages older than 1h ONLY in threads currently set to
                # 'disappear'. The primary delete is on-leave (dm_clear_on_leave);
                # this catches the case where the app was killed before the
                # leave signal fired. Keep-mode threads are never swept here.
                # System notices (event_type set) are always preserved.
                cutoff = _now() - timedelta(hours=1)
                dm = models.DmMessage
                ts = models.DmThreadState
                disappear_exists = (
                    select(ts.uid_a)
                    .where(
                        ts.uid_a == func.least(dm.from_uid, dm.to_uid),
                        ts.uid_b == func.greatest(dm.from_uid, dm.to_uid),
                        ts.retention_mode == "disappear",
                    )
                    .exists()
                )
                await session.execute(
                    delete(dm).where(
                        dm.read_at.is_not(None),
                        dm.read_at < cutoff,
                        dm.event_type.is_(None),
                        disappear_exists,
                    )
                )

                # Bound every thread to its most recent 500 messages so
                # keep-mode history can't grow without limit.
                rn = func.row_number().over(
                    partition_by=[
                        func.least(dm.from_uid, dm.to_uid),
                        func.greatest(dm.from_uid, dm.to_uid),
                    ],
                    order_by=dm.sent_at.desc(),
                ).label("rn")
                ranked = select(dm.id, rn).subquery()
                overflow = select(ranked.c.id).where(ranked.c.rn > 500)
                await session.execute(delete(dm).where(dm.id.in_(overflow)))

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
