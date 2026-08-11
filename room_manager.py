import time
import random
import string
import json
import uuid
import secrets
from dataclasses import dataclass, field
from typing import Optional


# Per-room chat caps. Keep memory bounded under heavy chatters.
MAX_CHAT_HISTORY = 200
TYPING_TTL_MS = 5_000


@dataclass
class RoomMember:
    client_id: str
    websocket: object
    name: str = "Unknown"
    time_offset: float = 0.0
    # Reconnect grace (Phase 1): on an unexpected WS drop we KEEP the member in
    # the roster with reconnecting=True (so a brief blip doesn't change the
    # roster for anyone) and skip their dead socket in broadcasts. Only if they
    # don't rejoin within the grace window do we actually remove + announce.
    reconnecting: bool = False
    disconnect_time: float = 0.0
    # Rejoin secret. Minted server-side at create/join and handed only to the
    # member it belongs to. Rejoin currently trusts a client-supplied
    # `previous_client_id` to restore host — but every member's client_id is
    # broadcast in get_member_list(), so anyone in the room can claim the
    # host's. This secret replaces that proof. Additive for now: rejoin still
    # accepts the legacy path until enough installs send a secret.
    secret: str = ""
    # Sticky record of "this member is/was the room's host". host_id is the
    # only host record today, and cleanup_stale_disconnects sets it to None
    # after 60s — without this a host reconnecting past that window comes
    # back as a guest, because the secret path deliberately skips the
    # legacy fallback that used to restore them.
    was_host: bool = False


@dataclass
class QueueItem:
    video_id: str
    title: str
    duration: str  # kept as original type from client (could be int or str)
    thumbnail: str
    uploader: str
    requested_by: str  # client_id
    requested_by_name: str  # display name
    votes: set = field(default_factory=set)  # set of client_ids who voted
    timestamp: float = field(default_factory=time.time)  # for tie-breaking
    is_suggestion: bool = False


@dataclass
class ChatMessageRecord:
    """In-memory chat message record. Lives only as long as the room exists.

    Required by the chat-redesign features: reactions and replies need a
    stable identifier per message so clients can address the right one
    when broadcasting `add_reaction` / `delete_message` / etc. We bound
    the per-room history list to the most recent N messages so a noisy
    room doesn't grow the server process unbounded.
    """
    id: str  # uuid hex (no dashes)
    sender_id: str
    sender_name: str
    text: str
    timestamp: int  # ms since epoch
    is_suggestion: bool = False
    is_share_moment: bool = False  # special tap-to-jump card; non-editable
    suggestion_video_id: Optional[str] = None
    suggestion_title: Optional[str] = None
    suggestion_thumbnail: Optional[str] = None
    suggestion_uploader: Optional[str] = None
    reply_to_id: Optional[str] = None  # message id this is a reply to
    reply_to_sender_name: Optional[str] = None
    reply_to_text: Optional[str] = None  # snapshot of original text for display
    edited_at: Optional[int] = None  # ms since epoch when last edited (None = never)
    deleted: bool = False  # tombstoned messages stay so clients can grey them out
    reactions: dict = field(default_factory=dict)  # emoji -> set[client_id]


@dataclass
class RoomState:
    code: str
    host_id: str
    host_name: str = "Unknown"
    password: Optional[str] = None  # None = open room, string = locked
    members: dict = field(default_factory=dict)  # client_id -> RoomMember
    current_song: Optional[dict] = None  # {videoId, title, duration, thumbnail, uploader, audioUrl}
    position: float = 0.0  # seconds
    is_playing: bool = False
    play_start_time: float = 0.0  # server timestamp when play started (for position calc)
    queue: list = field(default_factory=list)  # list of QueueItem
    created_at: float = field(default_factory=time.time)
    invite_tokens: dict = field(default_factory=dict)  # token -> expiry timestamp
    peak_members: int = 1
    songs_played: int = 0
    # Chat: bounded in-memory history (last MAX_CHAT_HISTORY messages).
    # Cleared when the room is destroyed; we deliberately do NOT persist
    # chat across room sessions.
    messages: list = field(default_factory=list)  # list of ChatMessageRecord
    # Tracks "X is typing" state. client_id -> last keystroke ts (ms).
    # Entries auto-expire after TYPING_TTL_MS in handle_typing.
    typing: dict = field(default_factory=dict)

    def get_sorted_queue(self) -> list:
        """Requests sorted by vote count desc, then timestamp asc, followed by suggestions."""
        requests = [q for q in self.queue if not q.is_suggestion]
        suggestions = [q for q in self.queue if q.is_suggestion]
        requests.sort(key=lambda q: (-len(q.votes), q.timestamp))
        return requests + suggestions

    def serialize_queue_for_client(self, client_id: str) -> list:
        """Convert queue to dicts with personalized votedByMe flag."""
        sorted_q = self.get_sorted_queue()
        result = []
        for q in sorted_q:
            result.append({
                "videoId": q.video_id,
                "title": q.title,
                "duration": q.duration,
                "thumbnail": q.thumbnail,
                "uploader": q.uploader,
                "requestedBy": q.requested_by if not q.is_suggestion else None,
                "requestedByName": q.requested_by_name if not q.is_suggestion else None,
                "voteCount": len(q.votes),
                "votedByMe": client_id in q.votes,
                "isSuggestion": q.is_suggestion,
            })
        return result

    def create_invite_token(self) -> str:
        """Generate a single-use invite token with 1-hour expiry."""
        self._clean_expired_tokens()
        token = str(uuid.uuid4())
        self.invite_tokens[token] = time.time() + 3600  # 1 hour
        return token

    def validate_invite_token(self, token: str) -> bool:
        """Check if invite token is valid. Consumes the token if valid (single-use)."""
        self._clean_expired_tokens()
        expiry = self.invite_tokens.get(token)
        if expiry is None:
            return False
        if time.time() > expiry:
            del self.invite_tokens[token]
            return False
        del self.invite_tokens[token]  # single-use
        return True

    def _clean_expired_tokens(self):
        """Remove expired invite tokens."""
        now = time.time()
        self.invite_tokens = {t: exp for t, exp in self.invite_tokens.items() if exp > now}

    def has_voted_request(self) -> bool:
        """Check if any request has more than 1 vote (beyond requester's auto-vote)."""
        return any(len(q.votes) > 1 for q in self.queue if not q.is_suggestion)

    def remove_member_from_queue(self, client_id: str):
        """Remove member's requests and their votes from other items."""
        # Remove their requests
        self.queue = [q for q in self.queue if q.is_suggestion or q.requested_by != client_id]
        # Remove their votes from remaining items
        for q in self.queue:
            q.votes.discard(client_id)


class RoomManager:
    def __init__(self):
        self.rooms: dict[str, RoomState] = {}  # code -> RoomState
        self._client_to_room: dict[str, str] = {}  # client_id -> room_code
        self._pending_disconnects: dict[str, dict] = {}  # client_id -> {code, was_host, name, time}

    def generate_code(self) -> str:
        while True:
            code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
            if code not in self.rooms:
                return code

    def create_room(self, host_id: str, websocket, host_name: str = "Unknown", password: str = None) -> RoomState:
        code = self.generate_code()
        member = RoomMember(client_id=host_id, websocket=websocket, name=host_name,
                            secret=secrets.token_urlsafe(24), was_host=True)
        room = RoomState(code=code, host_id=host_id, host_name=host_name, password=password, members={host_id: member})
        self.rooms[code] = room
        self._client_to_room[host_id] = code
        return room

    def join_room(self, code: str, client_id: str, websocket, name: str = "Unknown") -> tuple[Optional[RoomState], bool]:
        """Returns (room, promoted_to_host)"""
        room = self.rooms.get(code.upper())
        if room is None:
            return None, False
        member = RoomMember(client_id=client_id, websocket=websocket, name=name,
                            secret=secrets.token_urlsafe(24))
        room.members[client_id] = member
        self._client_to_room[client_id] = code.upper()
        room.peak_members = max(room.peak_members, len(room.members))
        # If room has no active host, promote this joiner to host
        promoted = False
        if room.host_id not in room.members or room.host_id == client_id:
            if room.host_id != client_id:  # only if actually promoting
                room.host_id = client_id
                room.host_name = name
                promoted = True
        return room, promoted

    def leave_room(self, client_id: str) -> tuple[Optional[str], bool, list]:
        """Returns (room_code, was_host, remaining_member_websockets)"""
        code = self._client_to_room.pop(client_id, None)
        if code is None:
            return None, False, []
        room = self.rooms.get(code)
        if room is None:
            return code, False, []
        was_host = room.host_id == client_id
        room.members.pop(client_id, None)
        remaining_ws = [m.websocket for m in room.members.values()]
        if was_host or len(room.members) == 0:
            for mid in list(room.members.keys()):
                self._client_to_room.pop(mid, None)
            del self.rooms[code]
        return code, was_host, remaining_ws

    def disconnect_member(self, client_id: str) -> tuple[Optional[str], bool, bool]:
        """Handle an UNEXPECTED disconnect (not an explicit leave). Marks the
        member `reconnecting` and KEEPS them in the roster for a grace window —
        the roster count stays unchanged for everyone. The caller schedules the
        actual removal (finalize_disconnect) if they never come back.
        Returns (code, was_host, member_kept)."""
        code = self._client_to_room.get(client_id)
        if code is None:
            return None, False, False
        room = self.rooms.get(code)
        if room is None:
            self._client_to_room.pop(client_id, None)
            return code, False, False

        was_host = room.host_id == client_id
        member = room.members.get(client_id)
        name = member.name if member else "Unknown"

        # Store for potential rejoin (host-restore + FCM targeting).
        self._pending_disconnects[client_id] = {
            "code": code, "was_host": was_host,
            "name": name, "time": time.time()
        }

        if member is not None:
            member.reconnecting = True
            member.disconnect_time = time.time()
            return code, was_host, True

        # No member object (shouldn't happen) — nothing to keep alive.
        self._client_to_room.pop(client_id, None)
        return code, was_host, False

    def finalize_disconnect(self, client_id: str) -> tuple[Optional[str], list]:
        """Grace expired. If the member is STILL reconnecting (never rejoined),
        remove them for real and return (code, remaining_ws) so the caller can
        broadcast member_left. If they already rejoined (or the room is gone),
        returns (None, [])."""
        self._pending_disconnects.pop(client_id, None)
        code = self._client_to_room.get(client_id)
        if code is None:
            return None, []
        room = self.rooms.get(code)
        if room is None:
            self._client_to_room.pop(client_id, None)
            return None, []
        member = room.members.get(client_id)
        if member is None or not member.reconnecting:
            return None, []   # already rejoined / replaced — nothing to do
        room.members.pop(client_id, None)
        self._client_to_room.pop(client_id, None)
        if len(room.members) == 0:
            del self.rooms[code]
            return None, []
        remaining_ws = [m.websocket for m in room.members.values()]
        return code, remaining_ws

    def rejoin_room(self, client_id: str, websocket, code: str, name: str = "Unknown",
                    previous_client_id: str = None,
                    member_secret: str = None) -> tuple[Optional[RoomState], bool]:
        """Attempt to rejoin a room after disconnect. Returns (room, was_host) or (None, False).
        previous_client_id: the client's old ID from before reconnection, used to clean up stale entries."""
        room = self.rooms.get(code.upper())
        if room is None:
            return None, False

        was_host = False
        carried_secret = ""

        # Method 0 (preferred): a secret we minted and handed only to that
        # member. Unlike previous_client_id this cannot be lifted from a
        # member-list broadcast, so it is real proof of who is rejoining.
        # Compared constant-time; the first match wins and short-circuits the
        # legacy fallbacks entirely.
        if member_secret:
            for mid, m in list(room.members.items()):
                # Compare BYTES: compare_digest raises TypeError on a
                # non-ASCII str, and member_secret is raw client input — an
                # unhandled raise here tears down the whole socket.
                if (m.secret and isinstance(member_secret, str)
                        and secrets.compare_digest(m.secret.encode("utf-8"),
                                                   member_secret.encode("utf-8"))):
                    was_host = (mid == room.host_id) or (room.host_id is None and m.was_host)
                    carried_secret = m.secret
                    if mid != client_id:
                        room.members.pop(mid, None)
                        self._client_to_room.pop(mid, None)
                    self._pending_disconnects.pop(mid, None)
                    break
            else:
                # A secret that matches nobody is a stale session (room was
                # recreated, or the member was already reaped). Fall through
                # to the legacy paths rather than hard-failing a reconnect.
                pass

        # Methods 1-3 are the LEGACY paths, kept only for installs that have
        # no secret yet. A verified secret is authoritative: if it said this
        # member is not the host, these must not override it, or the takeover
        # they exist to enable survives the fix.
        if not carried_secret:
            # Method 1: Check pending disconnects using the PREVIOUS client_id
            lookup_id = previous_client_id or client_id
            pending = self._pending_disconnects.pop(lookup_id, None)
            if pending and pending.get("was_host") and pending["code"] == code.upper():
                was_host = True

            # Method 2: Fallback — check if previous_client_id IS the room's host_id directly
            # (handles race condition where disconnect hasn't fired yet, or pending was already popped)
            if not was_host and previous_client_id and room.host_id == previous_client_id:
                was_host = True

            # Method 3: Fallback — if room has no active host (host_id not in members), restore host
            if not was_host and room.host_id not in room.members:
                was_host = True

        # Remove stale member entry with old client_id if it still lingers.
        # Gated on NOT having verified a secret: previous_client_id is raw
        # client input and every member's client_id is broadcast in
        # get_member_list, so an ungated eviction let any member throw the
        # host out of the roster — which then gets the whole room destroyed
        # by startup_room_cleanup's "host not in members" sweep. Method 0
        # already removed the caller's own stale entry.
        if not carried_secret and previous_client_id and previous_client_id in room.members:
            room.members.pop(previous_client_id)
            self._client_to_room.pop(previous_client_id, None)

        # If they were host, restore host status
        if was_host:
            room.host_id = client_id

        # Carry the existing secret forward so it stays stable across
        # reconnects; mint one for legacy clients so they get the safe path
        # from their next rejoin onward.
        member = RoomMember(client_id=client_id, websocket=websocket, name=name,
                            secret=carried_secret or secrets.token_urlsafe(24),
                            was_host=was_host)
        room.members[client_id] = member
        self._client_to_room[client_id] = code.upper()
        room.peak_members = max(room.peak_members, len(room.members))
        return room, was_host

    def cleanup_stale_disconnects(self, max_age: float = 60.0) -> list:
        """Remove pending disconnects older than max_age seconds. Returns stale client_ids."""
        now = time.time()
        stale = [cid for cid, info in self._pending_disconnects.items()
                 if now - info["time"] > max_age]
        for cid in stale:
            info = self._pending_disconnects.pop(cid)
            if info["was_host"]:
                room = self.rooms.get(info["code"])
                if room and room.host_id == cid:
                    room.host_id = None  # Signal host is gone
        return stale

    def get_room_for_client(self, client_id: str) -> Optional[RoomState]:
        code = self._client_to_room.get(client_id)
        if code:
            return self.rooms.get(code)
        return None

    def is_host(self, client_id: str) -> bool:
        room = self.get_room_for_client(client_id)
        return room is not None and room.host_id == client_id

    def get_estimated_position(self, room: RoomState) -> float:
        if not room.is_playing:
            return room.position
        elapsed = time.time() - room.play_start_time
        return room.position + elapsed

    def get_member_list(self, room: RoomState) -> list:
        """Return list of member info dicts for broadcasting"""
        return [
            {"clientId": mid, "name": member.name, "isHost": mid == room.host_id,
             "reconnecting": member.reconnecting}
            for mid, member in room.members.items()
        ]

    def kick_member(self, room_code: str, host_id: str, target_id: str):
        """Host kicks a member. Returns (target_websocket, success)"""
        room = self.rooms.get(room_code)
        if not room or room.host_id != host_id or target_id == host_id:
            return None, False
        member = room.members.pop(target_id, None)
        if member is None:
            return None, False
        self._client_to_room.pop(target_id, None)
        return member.websocket, True

    def list_rooms(self) -> list:
        """Return public info about all active rooms for discovery"""
        result = []
        for code, room in self.rooms.items():
            result.append({
                "code": code,
                "hostName": room.host_name,
                "memberCount": len(room.members),
                "hasPassword": room.password is not None,
                "currentSong": room.current_song.get("title") if room.current_song else None,
            })
        return result

    async def broadcast(self, room: RoomState, message: dict, exclude_id: str = None):
        data = json.dumps(message)
        dead_members = []
        for mid, member in list(room.members.items()):
            if mid == exclude_id:
                continue
            if member.reconnecting:
                continue  # in grace: dead socket, but keep them in the roster
            try:
                await member.websocket.send_text(data)
            except Exception:
                dead_members.append(mid)
        # Clean up members with dead WebSocket connections
        for mid in dead_members:
            room.members.pop(mid, None)
            self._client_to_room.pop(mid, None)

    async def broadcast_queue(self, room: RoomState):
        """Send personalized sync_queue to each member (votedByMe differs per client)."""
        dead_members = []
        for mid, member in list(room.members.items()):
            if member.reconnecting:
                continue  # in grace: dead socket, but keep them in the roster
            msg = {"type": "sync_queue", "queue": room.serialize_queue_for_client(mid)}
            try:
                await member.websocket.send_text(json.dumps(msg))
            except Exception:
                dead_members.append(mid)
        for mid in dead_members:
            room.members.pop(mid, None)
            self._client_to_room.pop(mid, None)
