"""Remote-control sessions: a browser driving a phone.

Deliberately SEPARATE from RoomManager rather than modelled as a room.
`RoomManager._client_to_room` maps one client_id to exactly ONE room code, so
a control-session-as-room would make it impossible for a phone to be remotely
controlled *while in a Listen Together room* — which is the headline use case,
not an edge case.

This module owns no I/O. It stores websocket objects but never sends on them;
callers do that and decide what to do when a send fails. `main.py`'s `ws_send`
swallows every exception, so a module that sent internally could not tell a
live phone from a dead one.
"""

import json
import time
import secrets
import string
from dataclasses import dataclass, field
from typing import Optional


# Pairing code alphabet: no O/0 or I/1, which are the pairs people misread
# when copying a code off a screen.
_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

TICKET_TTL = 90.0          # seconds a pairing code / ticket stays valid
SESSION_IDLE_TTL = 6 * 3600.0
CMD_WINDOW = 10.0          # rate-limit window for control_cmd
CMD_MAX_IN_WINDOW = 40     # generous for a human, closes the firehose
# /ws is unauthenticated, so anyone can open sockets and create sessions.
# Without these two, N sockets x a 16MB state blob (uvicorn's default
# ws_max_size) OOMs the whole API - extraction, updates, social included.
MAX_SESSIONS = 500
MAX_STATE_BYTES = 8 * 1024


@dataclass
class ControlSession:
    id: str
    phone_client_id: str
    phone_ws: object
    phone_secret: str
    owner_uid: Optional[str] = None
    controllers: dict = field(default_factory=dict)   # client_id -> websocket
    last_state: Optional[dict] = None
    created_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)


@dataclass
class _Ticket:
    code: str
    session_id: str
    expires_at: float


class ControlSessionManager:
    def __init__(self):
        self._sessions: dict[str, ControlSession] = {}
        # Reverse index covering BOTH the phone and every controller, so
        # disconnect cleanup is one lookup regardless of which side dropped.
        self._by_client: dict[str, str] = {}
        self._tickets: dict[str, _Ticket] = {}
        self._cmd_hits: dict[str, list] = {}

    # ── sessions ──────────────────────────────────────────────────────

    def create(self, phone_client_id: str, phone_ws, owner_uid: str = None):
        """Start a session for a phone.

        Returns (session, phone_secret, orphaned_ws) - the orphans belong to a
        previous session this client was running and MUST be told, or their
        browser sits on stale state forever believing it is still paired.
        """
        if len(self._sessions) >= MAX_SESSIONS:
            return None, None, []
        _, orphans = self.end_for_client(phone_client_id)
        sid = secrets.token_urlsafe(12)
        sess = ControlSession(
            id=sid,
            phone_client_id=phone_client_id,
            phone_ws=phone_ws,
            phone_secret=secrets.token_urlsafe(24),
            owner_uid=owner_uid,
        )
        self._sessions[sid] = sess
        self._by_client[phone_client_id] = sid
        return sess, sess.phone_secret, orphans

    def rejoin(self, phone_client_id: str, phone_ws, secret: str) -> Optional[ControlSession]:
        """Re-attach a phone after a reconnect.

        client_id is per-connection, and reconnects are routine (backoff, FCM
        wake, network changes), so without this every Wi-Fi switch would drop
        the pairing and force the user to re-scan.
        """
        # Raw client input: compare_digest raises TypeError on non-str and
        # on non-ASCII str, and an unhandled raise tears down the socket.
        if not secret or not isinstance(secret, str):
            return None
        # This id may already be mapped (e.g. it was a controller). Clear it
        # first so we never orphan another session by overwriting.
        self.end_for_client(phone_client_id)
        for sess in self._sessions.values():
            if secrets.compare_digest(sess.phone_secret.encode("utf-8"),
                                      secret.encode("utf-8")):
                self._by_client.pop(sess.phone_client_id, None)
                sess.phone_client_id = phone_client_id
                sess.phone_ws = phone_ws
                sess.last_seen = time.time()
                self._by_client[phone_client_id] = sess.id
                return sess
        return None

    def get(self, session_id: str) -> Optional[ControlSession]:
        return self._sessions.get(session_id)

    def for_client(self, client_id: str) -> Optional[ControlSession]:
        sid = self._by_client.get(client_id)
        return self._sessions.get(sid) if sid else None

    def is_phone(self, client_id: str) -> bool:
        sess = self.for_client(client_id)
        return bool(sess and sess.phone_client_id == client_id)

    # ── pairing ───────────────────────────────────────────────────────

    def mint_code(self, session_id: str) -> Optional[str]:
        """Phone-initiated pairing code. Single-use, short-lived."""
        if session_id not in self._sessions:
            return None
        self._sweep_tickets()
        for _ in range(12):
            code = "".join(secrets.choice(_CODE_ALPHABET) for _ in range(6))
            if code not in self._tickets:
                self._tickets[code] = _Ticket(code, session_id, time.time() + TICKET_TTL)
                return code
        return None

    def redeem(self, code: str) -> Optional[ControlSession]:
        """Burn a code and return its session. Single-use, right or wrong."""
        self._sweep_tickets()
        if not isinstance(code, str):
            return None
        t = self._tickets.pop(code.strip().upper(), None)
        if t is None or t.expires_at < time.time():
            return None
        return self._sessions.get(t.session_id)

    def attach_controller(self, session_id: str, client_id: str, ws) -> bool:
        sess = self._sessions.get(session_id)
        if sess is None:
            return False
        # Same reason as rejoin(): overwriting a live mapping would strand
        # whatever session it pointed at, with no way to ever reach it again.
        if self._by_client.get(client_id) not in (None, sess.id):
            self.end_for_client(client_id)
        sess.controllers[client_id] = ws
        sess.last_seen = time.time()
        self._by_client[client_id] = sess.id
        return True

    # ── teardown ──────────────────────────────────────────────────────

    def end_for_client(self, client_id: str):
        """Handle either side dropping. Returns (ended_session, orphaned_ws).

        Phone drops  -> the whole session ends; its controllers are orphaned.
        Controller drops -> only that controller is removed.
        """
        sid = self._by_client.pop(client_id, None)
        if not sid:
            return None, []
        sess = self._sessions.get(sid)
        if sess is None:
            return None, []

        if sess.phone_client_id == client_id:
            orphans = list(sess.controllers.values())
            for cid in list(sess.controllers):
                self._by_client.pop(cid, None)
            self._sessions.pop(sid, None)
            for code, t in list(self._tickets.items()):
                if t.session_id == sid:
                    self._tickets.pop(code, None)
            self._cmd_hits.pop(client_id, None)
            return sess, orphans

        sess.controllers.pop(client_id, None)
        self._cmd_hits.pop(client_id, None)
        return None, []

    # ── misc ──────────────────────────────────────────────────────────

    def allow_cmd(self, client_id: str) -> bool:
        now = time.time()
        hits = [t for t in self._cmd_hits.get(client_id, []) if now - t < CMD_WINDOW]
        if len(hits) >= CMD_MAX_IN_WINDOW:
            self._cmd_hits[client_id] = hits
            return False
        hits.append(now)
        self._cmd_hits[client_id] = hits
        return True

    def set_state(self, session: ControlSession, state) -> bool:
        """Store the phone's now-playing state. Rejects anything oversized."""
        session.last_seen = time.time()
        if not isinstance(state, dict):
            return False
        try:
            if len(json.dumps(state)) > MAX_STATE_BYTES:
                return False
        except (TypeError, ValueError):
            return False
        session.last_state = state
        return True

    def sweep(self) -> list:
        """Drop sessions idle past the TTL. Returns the sessions removed."""
        self._sweep_tickets()
        now = time.time()
        dead = [s for s in self._sessions.values()
                if now - s.last_seen > SESSION_IDLE_TTL]
        for s in dead:
            self._sessions.pop(s.id, None)
            self._by_client.pop(s.phone_client_id, None)
            self._cmd_hits.pop(s.phone_client_id, None)
            for cid in list(s.controllers):
                self._by_client.pop(cid, None)
                self._cmd_hits.pop(cid, None)
        # Rate-limit buckets outlive their session when the phone drops
        # first, since the controllers are unmapped before they disconnect.
        now2 = time.time()
        for cid, hits in list(self._cmd_hits.items()):
            if not hits or now2 - hits[-1] > CMD_WINDOW * 10:
                self._cmd_hits.pop(cid, None)
        return dead

    def _sweep_tickets(self):
        now = time.time()
        for code, t in list(self._tickets.items()):
            if t.expires_at < now:
                self._tickets.pop(code, None)

    @property
    def session_count(self) -> int:
        return len(self._sessions)


control_manager = ControlSessionManager()
