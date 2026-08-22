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
# A phone dropping its socket is routine - backoff, FCM wake, Wi-Fi to
# cellular. Ending the session immediately made control_rejoin unreachable:
# by the time the phone came back there was nothing to rejoin, so the remote
# bricked on the first blip and never re-paired.
PHONE_GRACE = 90.0


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
    # Set when the phone's socket drops; cleared on rejoin. While set, the
    # session is held open for PHONE_GRACE seconds.
    phone_gone_at: float = 0.0
    # What this phone's BUILD can do, as asserted by the phone itself on
    # control_create and re-asserted on every control_rejoin.
    #
    # Monotonic. rejoin() unions rather than replaces, because dropping a
    # capability on a frame that merely forgot to mention it would fail OPEN --
    # the approval gate would vanish from a live session with no symptom.
    # A genuine downgrade is handled by handle_control_rejoin instead: it drops
    # any outstanding pending and answers phone_needs_update.
    #
    # A phone can only ever weaken itself this way — the value is asserted over
    # the phone's own authenticated socket and no browser can influence it.
    caps: set = field(default_factory=set)


@dataclass
class _Ticket:
    code: str
    session_id: str
    expires_at: float
    # Spent, but deliberately left in _tickets until it expires or its session
    # dies. Popping on use would make "is this code known?" and "is this code
    # still usable?" the same question, so a user retyping a code they just
    # used would get bad_code instead of denied/expired. Sweeping is unchanged:
    # _sweep_tickets reaps on expires_at and ignores this.
    used: bool = False


class ControlSessionManager:
    def __init__(self):
        self._sessions: dict[str, ControlSession] = {}
        # Reverse index covering BOTH the phone and every controller, so
        # disconnect cleanup is one lookup regardless of which side dropped.
        self._by_client: dict[str, str] = {}
        self._tickets: dict[str, _Ticket] = {}
        self._cmd_hits: dict[str, list] = {}

    # ── sessions ──────────────────────────────────────────────────────

    def create(self, phone_client_id: str, phone_ws, owner_uid: str = None,
               caps: set = None):
        """Start a session for a phone.

        Returns (session, phone_secret, orphaned_ws) - the orphans belong to a
        previous session this client was running and MUST be told, or their
        browser sits on stale state forever believing it is still paired.
        """
        if len(self._sessions) >= MAX_SESSIONS:
            return None, None, []
        # destroy, not park: parking left the old session in _sessions with
        # the SAME phone_client_id, and its later sweep popped the reverse
        # index entry that by then pointed at the NEW session.
        _, orphans = self.destroy_for_client(phone_client_id)
        sid = secrets.token_urlsafe(12)
        sess = ControlSession(
            id=sid,
            phone_client_id=phone_client_id,
            phone_ws=phone_ws,
            phone_secret=secrets.token_urlsafe(24),
            owner_uid=owner_uid,
            caps=set(caps or ()),
        )
        self._sessions[sid] = sess
        self._by_client[phone_client_id] = sid
        return sess, sess.phone_secret, orphans

    def rejoin(self, phone_client_id: str, phone_ws, secret: str,
               caps: set = None) -> Optional[ControlSession]:
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
                sess.phone_gone_at = 0.0
                sess.last_seen = time.time()
                # Monotonic: a capability is added, never removed. Replacing
                # here was fail-OPEN -- one control_rejoin that omitted caps
                # (a dropped field, an older build, a truncated frame) silently
                # downgraded a live session back to immediate-attach, with
                # nothing on either side to show for it.
                #
                # The downgrade case it was meant to serve is handled properly
                # instead: handle_control_rejoin drops any outstanding pending
                # and answers phone_needs_update, and requestNewCode mints a
                # fresh session whose caps start empty.
                sess.caps |= set(caps or ())
                self._by_client[phone_client_id] = sess.id
                return sess
        return None

    def sessions_with_live_phone(self):
        """Every session whose phone socket is currently up, newest first.

        Ownership is deliberately NOT resolved here. owner_uid is a stored field
        that goes stale (nothing clears it when an account signs out while
        offline), so the caller derives the owner from the phone's LIVE socket
        identity instead. See handle_control_request.
        """
        live = [x for x in self._sessions.values()
                if x.phone_ws is not None and not x.phone_gone_at]
        return sorted(live, key=lambda x: x.last_seen, reverse=True)

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

    def peek(self, code: str):
        """Resolve a code WITHOUT spending it.

        Split out of the old redeem() because a code must survive being offered
        to a phone that turns out to be unreachable: the browser is told to try
        again, and the same code has to still work. Spending happens later, at
        the moment control is actually granted or refused — see burn().

        Returns (session, reason). reason is None on success, otherwise one of
        "bad_code" / "expired" / "denied", the last meaning the code was already
        spent on a decision.
        """
        if not isinstance(code, str):
            return None, "bad_code"
        # Look up BEFORE sweeping: sweeping first removes the expired ticket, so
        # a code that timed out would report bad_code ("that code didn't work")
        # instead of expired ("ask the phone for a new one") — two different
        # messages on the browser, and the wrong one sends the user hunting for
        # a typo that isn't there.
        t = self._tickets.get(code.strip().upper())
        self._sweep_tickets()
        if t is None:
            return None, "bad_code"
        if t.expires_at < time.time():
            return None, "expired"
        if t.used:
            return None, "denied"
        sess = self._sessions.get(t.session_id)
        if sess is None:
            return None, "expired"
        return sess, None

    def session_id_for_code(self, code: str) -> Optional[str]:
        """Which session a code belongs to, spent or not."""
        if not isinstance(code, str):
            return None
        t = self._tickets.get(code.strip().upper())
        return t.session_id if t else None

    def ticket_valid(self, code: str) -> bool:
        """Is this code still spendable?

        NOTE: the approve path checks the pending's own expires_at rather than
        calling this, because that value is already clamped to the ticket at
        peek time. Kept for tests and for callers that hold only a code.
        """
        if not isinstance(code, str):
            return False
        t = self._tickets.get(code.strip().upper())
        return bool(t and not t.used and t.expires_at >= time.time())

    def burn(self, code: str) -> bool:
        """Spend a code. Marks rather than pops — see _Ticket.used."""
        if not isinstance(code, str):
            return False
        t = self._tickets.get(code.strip().upper())
        if t is None or t.used:
            return False
        t.used = True
        return True

    def ticket_expiry(self, code: str) -> float:
        """When this code dies, for clamping a pending's own deadline."""
        if not isinstance(code, str):
            return 0.0
        t = self._tickets.get(code.strip().upper())
        return t.expires_at if t else 0.0

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

    def destroy_for_client(self, client_id: str):
        """Hard teardown: used for an explicit re-create or an explicit off.

        Unlike end_for_client this does NOT hold a grace window, and it burns
        any outstanding pairing codes — otherwise the previous code stays
        redeemable and lands a browser on a session nobody is driving.
        """
        sess = self.for_client(client_id)
        if sess is None:
            return None, []
        orphans = list(sess.controllers.values())
        self._sessions.pop(sess.id, None)
        self._by_client.pop(sess.phone_client_id, None)
        self._cmd_hits.pop(sess.phone_client_id, None)
        for cid in list(sess.controllers):
            self._by_client.pop(cid, None)
            self._cmd_hits.pop(cid, None)
        for code, t in list(self._tickets.items()):
            if t.session_id == sess.id:
                self._tickets.pop(code, None)
        return sess, orphans

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
            # Hold the session for the grace window so a reconnecting phone
            # can rejoin it. Controllers are told it went quiet, not that the
            # session is gone.
            sess.phone_ws = None
            sess.phone_gone_at = time.time()
            self._cmd_hits.pop(client_id, None)
            return None, list(sess.controllers.values())

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
                if (now - s.last_seen > SESSION_IDLE_TTL)
                or (s.phone_gone_at and now - s.phone_gone_at > PHONE_GRACE)]
        for s in dead:
            self._sessions.pop(s.id, None)
            # Guarded: a newer session may already own this client_id, and
            # popping it blind unmapped the LIVE session — the browser froze
            # on its last state a couple of minutes after pairing.
            if self._by_client.get(s.phone_client_id) == s.id:
                self._by_client.pop(s.phone_client_id, None)
                self._cmd_hits.pop(s.phone_client_id, None)
            for cid in list(s.controllers):
                if self._by_client.get(cid) == s.id:
                    self._by_client.pop(cid, None)
                    self._cmd_hits.pop(cid, None)
            for code, t in list(self._tickets.items()):
                if t.session_id == s.id:
                    self._tickets.pop(code, None)
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
