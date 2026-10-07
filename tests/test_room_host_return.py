"""A host returning through the FRONT DOOR must get their room back.

The 2026-10-07 hunt: the Oct-04 ghost-dedupe in handle_join_room was dead
code (register_fcm_token pops the ghost's live token entry the instant the
same device re-registers, so live-token equality never matched) AND it ran
AFTER join_room — so a returning host joined as guest behind their own
ghost, and destroy_room_after_grace / the zombie sweep destroyed the room
with the host inside it.

The fix: handle_disconnect stamps last_fcm_token onto the kept ghost; the
dedupe matches that snapshot and runs BEFORE join_room, so the promotion
check sees a hostless room and re-promotes the returning device.

This file pins the room_manager halves (promotion with the ghost absent,
was_host stickiness, the snapshot field) and source-pins the main.py wiring
(dedupe-before-join, snapshot match, disconnect stamp, same-room idempotent
guard, two-tick zombie sweep).
"""
import re
import sys

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')
import room_manager as rm


class FakeWS:
    """Stand-in for a Starlette WebSocket; never awaited in these paths."""


def make_room(mgr):
    room = mgr.create_room("HostPhone", FakeWS(), host_name="Tushar")
    room2, promoted = mgr.join_room(room.code, "GuestPhone", FakeWS(), name="Komal")
    assert room2 is room and not promoted
    return room


def main():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    # -- the bug shape: ghost present at join time blocks promotion --
    print("\n-- ghost present (pre-fix ordering): promotion blocked --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    mgr.disconnect_member("HostPhone")
    check("ghost kept reconnecting", room.members["HostPhone"].reconnecting)
    r, promoted = mgr.join_room(room.code, "HostPhone2", FakeWS(), name="Tushar")
    check("join behind own ghost = guest (why ordering matters)",
          promoted is False and r.host_id == "HostPhone")
    # cleanup for next scenario

    # -- the fix shape: ghost popped BEFORE join → host comes back --
    print("\n-- ghost popped first (fixed ordering): host re-promoted --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    mgr.disconnect_member("HostPhone")
    # what handle_join_room's dedupe does, minus tasks (none in harness):
    room.members.pop("HostPhone", None)
    mgr._client_to_room.pop("HostPhone", None)
    mgr._pending_disconnects.pop("HostPhone", None)
    r, promoted = mgr.join_room(room.code, "HostPhone2", FakeWS(), name="Tushar")
    check("promoted back to host", promoted is True)
    check("host_id follows the returning device", r.host_id == "HostPhone2")
    check("host_name refreshed", r.host_name == "Tushar")
    check("sticky was_host set on promotion",
          r.members["HostPhone2"].was_host is True)
    check("room survives a zombie-sweep condition check",
          r.host_id in r.members)
    check("guest untouched", "GuestPhone" in r.members)

    # -- snapshot field exists and defaults empty --
    print("\n-- last_fcm_token snapshot field --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    m = room.members["GuestPhone"]
    check("field present, default None", m.last_fcm_token is None)
    m.last_fcm_token = "tok_abc"
    check("snapshot sticks on the ghost object",
          room.members["GuestPhone"].last_fcm_token == "tok_abc")

    # -- main.py wiring (source pins; live behavior runs on the server) --
    print("\n-- main.py source pins --")
    src = open('d:/projects/audiosync-yt-dlp/server/main.py', encoding='utf-8').read()

    # dedupe must run BEFORE join_room inside handle_join_room
    handler = src.split("async def handle_join_room", 1)[1]
    handler = handler.split("async def ", 1)[0]
    dedupe_at = handler.find("deduped ghost")
    join_at = handler.find("room_manager.join_room")
    check("dedupe block exists in handle_join_room", dedupe_at != -1)
    check("dedupe runs BEFORE join_room", -1 < dedupe_at < join_at)
    check("dedupe matches the SNAPSHOT, not live tokens",
          "_ghost.last_fcm_token == _token" in handler)
    check("no await between pop and join_room",
          "await" not in handler[dedupe_at:join_at])
    check("same-room idempotent guard present (double-join must not destroy)",
          "idempotent same-room join" in handler)
    guard_at = handler.find("idempotent same-room join")
    leave_at = handler.find("room_manager.leave_room")
    check("idempotent guard sits BEFORE the leave-first block",
          -1 < guard_at and guard_at < leave_at)

    check("handle_disconnect stamps the ghost snapshot",
          re.search(r"last_fcm_token = _fcm_tokens\.get\(client_id\)", src)
          is not None)

    # (window, not a def-split: _cleanup_loop is itself an inner async def)
    sweep = src.split("async def startup_room_cleanup", 1)[1][:4000]
    check("zombie sweep requires two sightings (_zombie_since)",
          "_zombie_since" in sweep
          and "second sighting required before destroy" in sweep)

    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
