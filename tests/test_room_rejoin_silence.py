"""A secret-verified rejoin that replaces a live socket must be silent.

The 2026-09 flap storm: a client-side reconnect loop swapped sockets every
few seconds, every swap rejoined the room with a fresh client_id, and every
rejoin was broadcast as member_joined — which every other phone's roster
diff rendered as an "X left / X joined" pair.

This file pins room_manager.rejoin_room's
(room, was_host, displaced_id, displaced_was_live) return contract, which
main.py's handle_rejoin_room turns into broadcast silence. The handler's own
wiring is only source-pinned here (the elif branch must exist); its live
behavior is exercised on the server, not in this harness. displaced_was_live
must be True ONLY on the secret-verified path: the legacy previousClientId
path can displace someone else (ids are public in every roster broadcast),
and that abuse must stay visible.
"""
import sys

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')
import room_manager as rm


class FakeWS:
    """Stand-in for a Starlette WebSocket; never awaited in these paths."""


def make_room(mgr):
    room = mgr.create_room("HostPhone", FakeWS(), host_name="Tushar")
    guest_ws = FakeWS()
    room2, promoted = mgr.join_room(room.code, "GuestPhone", guest_ws, name="Komal")
    assert room2 is room and not promoted
    return room


def main():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    # -- live socket swap via secret: silent --
    print("\n-- live swap via secret --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    secret = room.members["GuestPhone"].secret
    r, was_host, displaced, live = mgr.rejoin_room(
        "GuestPhone2", FakeWS(), room.code, "Komal", member_secret=secret)
    check("room returned", r is room)
    check("not host", was_host is False)
    check("displaced the old id", displaced == "GuestPhone")
    check("flagged LIVE (server never saw a disconnect)", live is True)
    check("roster count unchanged", len(room.members) == 2)
    check("old id gone from roster", "GuestPhone" not in room.members)
    check("secret carried forward",
          room.members["GuestPhone2"].secret == secret)

    # -- second swap in a row (the storm's steady state) --
    r, _, displaced, live = mgr.rejoin_room(
        "GuestPhone3", FakeWS(), room.code, "Komal", member_secret=secret)
    check("chained swap also displaced+live",
          displaced == "GuestPhone2" and live is True)

    # -- rejoin within grace: displaced, but NOT live --
    print("\n-- within-grace rejoin --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    secret = room.members["GuestPhone"].secret
    code, was_host_d, kept = mgr.disconnect_member("GuestPhone")
    check("member kept reconnecting", kept and room.members["GuestPhone"].reconnecting)
    r, _, displaced, live = mgr.rejoin_room(
        "GuestPhone2", FakeWS(), room.code, "Komal", member_secret=secret)
    check("displaced id reported for grace-task cancel", displaced == "GuestPhone")
    check("NOT flagged live (was reconnecting)", live is False)

    # -- fresh rejoin after the grace reaped them: announce --
    print("\n-- after grace expiry --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    secret = room.members["GuestPhone"].secret
    mgr.disconnect_member("GuestPhone")
    fcode, remaining = mgr.finalize_disconnect("GuestPhone")
    check("finalize removed them", fcode == room.code and "GuestPhone" not in room.members)
    r, _, displaced, live = mgr.rejoin_room(
        "GuestPhone2", FakeWS(), room.code, "Komal", member_secret=secret)
    check("nothing displaced -> caller announces", displaced is None and live is False)
    check("they are back in the roster", "GuestPhone2" in room.members)

    # -- double rejoin from the SAME id: silent, no duplicate --
    print("\n-- double rejoin, same id --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    secret = room.members["GuestPhone"].secret
    r, _, displaced, live = mgr.rejoin_room(
        "GuestPhone", FakeWS(), room.code, "Komal", member_secret=secret)
    check("displaced self, live -> silent", displaced == "GuestPhone" and live is True)
    check("no duplicate roster entry", len(room.members) == 2)

    # -- legacy path (no secret): displacement must stay VISIBLE --
    print("\n-- legacy previousClientId swap --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    room.members["GuestPhone"].secret = ""   # install predating secrets
    r, _, displaced, live = mgr.rejoin_room(
        "GuestPhone2", FakeWS(), room.code, "Komal",
        previous_client_id="GuestPhone")
    check("legacy swap reports displaced id (for grace cancel)",
          displaced == "GuestPhone")
    check("legacy swap NOT flagged live — must be announced (previousClientId "
          "is unverified, silence would hide an eviction)", live is False)

    # -- host swap keeps host, still silent --
    print("\n-- host live swap --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    host_secret = room.members["HostPhone"].secret
    r, was_host, displaced, live = mgr.rejoin_room(
        "HostPhone2", FakeWS(), room.code, "Tushar", member_secret=host_secret)
    check("host restored", was_host is True and room.host_id == "HostPhone2")
    check("host swap displaced+live", displaced == "HostPhone" and live is True)

    # -- a stranger's bogus secret must not displace anyone --
    print("\n-- bogus secret --")
    mgr = rm.RoomManager()
    room = make_room(mgr)
    r, was_host, displaced, live = mgr.rejoin_room(
        "Stranger", FakeWS(), room.code, "Mallory", member_secret="nope")
    check("nothing displaced", displaced is None and live is False)
    check("both real members still present",
          "HostPhone" in room.members and "GuestPhone" in room.members)
    check("stranger did not become host", room.host_id == "HostPhone")

    # -- the handler wiring exists (source pin; behavior runs on the server) --
    print("\n-- main.py silence branch present --")
    src = open('d:/projects/audiosync-yt-dlp/server/main.py',
               encoding='utf-8').read()
    check("handle_rejoin_room has the displaced_was_live silent branch",
          "elif displaced_was_live:" in src
          and "replacing a live socket — silent (no flap)" in src)
    check("grace cancel uses t.cancel()'s verdict (a grace that already "
          "fired must be announced)", "if t is not None and t.cancel():" in src)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
