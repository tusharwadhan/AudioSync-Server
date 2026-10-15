"""The per-room transport gate must coalesce song-change bursts.

A machine-gunned next/play makes every member run one on-device extraction
per skip — the burst shape that trips YouTube's bot wall (429 → fallback →
403 → flagged session). The gate: leading edge applies instantly, anything
further inside TRANSPORT_GATE_WINDOW collapses into ONE latest-wins pending
slot fired on the trailing edge, all applications serialized per room.

This file executes the REAL gate code (extracted verbatim from main.py
between its sentinel lines and exec'd with stubbed inner handlers — main.py
itself can't be imported locally: fastapi/yt_dlp etc.), plus source pins on
the wiring that must never regress (create_task submission for BOTH
commands, identity re-validation, cleanup at the destruction sites).
"""
import asyncio
import sys
import time

MAIN = 'd:/projects/audiosync-yt-dlp/server/main.py'


class FakeRoom:
    def __init__(self, code):
        self.code = code


class FakeRM:
    def __init__(self):
        self.rooms = {}
        self.host_ok = True

    def is_host(self, cid):
        return self.host_ok


def load_gate(window=0.08):
    """Exec the real gate block from main.py into a fresh namespace."""
    src = open(MAIN, encoding='utf-8').read()
    start = src.index("TRANSPORT_GATE_WINDOW = 1.5")
    end = src.index("async def handle_play")
    block = src[start:end]
    ns = {"asyncio": asyncio, "time": time, "print": lambda *a, **k: None,
          "room_manager": FakeRM()}
    exec(compile(block, "gate_block", "exec"), ns)
    ns["TRANSPORT_GATE_WINDOW"] = window
    return ns


def main():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    async def run_all():
        W = 0.08

        # ── applied-command recorder with overlap detection ──
        def make_stubs(ns, applied, delay=0.0):
            state = {"active": 0, "overlap": False}

            async def next_inner(cid):
                state["active"] += 1
                if state["active"] > 1:
                    state["overlap"] = True
                if delay:
                    await asyncio.sleep(delay)
                else:
                    await asyncio.sleep(0)
                applied.append(("next", cid))
                state["active"] -= 1

            async def play_inner(cid, msg):
                state["active"] += 1
                if state["active"] > 1:
                    state["overlap"] = True
                await asyncio.sleep(0)
                applied.append(("play", msg["videoId"]))
                state["active"] -= 1

            ns["_handle_next_inner"] = next_inner
            ns["_handle_play_inner"] = play_inner
            return state

        # -- single press: immediate --
        print("\n-- single press --")
        ns = load_gate(W)
        room = FakeRoom("AAAA"); ns["room_manager"].rooms["AAAA"] = room
        applied = []; make_stubs(ns, applied)
        await ns["_gate_submit"](room, "next", "host", None)
        check("applied immediately", applied == [("next", "host")])

        # -- 5-press burst: exactly leading + trailing(latest) --
        print("\n-- 5 plays in a burst --")
        ns = load_gate(W)
        room = FakeRoom("BBBB"); ns["room_manager"].rooms["BBBB"] = room
        applied = []; state = make_stubs(ns, applied)
        for v in ["a", "b", "c", "d", "e"]:
            asyncio.create_task(ns["_gate_submit"](room, "play", "host", {"videoId": v}))
            await asyncio.sleep(0.005)
        await asyncio.sleep(W * 4)
        check("exactly 2 applies", len(applied) == 2)
        check("leading was first press", applied and applied[0] == ("play", "a"))
        check("trailing was LATEST press", len(applied) == 2 and applied[1] == ("play", "e"))
        check("no overlapping applies", not state["overlap"])

        # -- spaced presses: all immediate --
        print("\n-- spaced presses --")
        ns = load_gate(W)
        room = FakeRoom("CCCC"); ns["room_manager"].rooms["CCCC"] = room
        applied = []; make_stubs(ns, applied)
        for v in ["x", "y", "z"]:
            await ns["_gate_submit"](room, "play", "host", {"videoId": v})
            await asyncio.sleep(W * 1.6)
        check("all three applied", [p[1] for p in applied] == ["x", "y", "z"])

        # -- room identity swap kills the trailing command --
        print("\n-- re-minted code: trailing dropped --")
        ns = load_gate(W)
        room = FakeRoom("DDDD"); ns["room_manager"].rooms["DDDD"] = room
        applied = []; make_stubs(ns, applied)
        asyncio.create_task(ns["_gate_submit"](room, "next", "host", None))
        await asyncio.sleep(0.005)
        asyncio.create_task(ns["_gate_submit"](room, "next", "host", None))
        await asyncio.sleep(0.005)
        ns["room_manager"].rooms["DDDD"] = FakeRoom("DDDD")  # same code, NEW room
        await asyncio.sleep(W * 4)
        check("only the leading applied", len(applied) == 1)

        # -- host changed mid-window: pending skipped --
        print("\n-- host change mid-window --")
        ns = load_gate(W)
        room = FakeRoom("EEEE"); ns["room_manager"].rooms["EEEE"] = room
        applied = []; make_stubs(ns, applied)
        asyncio.create_task(ns["_gate_submit"](room, "next", "host", None))
        await asyncio.sleep(0.005)
        asyncio.create_task(ns["_gate_submit"](room, "next", "host", None))
        await asyncio.sleep(0.005)
        ns["room_manager"].host_ok = False
        await asyncio.sleep(W * 4)
        check("pending dropped for ex-host", len(applied) == 1)

        # -- discard cancels the sleeping trailing task --
        print("\n-- discard_transport_gate --")
        ns = load_gate(W)
        room = FakeRoom("FFFF"); ns["room_manager"].rooms["FFFF"] = room
        applied = []; make_stubs(ns, applied)
        asyncio.create_task(ns["_gate_submit"](room, "next", "host", None))
        await asyncio.sleep(0.005)
        asyncio.create_task(ns["_gate_submit"](room, "next", "host", None))
        await asyncio.sleep(0.005)
        ns["discard_transport_gate"]("FFFF")
        await asyncio.sleep(W * 4)
        check("trailing cancelled by discard", len(applied) == 1)
        check("gate entry removed", "FFFF" not in ns["_transport_gates"])

        # -- long leading apply: trailing still paced from COMPLETION --
        print("\n-- pacing measured from completion --")
        ns = load_gate(W)
        room = FakeRoom("GGGG"); ns["room_manager"].rooms["GGGG"] = room
        applied = []; state = make_stubs(ns, applied, delay=W * 2)  # slow apply
        t0 = time.monotonic()
        asyncio.create_task(ns["_gate_submit"](room, "play", "host", {"videoId": "s1"}))
        await asyncio.sleep(0.005)
        asyncio.create_task(ns["_gate_submit"](room, "play", "host", {"videoId": "s2"}))
        await asyncio.sleep(W * 8)
        check("both applied, serialized, no overlap",
              [p[1] for p in applied] == ["s1", "s2"] and not state["overlap"])

        return True

    asyncio.run(run_all())

    # ── source pins on the wiring ──
    print("\n-- main.py source pins --")
    src = open(MAIN, encoding='utf-8').read()
    play = src.split("async def handle_play", 1)[1].split("async def ", 1)[0]
    nxt = src.split("async def handle_next", 1)[1].split("async def ", 1)[0]
    check("handle_play submits via create_task (inline await would deadlock "
          "the host socket loop)",
          'create_task(_gate_submit(room, "play"' in play)
    check("handle_next submits via create_task",
          'create_task(_gate_submit(room, "next"' in nxt)
    check("trailing task re-validates room IDENTITY (`is`), not just code",
          "room_manager.rooms.get(g.room.code) is not g.room" in src)
    check("gate cleanup at >=5 destruction sites + sweep prune",
          src.count("discard_transport_gate(") >= 6)
    check("window measured from apply completion",
          "g.last = time.monotonic()" in src)

    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
