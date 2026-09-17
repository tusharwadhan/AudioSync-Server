"""A state push carrying battery must survive set_state's size gate.

set_state rejects an oversized state WHOLE — so a battery block that tips a
full push over MAX_STATE_BYTES would not degrade the battery display, it
would silence the entire remote. This pins the headroom against the biggest
push the phone can build (QUEUE_PUSH_LIMIT rows of maximum-length titles)
and proves the fields relay unchanged.
"""
import json
import sys

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')
import control_session

QUEUE_PUSH_LIMIT = 25          # RemoteControl.kt:809

# Exactly what BatteryTelemetry.snapshot emits with every optional field
# present — the worst case for size.
BATTERY = {
    "pct": 87, "state": "charging", "source": "ac", "tempC": 31.4,
    "voltV": 4.213, "health": "good", "currentMa": 1842.0,
    "chargeUah": 3921000, "pctFine": 87.3, "toFullSec": 2280,
}


# RemoteControl.pushStateInner caps every row: title.take(70),
# artist.take(40) — with a comment saying it exists precisely because the
# server rejects an oversized state rather than truncating it. Modelling
# longer rows here would test a push the app cannot build.
ROW_TITLE_CAP = 70
ROW_ARTIST_CAP = 40


def worst_case_state():
    return {
        # The TOP-LEVEL title is not capped, so it gets a length past any
        # real YouTube title.
        "title": "X" * 200, "artist": "Y" * 120, "isPlaying": True,
        "position": 123.456, "duration": 245.0, "volume": 0.7,
        "videoId": "kJQP7kiw5Fk", "serverTime": 1789123456789,
        "upNext": [
            {"videoId": "v%011d" % i,
             "title": "X" * ROW_TITLE_CAP, "artist": "Y" * ROW_ARTIST_CAP,
             "queued": True}
            for i in range(QUEUE_PUSH_LIMIT)
        ],
        "battery": dict(BATTERY),
    }


def main():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    cm = control_session.ControlSessionManager()
    sess, _, _ = cm.create("phoneC", object(), caps={"approval"})

    print("\n-- the size gate --")
    state = worst_case_state()
    size = len(json.dumps(state))
    cap = control_session.MAX_STATE_BYTES
    print(f"      worst-case push {size}B of {cap}B")
    check("worst-case push with battery is accepted", cm.set_state(sess, state))
    check("...with room to spare, not by a whisker", size < cap * 0.75)

    bare = worst_case_state()
    bare.pop("battery")
    cost = size - len(json.dumps(bare))
    print(f"      battery costs {cost}B")
    check("battery block stays under 300B", cost < 300)

    print("\n-- relayed unchanged --")
    check("every field survives set_state",
          sess.last_state["battery"] == BATTERY)
    check("floats keep their precision",
          sess.last_state["battery"]["voltV"] == 4.213
          and sess.last_state["battery"]["pctFine"] == 87.3)

    print("\n-- an oversized push is still refused whole --")
    huge = worst_case_state()
    huge["title"] = "Z" * (cap + 10)
    check("too-large state rejected", cm.set_state(huge and sess, huge) is False)
    check("and the previous good state is left intact",
          sess.last_state["battery"] == BATTERY)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
