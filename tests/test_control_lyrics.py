"""The browser's lyrics relay, against main.py's real handler.

What matters: only an attached controller can ask, the reply carries the
videoId for the browser's stale-guard, the line cap holds, and a resolver
blow-up still answers (a silent drop would leave skeletons shimmering)."""
import ast
import asyncio
import io
import sys
import time
import types

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')
import control_session

MAIN = 'd:/projects/audiosync-yt-dlp/server/main.py'
sent = []


class FakeWS:
    def __init__(self, name):
        self.name = name
        self.headers = {"user-agent": "Mozilla/5.0 Chrome/120 Safari/537"}
        self.client_state = 'CONNECTED'


async def fake_ws_send(ws, message):
    sent.append((ws, message))


class FakeResp:
    def __init__(self, lines):
        self._d = {"success": True, "videoId": "ignored", "hasTimestamps": True,
                   "lines": lines, "plainLyrics": None, "source": "TEST",
                   "error": None, "offsetMs": 250, "lyricsHash": "h"}

    def model_dump(self):
        return dict(self._d)


CALLS = []
_UIDS = {}
OWN_OFFSETS = {}          # uid -> offset the owner personally saved


async def fake_offset_for_uid(video_id, lyrics_hash, uid):
    return OWN_OFFSETS.get(uid)


async def fake_endpoint(video_id, title="", artist="", duration=0):
    CALLS.append({"video_id": video_id, "title": title, "artist": artist,
                  "duration": duration})
    if video_id == "boom":
        raise RuntimeError("resolver exploded")
    n = 500 if video_id == "huge" else 3
    return FakeResp([{"text": f"line {i}", "startMs": i * 1000, "endMs": 0}
                     for i in range(n)])


def load():
    src = io.open(MAIN, encoding='utf-8').read()
    tree = ast.parse(src)
    lines = src.splitlines()
    chunks = ['\n'.join(lines[n.lineno - 1:n.end_lineno]) for n in tree.body
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
              and n.name == 'handle_control_lyrics']
    assert chunks, 'handler not found'
    ns = {'asyncio': asyncio, 'time': time,
          'ws_send': fake_ws_send, 'control_manager': None,
          'get_lyrics_endpoint': fake_endpoint,
          'uid_for_ws': lambda cid: _UIDS.get(cid),
          '_offset_for_uid': fake_offset_for_uid,
          'WebSocket': object, 'print': print,
          'WebSocketState': types.SimpleNamespace(CONNECTED='CONNECTED')}
    exec('\n\n'.join(chunks), ns)
    return ns


async def main():
    ns = load()
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    async def settle():
        for _ in range(6):
            await asyncio.sleep(0)

    def fresh():
        cm = control_session.ControlSessionManager()
        ns['control_manager'] = cm
        sent.clear(); CALLS.clear(); _UIDS.clear(); OWN_OFFSETS.clear()
        phone = FakeWS("phone")
        sess, _, _ = cm.create("phoneC", phone, caps={"approval"})
        browser = FakeWS("browser")
        cm.attach_controller(sess.id, "bc1", browser, meta={})
        return cm, sess, phone, browser

    h = ns['handle_control_lyrics']

    print("\n-- happy path --")
    cm, sess, phone, b = fresh()
    await h("bc1", b, {"videoId": "abc123", "title": "T", "artist": "A",
                       "duration": 213})
    await settle()
    m = sent[-1][1]
    check("reply arrives", m["type"] == "control_lyrics_result")
    check("videoId echoed for the stale-guard", m["videoId"] == "abc123")
    check("lines + offset carried", len(m["lines"]) == 3 and m["offsetMs"] == 250)
    check("metadata forwarded to the resolver",
          CALLS[0] == {"video_id": "abc123", "title": "T", "artist": "A",
                       "duration": 213})

    print("\n-- the phone owner's own timing nudge wins --")
    cm, sess, phone, b = fresh()
    _UIDS["phoneC"] = "uid-owner"
    OWN_OFFSETS["uid-owner"] = -900
    await h("bc1", b, {"videoId": "abc123"})
    await settle()
    check("owner's saved offset replaces the shared one",
          sent[-1][1]["offsetMs"] == -900)

    cm, sess, phone, b = fresh()
    _UIDS["phoneC"] = "uid-owner"          # signed in, but never nudged
    await h("bc1", b, {"videoId": "abc123"})
    await settle()
    check("no personal row falls back to the shared value",
          sent[-1][1]["offsetMs"] == 250)

    cm, sess, phone, b = fresh()           # phone signed out entirely
    await h("bc1", b, {"videoId": "abc123"})
    await settle()
    check("no account still serves the shared value",
          sent[-1][1]["offsetMs"] == 250)

    print("\n-- gates --")
    cm, sess, phone, b = fresh()
    stranger = FakeWS("stranger")
    await h("nobody", stranger, {"videoId": "abc123"})
    await settle()
    check("a non-controller gets not_paired",
          sent[-1][1] == {"type": "control_error", "code": "not_paired"})
    n0 = len(CALLS)
    check("...and never reaches the resolver", n0 == 0)
    # The PHONE is in the session map but is not a controller.
    await h("phoneC", phone, {"videoId": "abc123"})
    await settle()
    check("the phone itself is refused too",
          sent[-1][1]["code"] == "not_paired" and len(CALLS) == 0)

    print("\n-- seatbelts --")
    cm, sess, phone, b = fresh()
    await h("bc1", b, {"videoId": "huge"})
    await settle()
    check("line cap holds at 400", len(sent[-1][1]["lines"]) == 400)

    cm, sess, phone, b = fresh()
    await h("bc1", b, {"videoId": "boom"})
    await settle()
    m = sent[-1][1]
    check("a resolver blow-up still answers (no eternal skeletons)",
          m["type"] == "control_lyrics_result" and m["success"] is False
          and m["videoId"] == "boom")

    cm, sess, phone, b = fresh()
    await h("bc1", b, {"videoId": "", "duration": "not-a-number"})
    await settle()
    check("empty videoId is a silent no-op", len(CALLS) == 0 and not sent)

    cm, sess, phone, b = fresh()
    await h("bc1", b, {"videoId": "abc123", "duration": "nan"})
    await settle()
    check("garbage duration coerces to 0", CALLS[0]["duration"] == 0)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
