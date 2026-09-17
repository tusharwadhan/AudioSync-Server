"""The browser's library relay, against main.py's real handler.

The load-bearing property: the uid is the BROWSER socket's, never the
phone's — a code-paired stranger must not read the owner's favorites. Plus
the presentation rules (tombstones dropped, ordering, cap) and in-band
errors that never touch the shared control_error channel."""
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
_UIDS = {}


class FakeWS:
    def __init__(self, name):
        self.name = name
        self.headers = {"user-agent": "Mozilla/5.0 Chrome/120"}
        self.client_state = 'CONNECTED'


async def fake_ws_send(ws, message):
    sent.append((ws, message))


class Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


QUERIED = []
FAVS = {}


async def q_favorites(session, uid, since=0):
    QUERIED.append(('favorites', uid))
    return FAVS.get(uid, [])


async def q_playlists(session, uid, since=0):
    QUERIED.append(('playlists', uid))
    return [Row(syncId='p1', name='Drive', createdAt=2, deleted=False),
            Row(syncId='p0', name='Old', createdAt=1, deleted=True)]


async def q_songs(session, uid, since=0, playlist_sync_id=None):
    QUERIED.append(('songs', uid, playlist_sync_id))
    # Returned out of position order on purpose: the grid's cover must come
    # from the LOWEST position, not from whichever row the DB hands back first.
    return [Row(playlistSyncId='p1', videoId='s2', title='Two', uploader='U',
                duration=100, thumbnail=None, position=2, deleted=False),
            Row(playlistSyncId='p1', videoId='s1', title='One', uploader='U',
                duration=90, thumbnail=None, position=1, deleted=False)]


class FakeSessionCtx:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


def load(factory='ok'):
    src = io.open(MAIN, encoding='utf-8').read()
    tree = ast.parse(src)
    lines = src.splitlines()
    chunks = []
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and n.name == 'handle_control_library':
            chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and n.name == '_yt_thumb':
            chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
        elif isinstance(n, ast.Assign):
            t = n.targets[0]
            if isinstance(t, ast.Name) and t.id in ('_LIBRARY_CAP', '_FAVOURITES_ID'):
                chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
    # handler + cap + sentinel id + thumb helper
    assert len(chunks) == 4, f'expected 4 chunks, got {len(chunks)}'
    # The constants and helper sit AFTER the handler in main.py — fine at
    # runtime, since names resolve at call time — so hoist them here.
    chunks.sort(key=lambda c: 0 if c.lstrip().startswith(
        ('_LIBRARY_CAP', '_FAVOURITES_ID', 'def _yt_thumb')) else 1)
    ns = {'asyncio': asyncio, 'time': time, 'ws_send': fake_ws_send,
          'control_manager': None, 'print': print,
          'uid_for_ws': lambda cid: _UIDS.get(cid),
          'try_session_factory': (lambda: (FakeSessionCtx if factory == 'ok' else None)),
          'sync': types.SimpleNamespace(query_favorites=q_favorites,
                                        query_playlists=q_playlists,
                                        query_playlist_songs=q_songs),
          'WebSocket': object,
          'WebSocketState': types.SimpleNamespace(CONNECTED='CONNECTED')}
    exec('\n\n'.join(chunks), ns)
    return ns


async def settle():
    for _ in range(8):
        await asyncio.sleep(0)


async def main():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    def fresh(ns):
        cm = control_session.ControlSessionManager()
        ns['control_manager'] = cm
        sent.clear(); QUERIED.clear(); _UIDS.clear(); FAVS.clear()
        phone = FakeWS("phone")
        sess, _, _ = cm.create("phoneC", phone, caps={"approval"})
        _UIDS["phoneC"] = "uid-PHONE-OWNER"
        browser = FakeWS("browser")
        cm.attach_controller(sess.id, "bc1", browser, meta={})
        return cm, sess, phone, browser

    ns = load()
    h = ns['handle_control_library']
    CAP = ns['_LIBRARY_CAP']

    print("\n-- whose library: the browser's, never the phone's --")
    cm, sess, phone, b = fresh(ns)
    _UIDS["bc1"] = "uid-BROWSER"
    FAVS["uid-BROWSER"] = [Row(videoId='f1', title='Mine', uploader='',
                               duration=1, thumbnail=None, favoritedAt=5,
                               deleted=False)]
    FAVS["uid-PHONE-OWNER"] = [Row(videoId='X', title='OWNERS', uploader='',
                                   duration=1, thumbnail=None, favoritedAt=9,
                                   deleted=False)]
    await h("bc1", b, {"kind": "favorites"})
    await settle()
    m = sent[-1][1]
    check("query ran as the BROWSER uid", QUERIED == [('favorites', 'uid-BROWSER')])
    check("owner's rows never appear",
          [i["videoId"] for i in m["items"]] == ['f1'])

    print("\n-- a code-paired stranger with no account gets an in-band miss --")
    cm, sess, phone, b = fresh(ns)
    await h("bc1", b, {"kind": "favorites"})
    await settle()
    m = sent[-1][1]
    check("not_signed_in, in-band, not control_error",
          m["type"] == "control_library_result" and m["error"] == "not_signed_in")
    check("and no query ran", QUERIED == [])

    print("\n-- presentation rules --")
    cm, sess, phone, b = fresh(ns)
    _UIDS["bc1"] = "u"
    FAVS["u"] = [Row(videoId=f'v{i}', title=f't{i}', uploader='', duration=1,
                     thumbnail=None, favoritedAt=i, deleted=(i % 2 == 0))
                 for i in range(500)]
    await h("bc1", b, {"kind": "favorites"})
    await settle()
    m = sent[-1][1]
    check("tombstones dropped", all(int(i["videoId"][1:]) % 2 == 1 for i in m["items"]))
    check(f"cap holds at {CAP} + truncated flagged",
          len(m["items"]) == CAP and m.get("truncated") is True)
    check("newest favourite first", m["items"][0]["videoId"] == 'v499')

    cm, sess, phone, b = fresh(ns)
    _UIDS["bc1"] = "u"
    FAVS["u"] = [Row(videoId='fv', title='Fav', uploader='', duration=1,
                     thumbnail=None, favoritedAt=9, deleted=False)]
    await h("bc1", b, {"kind": "playlists"})
    await settle()
    m = sent[-1][1]
    ids = [i["syncId"] for i in m["items"]]
    check("favourites leads the grid as a system row",
          ids[0] == '__favourites__' and m["items"][0]["system"] is True)
    check("deleted playlist hidden, live one present", ids[1:] == ['p1'])
    check("favourites card counts + covers the favourites themselves",
          m["items"][0]["count"] == 1 and 'fv' in (m["items"][0]["cover"] or ''))
    check("playlist card carries its song count", m["items"][1]["count"] == 2)
    check("cover is the LOWEST-position song, not the first row returned",
          's1' in (m["items"][1]["cover"] or ''))

    cm, sess, phone, b = fresh(ns)
    _UIDS["bc1"] = "u"
    FAVS["u"] = [Row(videoId='zz', title='Z', uploader='', duration=1,
                     thumbnail=None, favoritedAt=1, deleted=False)]
    await h("bc1", b, {"kind": "playlist_songs", "playlistId": "__favourites__"})
    await settle()
    check("opening the system card drills into favourites, not a playlist",
          QUERIED[-1][0] == 'favorites' and sent[-1][1]["kind"] == 'favorites')

    cm, sess, phone, b = fresh(ns)
    _UIDS["bc1"] = "u"
    await h("bc1", b, {"kind": "playlist_songs", "playlistId": "p1"})
    await settle()
    m = sent[-1][1]
    check("playlist filter forwarded", QUERIED[-1] == ('songs', 'u', 'p1'))
    check("songs in position order",
          [i["videoId"] for i in m["items"]] == ['s1', 's2'])
    check("playlistId echoed for the stale-guard", m["playlistId"] == 'p1')

    print("\n-- failure shapes --")
    ns2 = load(factory='dead')
    cm, sess, phone, b = fresh(ns2)
    _UIDS["bc1"] = "u"
    await ns2['handle_control_library']("bc1", b, {"kind": "favorites"})
    await settle()
    check("dead DB is in-band unavailable", sent[-1][1]["error"] == "unavailable")

    cm, sess, phone, b = fresh(ns)
    stranger = FakeWS("s")
    await h("nobody", stranger, {"kind": "favorites"})
    await settle()
    check("a non-controller is refused", sent[-1][1]["code"] == "not_paired")

    cm, sess, phone, b = fresh(ns)
    _UIDS["bc1"] = "u"
    n0 = len(sent)
    await h("bc1", b, {"kind": "everything_please"})
    await settle()
    check("an unknown kind is a silent no-op", len(sent) == n0)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
