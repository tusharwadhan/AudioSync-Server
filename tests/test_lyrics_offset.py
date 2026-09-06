"""Offset identity + the sharing rule, against main.py's real functions."""
import ast
import asyncio
import hashlib
import io
import os
import sys
import types

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')
MAIN = 'd:/projects/audiosync-yt-dlp/server/main.py'


class Row:
    def __init__(self, uid, ms, when=0):
        self.uid, self.offset_ms, self.updated_at = uid, ms, when


def load(trusted=()):
    src = io.open(MAIN, encoding='utf-8').read()
    tree = ast.parse(src)
    lines = src.splitlines()
    want = {'_lyrics_hash', '_serve_offset'}
    chunks = ['\n'.join(lines[n.lineno - 1:n.end_lineno])
              for n in tree.body
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
              and n.name in want]
    assert len(chunks) == len(want), f"missing: {want}"

    rows = []

    class FakeResult:
        def scalars(self):
            return self

        def all(self):
            return rows

    class FakeSession:
        async def execute(self, *_a, **_k):
            return FakeResult()

    class Q:
        """select(...).where(...) has to chain, or every lookup raises and
        _serve_offset's except branch returns 0 -- which looks exactly like
        'no offset shared' and made half of these assertions pass for the
        wrong reason."""
        def where(self, *_a, **_k):
            return self

    ns = {
        'hashlib': hashlib, 'os': os, 'print': print,
        'select': lambda *a, **k: Q(),
        'models': types.SimpleNamespace(
            LyricsOffset=types.SimpleNamespace(
                video_id=None, lyrics_hash=None, uid=None)),
        '_TRUSTED_OFFSET_UIDS': set(trusted),
    }
    exec('\n\n'.join(chunks), ns)
    return ns, rows, FakeSession()


async def main():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    ns, rows, sess = load()
    h = ns['_lyrics_hash']

    print("\n-- the hash identifies WHICH lyrics --")
    a = [{"text": "Yo, his palms are sweaty"}, {"text": "knees weak"}]
    b = [{"text": "Something else entirely"}, {"text": "knees weak"}]
    check("same source + lines -> same hash", h("LRCLIB", a) == h("LRCLIB", a))
    check("different first line -> different hash", h("LRCLIB", a) != h("LRCLIB", b))
    check("different source -> different hash", h("LRCLIB", a) != h("YTM", a))
    check("different line count -> different hash", h("LRCLIB", a) != h("LRCLIB", a + a))
    check("plain lyrics hash without crashing", len(h("LRCLIB", [], "line one\nline two")) == 32)
    check("no lyrics at all still returns a hash", len(h("", [])) == 32)

    print("\n-- one person cannot move everyone --")
    rows.clear()
    check("no submissions -> no offset", await ns['_serve_offset'](sess, "v", "h") == 0)
    rows.append(Row("u1", 1200))
    check("one submission is NOT shared", await ns['_serve_offset'](sess, "v", "h") == 0)
    rows.append(Row("u2", 1100))
    check("two are still not enough", await ns['_serve_offset'](sess, "v", "h") == 0)
    rows.append(Row("u3", 1300))
    check("three agree -> the median is served",
          await ns['_serve_offset'](sess, "v", "h") == 1200)
    rows.append(Row("u4", 99000))
    check("an outlier does not drag the median",
          await ns['_serve_offset'](sess, "v", "h") in (1200, 1300))
    check("an empty hash is never looked up",
          await ns['_serve_offset'](sess, "v", "") == 0)

    print("\n-- a trusted curator wins outright --")
    ns2, rows2, sess2 = load(trusted=("me",))
    rows2.append(Row("stranger", 5000, when=99))
    check("a stranger alone still shares nothing",
          await ns2['_serve_offset'](sess2, "v", "h") == 0)
    rows2.append(Row("me", -800, when=1))
    check("the trusted value wins even against more strangers",
          await ns2['_serve_offset'](sess2, "v", "h") == -800)
    rows2.append(Row("me2", 400))
    check("still the trusted value", await ns2['_serve_offset'](sess2, "v", "h") == -800)

    print("\n-- a dead database costs the offset and nothing else --")

    class Dead:
        async def execute(self, *_a, **_k):
            raise RuntimeError("connection refused")

    check("read failure returns 0 rather than raising",
          await ns['_serve_offset'](Dead(), "v", "h") == 0)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
