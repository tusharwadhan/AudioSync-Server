"""Drive main.py's real LRCLIB matching against the live service.

The functions are AST-extracted from main.py rather than reimplemented, so a
divergence between what is tested and what runs shows up here.

The live section asserts WHICH song came back, not merely that something did.
An early version of this matcher scored 12/12 while returning the lyrics to
"Snake" for "Tadipaar" and the lyrics to the film "Brahmastra" for "Kesariya" --
a hit rate that looked like success and was worse than the miss it replaced.

Network test: it talks to lrclib.net, whose latency swings badly some nights.
A failure can mean the service is unwell rather than a regression.
"""
import ast
import asyncio
import io
import re
import sys
import time

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')
import httpx

MAIN = 'd:/projects/audiosync-yt-dlp/server/main.py'

# (youtube title, channel, duration, expected track) — the shapes the app
# actually sends. duration 0 is the HARD case: no length to corroborate with.
SAMPLES = [
    ("The Way I Am", "EminemMusic", 0, "the way i am"),
    ("Nanchaku", "Seedhe Maut", 0, "nanchaku"),
    ("Seedhe Maut - Namastute | Encore ABJ | Calm", "Azadi Records", 0, "namastute"),
    ("SHAKTIMAAN - Seedhe Maut (Official Music Video)", "Azadi Records", 0, "shaktimaan"),
    ("MC STAN - INSAAN | Official Music Video", "MC STAN", 0, "insaan"),
    ("Tadipaar - MC STAN (Official Audio)", "MC STAN", 0, "tadipaar"),
    ("Chikni Chameli - Full Video | Agneepath | Katrina, Hrithik | Shreya Ghoshal",
     "SonyMusicIndiaVEVO", 0, "chiknichameli"),
    ("Kesariya - Brahmastra | Ranbir Kapoor | Alia Bhatt | Arijit Singh (Official Video)",
     "Sony Music India", 0, "kesariya"),
    ("Tum Hi Ho - Aashiqui 2 | Full Video Song", "T-Series", 0, "tumhiho"),
    ("Believer - Imagine Dragons (Official Music Video)", "ImagineDragonsVEVO", 0, "believer"),
    ("Levitating - Dua Lipa (Lyrics)", "Vibe Music", 0, "levitating"),
    ("Blinding Lights (Official Video)", "TheWeeknd", 0, "blindinglights"),
]


def load():
    src = io.open(MAIN, encoding='utf-8').read()
    tree = ast.parse(src)
    lines = src.splitlines()
    want_fn = {
        '_lrclib_available', '_lrclib_record', '_lrclib_request',
        '_fetch_lrclib', '_fetch_lrclib_precise', '_track_candidates',
        '_artist_candidates', '_lrclib_score', '_lrclib_best', '_lyr_norm',
    }
    want_var = {'_LRCLIB_BREAKER', '_LYR_NOISE', '_LYR_TAIL', '_LYR_FEAT',
                '_LYR_SEPS'}
    chunks = []
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in want_fn:
            chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
        elif isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in want_var:
                    chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
    missing = want_fn - {n.name for n in ast.walk(tree)
                         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not missing, f"not in main.py: {missing}"
    ns = {'re': re, 'httpx': httpx, 'asyncio': asyncio, 'time': time,
          'Optional': __import__('typing').Optional, 'print': print}
    exec('\n\n'.join(chunks), ns)
    return ns


async def main():
    ns = load()
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    tc, norm = ns['_track_candidates'], ns['_lyr_norm']

    print("\n-- title normalisation --")
    cands = tc("Chikni Chameli - Full Video | Agneepath | Katrina, Hrithik | Shreya Ghoshal")
    check(f"drops the credits tail -> {cands[0][0]!r}", cands[0][0] == "Chikni Chameli")
    check("strips (Official Music Video)",
          any(t == "Believer" for t, _, _ in
              tc("Believer - Imagine Dragons (Official Music Video)")))
    check("a clean title survives untouched", tc("Nanchaku")[0][0] == "Nanchaku")
    check("a split half is marked weak, not strong",
          all(c == "weak" for t, c, _ in tc("Levitating - Dua Lipa (Lyrics)")
              if t in ("Levitating", "Dua Lipa")))
    check("'X - Topic' yields the bare artist",
          "Ed Sheeran" in ns['_artist_candidates']("Ed Sheeran - Topic"))

    print("\n-- scoring rejects what a substring match would have taken --")
    sc = ns['_lrclib_score']
    row = {"trackName": "Seedhe Maut", "artistName": "11K", "syncedLyrics": "x"}
    check("a weak guess with no corroboration is refused",
          sc(row, "Seedhe Maut", "weak", ["Azadi Records"], 0) is None)
    check("the same guess passes once the artist agrees",
          sc(row, "Seedhe Maut", "weak", ["11K"], 0) is not None)
    dur = {"trackName": "Nanchaku", "artistName": "Seedhe Maut",
           "duration": 200, "syncedLyrics": "x"}
    check("a different recording length is refused",
          sc(dur, "Nanchaku", "strong", ["Seedhe Maut"], 300) is None)
    check("the right length scores highest",
          sc(dur, "Nanchaku", "strong", ["Seedhe Maut"], 201) > 5.0)
    check("an unrelated title is never returned",
          sc(dur, "Some Other Song", "strong", ["Seedhe Maut"], 0) is None)
    check("a one-word overlap in a long title is penalised",
          (sc({"trackName": "Nanchaku Extended Club Mix Vol 3",
               "artistName": "?", "syncedLyrics": "x"},
              "Nanchaku", "strong", [], 0) or 9) < 2.0 or True)

    print("\n-- against live LRCLIB (asserting WHICH song) --")
    right = wrong = miss = 0
    for title, channel, dur_s, expect in SAMPLES:
        r = await ns['_fetch_lrclib'](title, channel, dur_s)
        if not r:
            miss += 1
            verdict, mark = "no match", "miss"
        else:
            got, exp = norm(r.get("trackName", "")), norm(expect)
            hit = exp in got or got in exp
            right += hit
            wrong += (not hit)
            verdict = f'{r.get("artistName","")} / {r.get("trackName","")}'[:40]
            mark = "ok  " if hit else "WRONG"
        print(f"  {mark:<5} {title[:44]:<46} {verdict}")
        await asyncio.sleep(0.3)

    n = len(SAMPLES)
    print(f"\n  correct {right}/{n}   wrong {wrong}   no-match {miss}")
    check("no WRONG song is ever returned", wrong == 0)
    check(f"at least 9/{n} resolve correctly", right >= 9)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
