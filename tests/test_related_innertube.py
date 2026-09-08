"""Drive main.py's real innertube related-fetch against live YouTube.

Exists because the yt-dlp path it replaces returns 403 from Render's IP
while working fine from a residential one — so the only test that means
anything is one that actually calls YouTube.
"""
import ast
import asyncio
import io
import os
import re
import sys

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')
import httpx

MAIN = 'd:/projects/audiosync-yt-dlp/server/main.py'
SEEDS = ["kJQP7kiw5Fk", "dQw4w9WgXcQ", "Fbv6-50S1lc"]


def load():
    src = io.open(MAIN, encoding='utf-8').read()
    tree = ast.parse(src)
    lines = src.splitlines()
    want_fn = {'_runs_text', '_parse_length', '_walk_queue',
               '_fetch_related_innertube', '_ytm_client'}
    want_var = {'_YTM_NEXT_QUEUE_FIELDS', '_YTM_HEADERS', '_YTM_WEB_VERSION',
                '_ytm_http'}
    chunks = []
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in want_fn:
            chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
        elif isinstance(n, (ast.Assign, ast.AnnAssign)):
            t = n.targets[0] if isinstance(n, ast.Assign) else n.target
            if isinstance(t, ast.Name) and t.id in want_var:
                chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
    missing = want_fn - {n.name for n in ast.walk(tree)
                         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not missing, f"not in main.py: {missing}"
    ns = {'httpx': httpx, 'os': os, 're': re, 'print': print,
          'Optional': __import__('typing').Optional}
    exec('\n\n'.join(chunks), ns)
    return ns


async def main():
    ns = load()
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    print("\n-- parsing --")
    pl = ns['_parse_length']
    check("'3:31' -> 211", pl("3:31") == 211)
    check("'1:02:03' -> 3723", pl("1:02:03") == 3723)
    check("empty -> None", pl("") is None)
    check("garbage -> None rather than a wrong number", pl("later") is None)
    check("runs join in order",
          ns['_runs_text']({"runs": [{"text": "Con "}, {"text": "Calma"}]}) == "Con Calma")
    check("a missing node is empty, not a crash", ns['_runs_text'](None) == "")
    check("the walker finds a renderer at any depth",
          len(list(ns['_walk_queue'](
              {"a": [{"b": {"playlistPanelVideoRenderer": {"videoId": "x"}}}]}))) == 1)

    print("\n-- against live YouTube --")
    total = 0
    for vid in SEEDS:
        rows = await ns['_fetch_related_innertube'](vid, 25)
        total += len(rows)
        ids = {r["videoId"] for r in rows}
        titled = sum(1 for r in rows if r["title"] and r["title"] != "Unknown")
        named = sum(1 for r in rows if r["uploader"] and r["uploader"] != "Unknown")
        timed = sum(1 for r in rows if r["duration"])
        print(f"  {vid}: {len(rows)} rows | titles {titled} | artists {named} | durations {timed}")
        if rows:
            print(f"      e.g. {rows[0]['title'][:40]} — {rows[0]['uploader'][:22]}")
        check(f"{vid}: got suggestions", len(rows) >= 10)
        check(f"{vid}: the seed itself is excluded", vid not in ids)
        check(f"{vid}: no duplicate ids", len(ids) == len(rows))
        check(f"{vid}: every row has a title", titled == len(rows))
        await asyncio.sleep(0.4)

    check("suggestions across all seeds", total >= 40)
    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
