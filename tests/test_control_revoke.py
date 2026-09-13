"""Connected-devices list, kick, and the email opt-out, against main.py's
real handlers.

The properties that matter: only the PHONE of a session can list or kick;
a kicked browser is told, not just unmapped; and an opted-out owner is
indistinguishable from an address that has no phone behind it.
"""
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
_EMAILS = {}


class FakeWS:
    def __init__(self, name, ua="Mozilla/5.0 (Windows NT 10.0) Chrome/120 Safari/537"):
        self.name = name
        self.headers = {"user-agent": ua}
        self.client_state = 'CONNECTED'

    def __repr__(self):
        return f"<{self.name}>"


async def fake_ws_send(ws, message):
    sent.append((ws, message))


def load():
    src = io.open(MAIN, encoding='utf-8').read()
    tree = ast.parse(src)
    lines = src.splitlines()
    want_fn = {
        '_describe_requester', '_drop_pending', '_prompt_allowed',
        '_expire_unacked', '_resolve_pending', 'handle_control_approval_ack',
        'handle_control_approve', 'handle_control_deny',
        'handle_control_request_email', '_email_target_allowed',
        '_cancel_decoy', '_decoy_wait',
        '_controllers_payload', '_phone_session_or_none',
        'handle_control_list_controllers', 'handle_control_kick',
        'handle_control_email_optout',
    }
    want_var = {'_pending', '_pending_by_browser', '_ack_tasks', '_prompt_hits',
                '_email_target_hits', '_email_deny_until', '_decoys',
                '_deny_until'}
    want_const = {'ACK_DEADLINE', 'PROMPT_TTL', 'PROMPT_CAP', '_UA_FAMILIES',
                  '_UA_PLATFORMS', 'EMAIL_TARGET_HOURLY_CAP',
                  'EMAIL_DENY_COOLDOWN', 'DENY_COOLDOWN'}
    def seg(n):
        # lineno points at the `class`/`def` keyword, which silently drops any
        # decorator above it -- turning @dataclass into a plain class whose
        # __init__ takes no kwargs. The original harness documented this trap;
        # this loader walked into it anyway.
        start = n.lineno
        for d in getattr(n, 'decorator_list', []):
            start = min(start, d.lineno)
        return '\n'.join(lines[start - 1:n.end_lineno])

    chunks = []
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in want_fn:
            chunks.append(seg(n))
        elif isinstance(n, ast.ClassDef) and n.name == '_Pending':
            chunks.append(seg(n))
        elif isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id in want_const:
                    chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
        elif isinstance(n, ast.AnnAssign):
            if isinstance(n.target, ast.Name) and n.target.id in want_var:
                chunks.append('\n'.join(lines[n.lineno - 1:n.end_lineno]))
    missing = want_fn - {n.name for n in ast.walk(tree)
                         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not missing, f"not in main.py: {missing}"
    ns = {
        'asyncio': asyncio, 'time': time, 'secrets': __import__('secrets'),
        'dataclasses': __import__('dataclasses'),
        'ws_send': fake_ws_send, 'control_manager': None,
        'uid_for_ws': lambda cid: _UIDS.get(cid),
        'email_for_ws': lambda cid: _EMAILS.get(cid),
        'WebSocketState': types.SimpleNamespace(CONNECTED='CONNECTED'),
        'WebSocket': object,
    }
    exec('\n\n'.join(chunks), ns)
    return ns


def to(ws):
    return [m for w, m in sent if w is ws]


def last_to(ws):
    got = to(ws)
    return got[-1] if got else None


async def main():
    ns = load()
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    def fresh(email="owner@example.com"):
        cm = control_session.ControlSessionManager()
        ns['control_manager'] = cm
        for d in ('_pending', '_pending_by_browser', '_ack_tasks', '_prompt_hits',
                  '_email_target_hits', '_email_deny_until', '_decoys',
                  '_deny_until'):
            ns[d].clear()
        sent.clear(); _UIDS.clear(); _EMAILS.clear()
        phone = FakeWS("phone")
        sess, _, _ = cm.create("phoneC", phone, caps={"approval"})
        _UIDS["phoneC"] = "uid-owner"
        if email:
            _EMAILS["phoneC"] = email
        return cm, sess, phone

    lst = ns['handle_control_list_controllers']
    kick = ns['handle_control_kick']
    optout = ns['handle_control_email_optout']
    ask = ns['handle_control_request_email']
    ack = ns['handle_control_approval_ack']
    approve = ns['handle_control_approve']

    print("\n-- the list shows who is connected, and how they got in --")
    cm, sess, phone = fresh()
    b1, b2 = FakeWS("b1"), FakeWS("b2")
    cm.attach_controller(sess.id, "bc1", b1, meta={
        "requester": "Chrome on Windows", "via": "code",
        "connected_at": time.time() - 120})
    cm.attach_controller(sess.id, "bc2", b2, meta={
        "requester": "Firefox on Linux  \u00b7  by email", "via": "email",
        "connected_at": time.time()})
    await lst("phoneC", phone)
    m = last_to(phone)
    check("list arrives", m and m["type"] == "control_controllers")
    check("two rows", len(m["controllers"]) == 2)
    rows = {r["clientId"]: r for r in m["controllers"]}
    check("via recorded", rows["bc1"]["via"] == "code" and rows["bc2"]["via"] == "email")
    check("age is seconds, not a timestamp", 100 <= rows["bc1"]["connectedSec"] <= 140)

    print("\n-- only the phone can list or kick --")
    n = len(sent)
    await lst("bc1", b1)
    check("a controller listing gets silence", len(sent) == n)
    await kick("bc1", b1, {"clientId": "bc2"})
    check("a controller kicking gets silence and changes nothing",
          len(sent) == n and "bc2" in sess.controllers)

    print("\n-- kick tells the browser, cleans the maps, refreshes the phone --")
    await kick("phoneC", phone, {"clientId": "bc2"})
    check("kicked browser told why",
          last_to(b2) == {"type": "control_closed", "reason": "kicked"})
    check("kicked id unmapped", cm.for_client("bc2") is None)
    check("meta gone with it", "bc2" not in sess.controller_meta)
    phone_msgs = to(phone)
    check("phone got controller_left with the new count",
          any(m.get("type") == "control_controller_left" and m.get("controllers") == 1
              for m in phone_msgs))
    check("phone got a fresh one-row list",
          phone_msgs[-1]["type"] == "control_controllers"
          and len(phone_msgs[-1]["controllers"]) == 1)
    n = len(sent)
    await kick("phoneC", phone, {"clientId": "bc2"})
    check("kicking a ghost still answers with a truthful list",
          last_to(phone)["type"] == "control_controllers" and len(sent) == n + 1)
    check("survivor still mapped", cm.for_client("bc1") is sess)

    print("\n-- approve stores meta, so approved browsers appear in the list --")
    cm, sess, phone = fresh()
    b = FakeWS("bmail")
    await ask("bcM", b, {"email": "owner@example.com"})
    rid = last_to(phone)["requestId"]
    await ack("phoneC", {"requestId": rid})
    await approve("phoneC", {"requestId": rid})
    check("browser attached", last_to(b)["type"] == "control_joined")
    await lst("phoneC", phone)
    row = last_to(phone)["controllers"][0]
    check("via=email recorded through the approve path", row["via"] == "email")
    check("requester carried through", "by email" in row["requester"])

    print("\n-- email opt-out: off means indistinguishable from absent --")
    cm, sess, phone = fresh()
    await optout("phoneC", phone, {"optOut": True})
    check("ack carries the state", last_to(phone) == {
        "type": "control_email_optout_ok", "optOut": True})
    n_phone = len(to(phone))
    b = FakeWS("bOpt")
    await ask("bcOpt", b, {"email": "owner@example.com"})
    await asyncio.sleep(0); await asyncio.sleep(0)
    check("the phone is never rung", len(to(phone)) == n_phone)
    m = last_to(b)
    check("the asker sees the standard decoy (digits + waiting)",
          m and m["type"] == "control_requested" and len(m["pairDigits"]) == 4)
    check("a decoy task is holding the browser", len(ns['_decoys']) == 1)
    for t in ns['_decoys'].values():
        t.cancel()
    ns['_decoys'].clear()

    await optout("phoneC", phone, {"optOut": False})
    b2 = FakeWS("bOpt2")
    await ask("bcOpt2", b2, {"email": "owner@example.com"})
    check("turning it back on lets the prompt through",
          (last_to(phone) or {}).get("type") == "control_approval_request")

    print("\n-- a controller cannot set the opt-out either --")
    cm, sess, phone = fresh()
    cm.attach_controller(sess.id, "bcX", FakeWS("bx"), meta={})
    await optout("bcX", FakeWS("bx"), {"optOut": True})
    check("flag untouched", sess.email_optout is False)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
