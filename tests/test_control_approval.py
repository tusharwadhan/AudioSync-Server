"""Exercise the approval state machine against the real handlers.

Not a unit test of my own reimplementation -- it imports main.py's actual
functions and drives them, so a divergence between plan and code shows up here.
"""
import asyncio, sys, types

sys.path.insert(0, 'd:/projects/audiosync-yt-dlp/server')

# main.py pulls in a lot at import time; stub what we cannot have.
import control_session

sent = []          # (ws, message)


class FakeWS:
    def __init__(self, name, ua="Mozilla/5.0 (Windows NT 10.0) Chrome/120 Safari/537"):
        self.name = name
        self.headers = {"user-agent": ua}
        self.client_state = None

    def __repr__(self):
        return f"<{self.name}>"


async def fake_ws_send(ws, message):
    sent.append((ws, message))


def load_main_pieces():
    """Pull the approval functions out of main.py without importing the app."""
    import io, ast
    src = io.open('d:/projects/audiosync-yt-dlp/server/main.py', encoding='utf-8').read()
    tree = ast.parse(src)
    want_fn = {
        '_describe_requester', '_drop_pending', 'drop_pendings_for_session',
        '_prompt_allowed', '_expire_unacked', 'handle_control_join',
        'handle_control_approval_ack', '_resolve_pending',
        'handle_control_approve', 'handle_control_deny', 'handle_control_cancel',
        'sweep_pendings',
    }
    want_cls = {'_Pending'}
    lines = src.splitlines()

    def seg(node):
        # get_source_segment starts at the `class`/`def` line, dropping any
        # decorator -- which silently turns @dataclass into a plain class.
        start = node.lineno
        for d in getattr(node, 'decorator_list', []):
            start = min(start, d.lineno)
        return chr(10).join(lines[start - 1:node.end_lineno])

    chunks = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in want_fn:
            chunks.append(seg(node))
        elif isinstance(node, ast.ClassDef) and node.name in want_cls:
            chunks.append(seg(node))
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in (
                        'ACK_DEADLINE', 'PROMPT_TTL', 'PROMPT_CAP',
                        '_UA_FAMILIES', '_UA_PLATFORMS'):
                    chunks.append(seg(node))
        elif isinstance(node, ast.AnnAssign):
            t = node.target
            if isinstance(t, ast.Name) and t.id in (
                    '_pending', '_pending_by_browser', '_ack_tasks', '_prompt_hits'):
                chunks.append(seg(node))

    ns = {
        'asyncio': asyncio, 'time': __import__('time'),
        'secrets': __import__('secrets'), 'dataclasses': __import__('dataclasses'),
        'ws_send': fake_ws_send, 'control_manager': None,
        'WebSocketState': types.SimpleNamespace(CONNECTED='CONNECTED'),
        'WebSocket': object,
    }
    exec("\n\n".join(chunks), ns)
    return ns


def last_to(ws):
    for w, m in reversed(sent):
        if w is ws:
            return m
    return None


async def main():
    ns = load_main_pieces()
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")

    def fresh(caps={"approval"}):
        cm = control_session.ControlSessionManager()
        ns['control_manager'] = cm
        ns['_pending'].clear(); ns['_pending_by_browser'].clear()
        ns['_ack_tasks'].clear(); ns['_prompt_hits'].clear()
        sent.clear()
        phone = FakeWS("phone")
        sess, secret, _ = cm.create("phoneC", phone, caps=set(caps))
        return cm, sess, secret, phone

    join = ns['handle_control_join']
    ack = ns['handle_control_approval_ack']
    approve = ns['handle_control_approve']
    deny = ns['handle_control_deny']
    cancel = ns['handle_control_cancel']

    # ---- happy path
    cm, sess, _, phone = fresh()
    code = cm.mint_code(sess.id)
    b = FakeWS("browser")
    await join("browserC", b, {"code": code})
    req = last_to(phone)
    check("prompt sent to phone", req and req["type"] == "control_approval_request")
    check("requester is a closed-set string", req["requester"] == "Chrome on Windows")
    check("code NOT spent before a decision", cm.ticket_valid(code))
    rid = req["requestId"]
    await ack("phoneC", {"requestId": rid})
    check("browser told pending", last_to(b)["type"] == "control_pending")
    await approve("phoneC", {"requestId": rid})
    check("browser attached", last_to(b)["type"] == "control_joined")
    check("code spent on approve", not cm.ticket_valid(code))

    # ---- deny is terminal
    cm, sess, _, phone = fresh()
    code = cm.mint_code(sess.id)
    b = FakeWS("b2")
    await join("bc2", b, {"code": code})
    rid = last_to(phone)["requestId"]
    await ack("phoneC", {"requestId": rid})
    await deny("phoneC", {"requestId": rid})
    check("deny -> denied", last_to(b)["code"] == "denied")
    check("deny spends the code", not cm.ticket_valid(code))
    sess2, reason = cm.peek(code)
    check("spent code peeks as denied", sess2 is None and reason == "denied")

    # ---- cancel is NOT terminal (the whole point of burn-at-decision)
    cm, sess, _, phone = fresh()
    code = cm.mint_code(sess.id)
    b = FakeWS("b3")
    await join("bc3", b, {"code": code})
    rid = last_to(phone)["requestId"]
    await ack("phoneC", {"requestId": rid})
    await cancel("phoneC", {"requestId": rid})
    check("cancel -> cancelled", last_to(b)["code"] == "cancelled")
    check("cancel does NOT spend the code", cm.ticket_valid(code))
    await join("bc3b", FakeWS("b3b"), {"code": code})
    check("same code works again after cancel",
          last_to(phone)["type"] == "control_approval_request")

    # ---- a phone cannot answer another phone's prompt
    cm, sess, _, phone = fresh()
    code = cm.mint_code(sess.id)
    b = FakeWS("b4")
    await join("bc4", b, {"code": code})
    rid = last_to(phone)["requestId"]
    await ack("phoneC", {"requestId": rid})
    n_before = len(sent)
    await approve("someOtherPhone", {"requestId": rid})
    check("foreign approve ignored", len(sent) == n_before)
    check("pending survives foreign approve", rid in ns['_pending'])

    # ---- offline phone: refused, code NOT spent
    cm, sess, _, phone = fresh()
    code = cm.mint_code(sess.id)
    sess.phone_ws = None
    b = FakeWS("b5")
    await join("bc5", b, {"code": code})
    check("offline -> phone_offline", last_to(b)["code"] == "phone_offline")
    check("offline does not spend the code", cm.ticket_valid(code))

    # ---- legacy phone keeps the old immediate attach
    cm, sess, _, phone = fresh(caps=set())
    code = cm.mint_code(sess.id)
    b = FakeWS("b6")
    await join("bc6", b, {"code": code})
    check("no caps -> attached immediately", last_to(b)["type"] == "control_joined")
    check("legacy path spends the code", not cm.ticket_valid(code))

    # ---- one prompt at a time
    cm, sess, _, phone = fresh()
    c1, c2 = cm.mint_code(sess.id), cm.mint_code(sess.id)
    await join("bcA", FakeWS("bA"), {"code": c1})
    bB = FakeWS("bB")
    await join("bcB", bB, {"code": c2})
    check("second prompt -> busy", last_to(bB)["code"] == "busy")

    # ---- prompt cap
    cm, sess, _, phone = fresh()
    codes = [cm.mint_code(sess.id) for _ in range(5)]
    hits = []
    for i, c in enumerate(codes):
        bx = FakeWS(f"bx{i}")
        await join(f"bcx{i}", bx, {"code": c})
        m = last_to(bx)
        if m and m.get("code") == "throttled":
            hits.append(i)
        else:
            # clear the pending so the next one isn't just "busy"
            for rid in list(ns['_pending']):
                ns['_drop_pending'](rid)
    check("cap eventually throttles", len(hits) > 0)

    # ---- unknown requestId never burns anyone else's code
    cm, sess, _, phone = fresh()
    code = cm.mint_code(sess.id)
    await approve("phoneC", {"requestId": "totally-made-up"})
    check("stale approve burns nothing", cm.ticket_valid(code))
    m = last_to(phone)
    check("stale approve cancels the phone's sheet",
          m and m["type"] == "control_approval_cancelled")

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


sys.exit(asyncio.run(main()))
