"""Exercise the connect-by-email flow against main.py's real handler.

The property under test is mostly a NEGATIVE one: every state that does not
make a phone buzz has to answer identically, or the endpoint becomes a
membership check for any address in the world. So most cases here assert that
two different situations produce the same bytes -- not that each produces the
right error, which is the thing the flow must never do.
"""
import asyncio, ast, io, sys, time, types

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
        'handle_control_approve', 'handle_control_deny', 'handle_control_cancel',
        'handle_control_request_email', '_email_target_allowed',
        '_cancel_decoy', '_decoy_wait', 'handle_control_disconnect',
        'handle_control_request', '_request_allowed',
    }
    want_const = {
        'ACK_DEADLINE', 'PROMPT_TTL', 'PROMPT_CAP', '_UA_FAMILIES',
        '_UA_PLATFORMS', 'REQUEST_CAP', 'DENY_COOLDOWN',
        'EMAIL_TARGET_HOURLY_CAP', 'EMAIL_DENY_COOLDOWN',
    }
    want_var = {
        '_pending', '_pending_by_browser', '_ack_tasks', '_prompt_hits',
        '_request_hits', '_deny_until', '_email_target_hits',
        '_email_deny_until', '_decoys',
    }

    def seg(node):
        start = node.lineno
        for d in getattr(node, 'decorator_list', []):
            start = min(start, d.lineno)
        return chr(10).join(lines[start - 1:node.end_lineno])

    chunks = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in want_fn:
            chunks.append(seg(node))
        elif isinstance(node, ast.ClassDef) and node.name == '_Pending':
            chunks.append(seg(node))
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id in want_const:
                    chunks.append(seg(node))
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id in want_var:
                chunks.append(seg(node))

    missing = want_fn - {n.name for n in ast.walk(tree)
                         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not missing, f"not found in main.py: {missing}"

    ns = {
        'asyncio': asyncio, 'time': time, 'secrets': __import__('secrets'),
        'dataclasses': __import__('dataclasses'),
        'ws_send': fake_ws_send, 'control_manager': None,
        'uid_for_ws': lambda cid: _UIDS.get(cid),
        'email_for_ws': lambda cid: _EMAILS.get(cid),
        'WebSocketState': types.SimpleNamespace(CONNECTED='CONNECTED'),
        'WebSocket': object,
        # Only the control half of teardown is under test here.
        'handle_disconnect': None, 'social': None,
    }
    exec("\n\n".join(chunks), ns)
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

    ask = ns['handle_control_request_email']
    deny = ns['handle_control_deny']
    approve = ns['handle_control_approve']
    ack = ns['handle_control_approval_ack']

    def fresh(email="owner@example.com", caps={"approval"}, uid="uid-owner"):
        cm = control_session.ControlSessionManager()
        ns['control_manager'] = cm
        for d in ('_pending', '_pending_by_browser', '_ack_tasks', '_prompt_hits',
                  '_email_target_hits', '_email_deny_until', '_decoys', '_deny_until'):
            ns[d].clear()
        sent.clear(); _UIDS.clear(); _EMAILS.clear()
        phone = FakeWS("phone")
        sess, _, _ = cm.create("phoneC", phone, caps=set(caps))
        _UIDS["phoneC"] = uid
        if email:
            _EMAILS["phoneC"] = email
        return cm, sess, phone

    async def settle():
        # Let create_task'd decoys reach their first await.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    def kill_decoys():
        for t in list(ns['_decoys'].values()):
            t.cancel()
        ns['_decoys'].clear()

    def is_decoy_shape(msgs):
        """What the browser sees for every non-buzzing state."""
        return (len(msgs) == 1 and msgs[0].get("type") == "control_requested"
                and isinstance(msgs[0].get("pairDigits"), str)
                and len(msgs[0]["pairDigits"]) == 4
                and msgs[0]["pairDigits"].isdigit())

    print("\n-- the address matches a live phone --")
    cm, sess, phone = fresh()
    b = FakeWS("b1")
    await ask("bc1", b, {"email": "owner@example.com"})
    req = last_to(phone)
    check("phone is asked", req and req["type"] == "control_approval_request")
    check("prompt says the request came by email",
          req and "by email" in req["requester"])
    check("requester still a closed-set string",
          req and req["requester"].startswith("Chrome on Windows"))
    check("digits on the prompt match the browser's",
          req["pairDigits"] == last_to(b)["pairDigits"])
    check("browser told it is waiting", last_to(b)["type"] == "control_requested")
    check("no decoy for a real hit", not ns['_decoys'])

    print("\n-- approve attaches the browser --")
    rid = req["requestId"]
    await ack("phoneC", {"requestId": rid})
    await approve("phoneC", {"requestId": rid})
    check("browser attached", last_to(b)["type"] == "control_joined")

    print("\n-- matching is forgiving about how it was typed --")
    for typed in ("  Owner@Example.com  ", "OWNER@EXAMPLE.COM"):
        cm, sess, phone = fresh()
        await ask("bcN", FakeWS("bN"), {"email": typed})
        check(f"{typed!r} reaches the phone",
              (last_to(phone) or {}).get("type") == "control_approval_request")

    print("\n-- every non-buzzing state looks the same --")
    shapes = {}

    cm, sess, phone = fresh()
    b = FakeWS("miss")
    await ask("bcMiss", b, {"email": "nobody@example.com"})
    await settle()
    shapes['unknown address'] = to(b)
    check("unknown address does NOT reach any phone", not to(phone))
    check("unknown address starts a decoy", len(ns['_decoys']) == 1)
    kill_decoys()

    # A phone on an older build cannot show a prompt. Joining it anyway would be
    # a silent takeover of any phone whose owner's address someone knew.
    cm, sess, phone = fresh(caps=set())
    b = FakeWS("nocap")
    await ask("bcNoCap", b, {"email": "owner@example.com"})
    await settle()
    shapes['phone cannot approve'] = to(b)
    check("a phone with no approval cap is NOT joined", not to(phone))
    kill_decoys()

    cm, sess, phone = fresh(email=None)
    b = FakeWS("noemail")
    await ask("bcNoEmail", b, {"email": "owner@example.com"})
    await settle()
    shapes['phone signed out'] = to(b)
    kill_decoys()

    # refused a moment ago
    cm, sess, phone = fresh()
    b0 = FakeWS("d0")
    await ask("bcD0", b0, {"email": "owner@example.com"})
    rid = last_to(phone)["requestId"]
    await ack("phoneC", {"requestId": rid})
    await deny("phoneC", {"requestId": rid})
    check("deny answers the browser honestly", last_to(b0)["code"] == "denied")
    check("deny arms the cooldown on the TARGET",
          ns['_email_deny_until'].get("uid-owner", 0) > time.time())
    n_phone = len(to(phone))
    b1 = FakeWS("d1")
    await ask("bcD1", b1, {"email": "owner@example.com"})
    await settle()
    shapes['refused a moment ago'] = to(b1)
    check("a refused address raises NO second prompt", len(to(phone)) == n_phone)
    kill_decoys()

    # over the hourly cap
    cm, sess, phone = fresh()
    for i in range(ns['EMAIL_TARGET_HOURLY_CAP']):
        ns['_email_target_allowed']("uid-owner")
    b = FakeWS("cap")
    await ask("bcCap", b, {"email": "owner@example.com"})
    await settle()
    shapes['over the hourly cap'] = to(b)
    check("the cap stops the phone being rung", not to(phone))
    kill_decoys()

    # already being asked
    cm, sess, phone = fresh()
    await ask("bcBusy1", FakeWS("busy1"), {"email": "owner@example.com"})
    n_phone = len(to(phone))
    b = FakeWS("busy2")
    await ask("bcBusy2", b, {"email": "owner@example.com"})
    await settle()
    shapes['already being asked'] = to(b)
    check("a second asker raises no second prompt", len(to(phone)) == n_phone)
    kill_decoys()

    for label, msgs in shapes.items():
        check(f"{label!r} -> the same waiting screen", is_decoy_shape(msgs))
    keys = list(shapes)
    check("all six are byte-identical apart from the random digits",
          len({tuple(sorted((k, v) for k, v in m[0].items() if k != 'pairDigits'))
               for m in shapes.values() if m}) == 1
          and len(shapes) == 6)

    print("\n-- the decoy runs to the same ending a real timeout has --")
    cm, sess, phone = fresh()
    ns['PROMPT_TTL'] = 0.05
    b = FakeWS("decoy")
    await ask("bcDecoy", b, {"email": "nobody@example.com"})
    for _ in range(400):
        await asyncio.sleep(0.01)
        if (last_to(b) or {}).get("code") == "expired":
            break
    got = [m["type"] for m in to(b)]
    check("decoy: requested -> pending -> expired",
          got == ["control_requested", "control_pending", "control_error"])
    check("decoy cleans itself up", not ns['_decoys'])
    ns['PROMPT_TTL'] = 60.0

    print("\n-- a browser that leaves takes its decoy with it --")
    cm, sess, phone = fresh()
    b = FakeWS("gone")
    await ask("bcGone", b, {"email": "nobody@example.com"})
    await settle()
    check("decoy armed", len(ns['_decoys']) == 1)
    await ns['handle_control_disconnect']("bcGone")
    check("decoy cancelled on disconnect", not ns['_decoys'])

    print("\n-- malformed input is refused outright, not held for a minute --")
    for bad in ("", "   ", "nope", "a@b", "@example.com", "x@", "a@@b.com",
                "x" * 250 + "@example.com"):
        cm, sess, phone = fresh()
        b = FakeWS("bad")
        await ask("bcBad", b, {"email": bad})
        m = last_to(b)
        check(f"{bad[:18]!r} -> bad_email",
              m and m.get("code") == "bad_email" and not ns['_decoys'])
    cm, sess, phone = fresh()
    b = FakeWS("badtype")
    await ask("bcBadType", b, {"email": {"not": "a string"}})
    check("a non-string address does not crash the handler",
          (last_to(b) or {}).get("code") == "bad_email")

    print("\n-- one request in flight per browser --")
    cm, sess, phone = fresh()
    b = FakeWS("dup")
    await ask("bcDup", b, {"email": "owner@example.com"})
    await ask("bcDup", b, {"email": "owner@example.com"})
    check("second concurrent ask -> busy_here",
          (last_to(b) or {}).get("code") == "busy_here")
    kill_decoys()

    print("\n-- the owner's own signed-in flow is untouched by all of it --")
    cm, sess, phone = fresh()
    ns['_email_deny_until']["uid-owner"] = time.time() + 9999
    ns['_email_target_hits']["uid-owner"] = [time.time()] * 99
    _UIDS["ownerBrowser"] = "uid-owner"
    b = FakeWS("owner")
    await ns['handle_control_request']("ownerBrowser", b, {})
    check("email limits do not block the account flow",
          (last_to(phone) or {}).get("type") == "control_approval_request")
    check("and that prompt is NOT labelled by email",
          "by email" not in last_to(phone)["requester"])
    kill_decoys()

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
