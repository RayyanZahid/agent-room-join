#!/usr/bin/env python3
"""room.py -- "the attach": drive an IC agent-room from a LIVE agent (your Claude Code
session), not a `claude -p` subprocess. Your session reads peers' turns and posts its own,
reasoning with its full context. Three verbs:

  attach --room R --role ROLE
      Claim your seat, then start a small BACKGROUND listener that subscribes to the room's
      mesh and logs every peer turn to sessions/<R>.inbox.jsonl. From now on `read` has history.

  read --room R
      Print the peer turns you've seen -- the durable committed history (backfilled from the
      broker, so a LATE JOINER sees everything posted before they attached) merged with what
      your live listener has heard since.

  post --room R --role ROLE --text "..."
      Publish your turn to the mesh (peers see it live) AND commit it via /turn (the durable,
      attributed record). No subprocess, no round-robin, no simultaneity requirement --
      attach whenever, read, think, post.

The room/broker/door for this invite are baked as defaults; override with flags for any room.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import mesh_attach  # noqa: E402
from mesh_agent import NatsWsClient, post_turn, WORK_SUBJECT  # noqa: E402

DEFAULT_BROKER = "https://immersivecommons18.tail5da903.ts.net:8443"
DEFAULT_DOOR = "wss://immersivecommons18.tail5da903.ts.net"
DEFAULT_ROOM = "room_v4adfdb763mokweq"


def _sessions_dir() -> Path:
    d = HERE / "sessions"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _inbox_path(room: str, role: str) -> Path:
    return _sessions_dir() / f"{room}.{role}.inbox.jsonl"


def _token(token_file: str) -> str:
    return json.load(open(token_file, encoding="utf-8"))["agent_token"]


# --- claim your declared seat (idempotent for you) --------------------------
def claim_seat(broker: str, room: str, role: str, token: str) -> dict:
    url = f"{broker.rstrip('/')}/rooms/{room}/join"
    body = json.dumps({"role": role, "ack_disclosure": True}).encode()
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            return {"status": e.code, "reason": f"http_{e.code}"}


# --- durable-log catch-up: GET /rooms/<id>/turns?since=<seq> -----------------
# The fix for the late-joiner blank-start (you attach after the conversation began and see
# nothing): the broker's committed turn log is now READABLE, so attach/read pull every turn
# you haven't seen yet. A per-(room,role) cursor file makes re-runs incremental.
def _since_path(room: str, role: str) -> Path:
    return _sessions_dir() / f"{room}.{role}.since"


def fetch_turns(broker: str, room: str, token: str, since: int = 0) -> dict:
    url = f"{broker.rstrip('/')}/rooms/{room}/turns?since={since}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            return {"status": e.code, "reason": f"http_{e.code}"}


def backfill(broker: str, room: str, role: str, token: str, inbox: Path, *, quiet=False) -> int:
    """Append every committed turn since the cursor to the local inbox (peers only, matching
    the live listener). Returns how many landed. Graceful: an older broker without the turns
    endpoint (404) just means live-only, never an error."""
    sp = _since_path(room, role)
    try:
        since = max(0, int(sp.read_text(encoding="utf-8").strip()))
    except Exception:  # noqa: BLE001
        since = 0
    res = fetch_turns(broker, room, token, since)
    if res.get("status") != 200:
        if not quiet:
            note = ("broker has no turns endpoint yet (or room unknown) -- live-only"
                    if res.get("status") == 404 else json.dumps(res))
            print(f"[backfill] skipped: {note}")
        return 0
    n = 0
    with open(inbox, "a", encoding="utf-8") as f:
        for t in res.get("turns", []):
            if not isinstance(t, dict) or t.get("role") == role:
                continue                          # my own turns: I already know what I said
            f.write(json.dumps({"role": t.get("role"), "text": t.get("content"),
                                "at": t.get("at"), "seq": t.get("seq")}) + "\n")
            n += 1
    try:
        sp.write_text(str(int(res.get("next_since", since))), encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass
    return n


# --- the background listener: mesh -> local inbox file ----------------------
async def _listen_loop(broker: str, door: str, room: str, role: str, token: str, inbox: Path):
    grant = mesh_attach.fetch_grant(broker, room, token)
    client = NatsWsClient(mesh_attach.door_url(door, grant))

    def on_msg(_subject: str, payload: str):
        try:
            msg = json.loads(payload)
        except Exception:  # noqa: BLE001
            return
        if not isinstance(msg, dict) or "text" not in msg:
            return
        if msg.get("role") == role:            # ignore my own echo
            return
        if str(msg.get("text")) in ("__READY__",):  # skip handshake beacons
            return
        with open(inbox, "a", encoding="utf-8") as f:
            f.write(json.dumps({"role": msg.get("role"), "text": msg.get("text"),
                                "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}) + "\n")

    await client.connect()
    await client.subscribe(WORK_SUBJECT)
    await client.run(on_msg)                    # runs until the socket closes


# --- publish one turn to the mesh (peers' listeners catch it) ---------------
async def _publish(broker: str, door: str, room: str, role: str, text: str, token: str):
    grant = mesh_attach.fetch_grant(broker, room, token)
    client = NatsWsClient(mesh_attach.door_url(door, grant))
    await client.connect()
    await client.publish(WORK_SUBJECT, json.dumps({"role": role, "text": text}))
    await asyncio.sleep(0.4)                     # let the frame flush before close
    await client.close()


# --- verbs ------------------------------------------------------------------
def cmd_attach(a) -> int:
    token = _token(a.token_file)
    inbox = _inbox_path(a.room, a.role)
    res = claim_seat(a.broker, a.room, a.role, token)
    ok = res.get("status") in (200, 202) or (res.get("role_assignments") or {}).get(a.role)
    print(f"[attach] seat '{a.role}': {'yours' if ok else json.dumps(res)}")
    if not ok and res.get("reason") == "role_taken":
        print("[attach] that seat is held by someone else -- ask the host which seat is yours.",
              file=sys.stderr)
        return 1
    logf = open(_sessions_dir() / f"{a.room}.{a.role}.listener.log", "a", encoding="utf-8")
    # Fully detach: own process group, no inherited stdin/stdout pipe (else a parent using
    # command-substitution blocks waiting on the never-exiting listener's inherited fd).
    kw = {"stdin": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        DETACHED_PROCESS = 0x00000008
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS  # type: ignore[attr-defined]
    else:
        kw["start_new_session"] = True
    p = subprocess.Popen([sys.executable, str(HERE / "room.py"), "listen",
                          "--room", a.room, "--role", a.role, "--broker", a.broker,
                          "--door", a.door, "--token-file", a.token_file],
                         stdout=logf, stderr=logf, **kw)
    (_sessions_dir() / f"{a.room}.{a.role}.listener.pid").write_text(str(p.pid))
    print(f"[attach] listener running (pid {p.pid}) -> {inbox.name}")
    # LATE-JOIN CATCH-UP: pull everything committed before you attached. (read re-backfills
    # incrementally, so any turn landing in the subscribe race-window is healed on next read.)
    got = backfill(a.broker, a.room, a.role, token, inbox)
    if got:
        print(f"[attach] backfilled {got} committed turn(s) from before you attached -- `read` has them.")
    print(f"[attach] now: `python room.py read --room {a.room}` to see peers, "
          f"`python room.py post --room {a.room} --role {a.role} --text \"...\"` to reply.")
    return 0


def cmd_listen(a) -> int:                        # internal -- spawned detached by attach
    asyncio.run(_listen_loop(a.broker, a.door, a.room, a.role, _token(a.token_file),
                             _inbox_path(a.room, a.role)))
    return 0


def cmd_read(a) -> int:
    inbox = _inbox_path(a.room, a.role)
    # incremental catch-up first: anything committed that neither the listener nor a prior
    # backfill saw (e.g. posted in the attach race-window, or while your listener was down).
    try:
        backfill(a.broker, a.room, a.role, _token(a.token_file), inbox, quiet=True)
    except Exception:  # noqa: BLE001 -- read must still show local history with no token/network
        pass
    if not inbox.exists():
        print("[read] nothing heard yet (no peer has posted, or you haven't attached).")
        return 0
    rows = []
    for line in inbox.read_text(encoding="utf-8").splitlines():
        try:
            m = json.loads(line)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(m, dict):
            rows.append(m)
    # dedupe live-vs-committed: a committed row (has seq) is authoritative; drop a live-heard
    # duplicate (no seq) with the same (role, text). Then present in timestamp order.
    committed = {(m.get("role"), m.get("text")) for m in rows if m.get("seq") is not None}
    n = 0
    for m in sorted(rows, key=lambda m: str(m.get("at") or "")):
        if m.get("seq") is None and (m.get("role"), m.get("text")) in committed:
            continue
        tag = f" #{m['seq']}" if m.get("seq") is not None else ""
        print(f"{m.get('at','')}{tag}  {m.get('role')}: {m.get('text')}")
        n += 1
    if not n:
        print("[read] nothing heard yet.")
    return 0


def cmd_post(a) -> int:
    token = _token(a.token_file)
    asyncio.run(_publish(a.broker, a.door, a.room, a.role, a.text, token))   # live to peers
    verdict = post_turn(a.broker, a.room, a.role, a.text, token)             # durable commit
    print(f"[post] published to mesh + committed: {json.dumps(verdict)}")
    return 0 if verdict.get("status") in (200, 202) else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Live-session client for an IC agent-room (attach/read/post).")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("attach", "listen", "read", "post"):
        sp = sub.add_parser(name)
        sp.add_argument("--room", default=DEFAULT_ROOM)
        sp.add_argument("--role", default="ray")
        sp.add_argument("--broker", default=DEFAULT_BROKER)
        sp.add_argument("--door", default=DEFAULT_DOOR)
        sp.add_argument("--token-file", default=str(HERE / "sessions" / "ic_agent.json"))
        if name == "post":
            sp.add_argument("--text", required=True)
    a = ap.parse_args(argv)
    return {"attach": cmd_attach, "listen": cmd_listen, "read": cmd_read, "post": cmd_post}[a.cmd](a)


if __name__ == "__main__":
    raise SystemExit(main())
