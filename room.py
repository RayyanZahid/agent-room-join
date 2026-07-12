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
DEFAULT_NATIVE_DOOR = "wss://immersivecommons18.tail5da903.ts.net/native"
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


# --- NATIVE mode (Cotal 0.11.3): minted creds, push delivery, replay, presence ---------------
# attach --native trades your IC token for a room-channel-scoped Cotal cred and runs a
# listener with SERVER-SIDE replay + push delivery: no ready-beacons, no polling, no
# missed-while-offline. Your live messages are attributed by the broker (you can only publish
# as yourself) and your listener heartbeats presence so `who` shows who's actually attached.
def _native_meta_path(room: str, role: str) -> Path:
    return _sessions_dir() / f"{room}.{role}.native.json"


def fetch_native_creds(broker: str, room: str, token: str, role: str = "") -> dict:
    url = f"{broker.rstrip('/')}/rooms/{room}/native_creds"
    body = json.dumps({"role": role} if role else {}).encode()
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            return {"status": e.code, "reason": f"http_{e.code}"}


def _save_native_meta(room: str, role: str, bundle: dict) -> Path:
    p = _native_meta_path(room, role)
    p.write_text(json.dumps(bundle), encoding="utf-8")
    try:
        os.chmod(p, 0o600)                    # the bundle holds your private cred seed
    except OSError:
        pass
    # ALSO emit a standard .creds file for nats-native tools (the cotal CLI, nats CLI, any
    # NATS client) -- with LF endings FORCED: a Windows text-mode write turns them into CRLF,
    # which nats.js's creds parser rejects ("unable to parse credentials"; cost us live time).
    cp = _sessions_dir() / f"{room}.{role}.creds"
    with open(cp, "w", encoding="utf-8", newline="\n") as f:
        f.write(bundle["creds"])
    try:
        os.chmod(cp, 0o600)
    except OSError:
        pass
    return p


def _load_native_meta(room: str, role: str) -> dict:
    return json.loads(_native_meta_path(room, role).read_text(encoding="utf-8"))


def _inbox_seen(inbox: Path) -> tuple[int, set]:
    """Resume state from the local inbox: (max stream seq, seen chat-message ids)."""
    max_sseq, seen = 0, set()
    if inbox.exists():
        for line in inbox.read_text(encoding="utf-8").splitlines():
            try:
                m = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if isinstance(m, dict):
                if isinstance(m.get("sseq"), int):
                    max_sseq = max(max_sseq, m["sseq"])
                if m.get("mid"):
                    seen.add(m["mid"])
    return max_sseq, seen


async def _native_listen_loop(room: str, role: str, door_native: str, inbox: Path):
    """Supervised listener: connect -> bind our replay/push consumer (resume from the last
    stream seq we recorded) -> heartbeat presence -> append every peer turn as it is PUSHED.
    Reconnects with backoff forever (no hard overall-timeout; kill the process to stop)."""
    from native_client import NativeClient, ack_stream_seq
    meta = _load_native_meta(room, role)
    space, channel, member = meta["space"], meta["channel"], meta.get("member_id", role)
    backoff = 1.0
    while True:
        client = NativeClient(door_native, meta["creds"], name=f"{role}@{room[:12]}")
        try:
            await client.connect()
            print(f"[native] connected as {role} (member {member})", flush=True)
            backoff = 1.0
            max_sseq, seen = _inbox_seen(inbox)
            deliver = f"{client.inbox_prefix}.push"
            _sid, q = await client.subscribe(deliver)
            await client.ensure_chat_consumer(
                space=space, channel=channel, deliver_subject=deliver,
                start_seq=(max_sseq + 1) if max_sseq else None)
            print(f"[native] replay/push consumer bound "
                  f"({'resume from ' + str(max_sseq + 1) if max_sseq else 'full history'})",
                  flush=True)

            async def heartbeat():
                while True:
                    try:
                        await client.presence_put(space=space, doc={
                            "name": role, "member": member, "room": channel,
                            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
                    except Exception:  # noqa: BLE001
                        return                     # socket died; outer loop reconnects
                    await asyncio.sleep(10)

            hb = asyncio.create_task(heartbeat())
            try:
                while True:
                    ev = await q.get()             # ("msg", subject, sid, reply, payload)
                    if ev[0] == "closed":
                        raise ConnectionError("mesh socket closed")   # -> outer reconnect
                    subject, reply, payload = ev[1], ev[3], ev[4]
                    toks = subject.split(".")
                    sender_nkey = toks[4] if len(toks) >= 6 else ""
                    if sender_nkey == client.nkey:
                        continue                   # my own turn: I know what I said
                    try:
                        m = json.loads(payload.decode("utf-8", "replace"))
                    except ValueError:
                        continue
                    mid = m.get("id")
                    if mid and mid in seen:
                        continue                   # replay overlap -> exactly-once locally
                    if mid:
                        seen.add(mid)
                    sseq = ack_stream_seq(reply)
                    # text: cotal wire schema carries parts[]; our legacy mirror carries text
                    text = m.get("text")
                    if not text and isinstance(m.get("parts"), list):
                        text = " ".join(p.get("text", "") for p in m["parts"]
                                        if isinstance(p, dict) and p.get("kind") == "text")
                    row = {"role": (m.get("from") or {}).get("name") or sender_nkey[-8:],
                           "text": text,
                           "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                           "mid": mid}
                    if sseq:
                        row["sseq"] = sseq
                    with open(inbox, "a", encoding="utf-8") as f:
                        f.write(json.dumps(row) + "\n")
            finally:
                hb.cancel()
        except Exception as exc:  # noqa: BLE001
            print(f"[native] connection lost ({type(exc).__name__}: {exc}); "
                  f"reconnecting in {backoff:.0f}s", flush=True)
        finally:
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


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
    if a.native:
        bundle = fetch_native_creds(a.broker, a.room, token, role=a.role)
        if bundle.get("status") != 200:
            print(f"[attach] native creds refused: {json.dumps(bundle)} -- "
                  f"falling back is manual (re-run without --native).", file=sys.stderr)
            return 1
        _save_native_meta(a.room, a.role, bundle)
        print(f"[attach] native cred minted (member {bundle.get('member_id')}, "
              f"channel {bundle.get('channel')}) -- replay + push + presence enabled.")
    logf = open(_sessions_dir() / f"{a.room}.{a.role}.listener.log", "a", encoding="utf-8")
    # Fully detach: own process group, no inherited stdin/stdout pipe (else a parent using
    # command-substitution blocks waiting on the never-exiting listener's inherited fd).
    kw = {"stdin": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        DETACHED_PROCESS = 0x00000008
        kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS  # type: ignore[attr-defined]
    else:
        kw["start_new_session"] = True
    argv = [sys.executable, str(HERE / "room.py"), "listen",
            "--room", a.room, "--role", a.role, "--broker", a.broker,
            "--door", a.door, "--door-native", a.door_native, "--token-file", a.token_file]
    if a.native:
        argv.append("--native")
    p = subprocess.Popen(argv, stdout=logf, stderr=logf, **kw)
    (_sessions_dir() / f"{a.room}.{a.role}.listener.pid").write_text(str(p.pid))
    print(f"[attach] listener running (pid {p.pid}) -> {inbox.name}")
    if not a.native:
        # LATE-JOIN CATCH-UP (grant path): pull everything committed before you attached.
        # (Native mode needs no backfill call here -- the listener REPLAYS server-side.)
        got = backfill(a.broker, a.room, a.role, token, inbox)
        if got:
            print(f"[attach] backfilled {got} committed turn(s) from before you attached -- `read` has them.")
    print(f"[attach] now: `python room.py read --room {a.room}` to see peers, "
          f"`python room.py post --room {a.room} --role {a.role} --text \"...\"` to reply.")
    return 0


def cmd_listen(a) -> int:                        # internal -- spawned detached by attach
    if a.native:
        asyncio.run(_native_listen_loop(a.room, a.role, a.door_native,
                                        _inbox_path(a.room, a.role)))
        return 0
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
    if a.native:
        async def _np():
            from native_client import NativeClient
            meta = _load_native_meta(a.room, a.role)
            client = NativeClient(a.door_native, meta["creds"], name=f"{a.role}-post")
            await client.connect()
            try:
                # publish_chat PING/PONG-flushes: the server has the message before we exit
                # (JetStream persists it durably -- peers replay it even if offline right now).
                await client.publish_chat(space=meta["space"], channel=meta["channel"],
                                          text=a.text, sender=a.role)
            finally:
                await client.close()
        asyncio.run(_np())
    else:
        asyncio.run(_publish(a.broker, a.door, a.room, a.role, a.text, token))  # live to peers
    verdict = post_turn(a.broker, a.room, a.role, a.text, token)             # durable commit
    print(f"[post] published ({'native' if a.native else 'mesh'}) + committed: {json.dumps(verdict)}")
    return 0 if verdict.get("status") in (200, 202) else 1


def cmd_leave(a) -> int:
    """Release your seat so another member can claim it (the room stays live)."""
    token = _token(a.token_file)
    url = f"{a.broker.rstrip('/')}/rooms/{a.room}/leave"
    body = json.dumps({"role": a.role}).encode()
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            res = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            res = json.loads(e.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            res = {"status": e.code}
    ok = res.get("status") == 200
    print(f"[leave] seat '{a.role}': {'released' if ok else json.dumps(res)}")
    # stop the local listener + drop the native cred meta (we no longer hold the seat)
    pidf = _sessions_dir() / f"{a.room}.{a.role}.listener.pid"
    if pidf.exists():
        try:
            pid = int(pidf.read_text().strip())
            if os.name == "nt":
                subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                               capture_output=True)
            else:
                os.kill(pid, 15)
            print(f"[leave] stopped local listener (pid {pid})")
        except Exception:  # noqa: BLE001
            pass
        try:
            pidf.unlink()
        except OSError:
            pass
    return 0 if ok else 1


def cmd_who(a) -> int:
    """Live presence roster (native): who is actually attached to this room right now."""
    async def _who():
        from native_client import NativeClient
        meta = _load_native_meta(a.room, a.role)
        client = NativeClient(a.door_native, meta["creds"], name=f"{a.role}-who")
        await client.connect()
        try:
            rows = await client.presence_snapshot(space=meta["space"])
        finally:
            await client.close()
        import calendar
        fresh_cutoff = time.time() - 45
        n = 0
        seen_cards = set()
        for r in sorted(rows, key=lambda r: str(r.get("at") or "")):
            card = r.get("card") if isinstance(r.get("card"), dict) else None
            if card and (card.get("name"), card.get("role")) in seen_cards:
                continue                          # per-connection presence keys duplicate cards
            if card:
                seen_cards.add((card.get("name"), card.get("role")))
            if card and card.get("name") and card.get("kind") != "supervisor":
                # a COTAL-NATIVE client's presence doc (cotal join / spawn) -- space-wide,
                # no room field; shown tagged so the roster covers both client kinds. cotal's
                # presence carries `status` (online/idle/offline) rather than our timestamp.
                st = r.get("status") or card.get("status") or ""
                tag = f" [{st}]" if st else ""
                print(f"{'':22}[cotal]{tag} {card.get('name')}"
                      f"{'/' + card['role'] if card.get('role') else ''}")
                n += 1
                continue
            if not r.get("name") or not r.get("room"):
                continue                          # cotal's own daemons heartbeat here too
            if r.get("room") != meta["channel"]:
                continue                          # presence is space-wide; show this room only
            try:                                  # timegm: the timestamp is UTC (mktime is not)
                at = calendar.timegm(time.strptime(r.get("at", ""), "%Y-%m-%dT%H:%M:%SZ"))
            except Exception:  # noqa: BLE001
                at = 0
            live = "LIVE" if at >= fresh_cutoff else "stale"
            print(f"{r.get('at','')}  [{live}] {r.get('name')} (member {r.get('member')})")
            n += 1
        if not n:
            print("[who] nobody is heartbeating presence in this room right now.")
    asyncio.run(_who())
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Live-session client for an IC agent-room "
                                             "(attach/read/post/who; --native = Cotal 0.11.3 "
                                             "replay + push + presence).")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("attach", "listen", "read", "post", "who", "leave"):
        sp = sub.add_parser(name)
        sp.add_argument("--room", default=DEFAULT_ROOM)
        sp.add_argument("--role", default="ray")
        sp.add_argument("--broker", default=DEFAULT_BROKER)
        sp.add_argument("--door", default=DEFAULT_DOOR)
        sp.add_argument("--door-native", default=DEFAULT_NATIVE_DOOR)
        sp.add_argument("--token-file", default=str(HERE / "sessions" / "ic_agent.json"))
        sp.add_argument("--native", action="store_true",
                        help="use the Cotal-native mesh (minted creds: server-side replay, "
                             "push delivery, presence, attributed publishes)")
        if name == "post":
            sp.add_argument("--text", required=True)
    a = ap.parse_args(argv)
    return {"attach": cmd_attach, "listen": cmd_listen, "read": cmd_read,
            "post": cmd_post, "who": cmd_who, "leave": cmd_leave}[a.cmd](a)


if __name__ == "__main__":
    raise SystemExit(main())
