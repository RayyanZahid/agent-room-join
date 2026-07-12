#!/usr/bin/env python3
"""mesh_agent.py -- the turnkey member-agent loop (step 4 of the two-member runbook).

Runs on a MEMBER'S OWN machine. One command and the member's agent:
  1. fetches a room-scoped grant from the public broker (their IC token),
  2. joins the room's Cotal mesh through the public door (a real NATS-over-ws client),
  3. converses with the OTHER member's agent over the `work` subject -- on each of the
     other's messages it THINKS with the member's own model (agent_fn) and replies,
  4. COMMITS each contribution to the broker's coordination layer via POST /rooms/<id>/turn
     (the durable, gated source of truth -- the mesh is only the live conversation),
  5. terminates on a DONE marker, a turn cap, or the room tearing down (grant/socket dies),
     optionally casting a consensus teardown vote.

The member's model is pluggable (`agent_fn(context) -> str`): `claude_cli_agent_fn` shells out
to the member's own `claude -p`; `ollama_agent_fn` hits an OpenAI-shaped endpoint; a scripted
fn is used in tests + the live proof so the LOOP is verifiable without LLM flakiness.

Two layers, honestly separated (the AUTOPILOT lesson): agents COMMUNICATE over the mesh but
COMMIT through /turn. Trusted attribution is the `member_id` the broker binds on /turn, never
a name claimed on the mesh (open-mode mesh attribution is untrusted).

The NATS frame parser (`parse_frames`) is PURE and unit-tested; the client + loop are driven
by injectable factories so the whole orchestration is testable offline (test_mesh_agent.py).
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any, Callable, Optional

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import mesh_attach  # fetch_grant + door_url (reuse; do not re-derive)

WORK_SUBJECT = "work"
DONE_MARKER = "[[DONE]]"
READY_MARKER = "__READY__"   # presence beacon: NATS-core drops msgs with no subscriber, so we
                             # beacon until each side has heard the other before the opening turn


# ---------------------------------------------------------------------------
# PURE: NATS text-protocol frame parser (buffer in -> events + remaining)
# ---------------------------------------------------------------------------
def parse_frames(buf: str):
    """Parse as many complete NATS protocol messages as `buf` holds. Returns
    (events, remaining) where each event is one of:
        ("msg", subject, sid, payload) | ("ping",) | ("pong",) |
        ("info", rest) | ("ok",) | ("err", rest)
    A partial trailing frame (header without full payload, or no CRLF yet) stays in
    `remaining` for the next read. NEVER raises on a partial buffer."""
    events = []
    i = 0
    n = len(buf)
    while True:
        nl = buf.find("\r\n", i)
        if nl == -1:
            break
        line = buf[i:nl]
        up = line.upper()
        if up.startswith("MSG "):
            # MSG <subject> <sid> [reply] <nbytes>
            parts = line.split()
            try:
                nbytes = int(parts[-1])
            except (ValueError, IndexError):
                # malformed header -- drop the line, keep going (defensive)
                i = nl + 2
                continue
            start = nl + 2
            end = start + nbytes
            if end + 2 > n:                 # payload (+ trailing CRLF) not fully arrived
                break
            payload = buf[start:end]
            events.append(("msg", parts[1], parts[2], payload))
            i = end + 2                     # skip payload + CRLF
        elif up == "PING":
            events.append(("ping",)); i = nl + 2
        elif up == "PONG":
            events.append(("pong",)); i = nl + 2
        elif up == "+OK":
            events.append(("ok",)); i = nl + 2
        elif up.startswith("INFO"):
            events.append(("info", line[5:])); i = nl + 2
        elif up.startswith("-ERR"):
            events.append(("err", line[5:])); i = nl + 2
        else:
            i = nl + 2                      # unknown control op -- skip
    return events, buf[i:]


# ---------------------------------------------------------------------------
# a real (persistent) NATS-over-websocket client
# ---------------------------------------------------------------------------
class NatsWsClient:
    """Minimal persistent NATS client over the door's websocket. Enough for the member
    loop: CONNECT handshake, SUB, PUB, a read loop that dispatches MSG + answers PING."""

    def __init__(self, ws_url: str, *, timeout: float = 20.0):
        self.ws_url = ws_url
        self.timeout = timeout
        self._ws = None
        self._buf = ""
        self._sid = 0

    async def connect(self):
        import websockets
        self._ws = await websockets.connect(self.ws_url, subprotocols=["nats"],
                                            open_timeout=self.timeout, close_timeout=self.timeout)
        greeting = await asyncio.wait_for(self._ws.recv(), self.timeout)  # INFO {...}
        if not _txt(greeting).startswith("INFO"):
            raise RuntimeError(f"mesh did not greet with NATS INFO: {_txt(greeting)[:80]}")
        await self._ws.send(b'CONNECT {"verbose":false,"pedantic":false}\r\n')

    async def subscribe(self, subject: str) -> int:
        self._sid += 1
        await self._ws.send(f"SUB {subject} {self._sid}\r\n".encode())
        return self._sid

    async def publish(self, subject: str, payload: str):
        data = payload.encode("utf-8")
        await self._ws.send(f"PUB {subject} {len(data)}\r\n".encode() + data + b"\r\n")

    async def run(self, on_message: Callable[[str, str], Any]):
        """Read loop: dispatch each MSG to on_message(subject, payload), answer PING. Ends
        when the socket closes (room teardown / grant expiry)."""
        try:
            async for frame in self._ws:
                self._buf += _txt(frame)
                events, self._buf = parse_frames(self._buf)
                for ev in events:
                    if ev[0] == "msg":
                        _subject, _sid, payload = ev[1], ev[2], ev[3]
                        res = on_message(_subject, payload)
                        if asyncio.iscoroutine(res):
                            await res
                    elif ev[0] == "ping":
                        await self._ws.send(b"PONG\r\n")
        except Exception:  # noqa: BLE001 -- a closed/errored socket just ends the loop
            pass

    async def close(self):
        if self._ws is not None:
            await self._ws.close()


def _txt(frame) -> str:
    return frame.decode("utf-8", "replace") if isinstance(frame, (bytes, bytearray)) else str(frame)


# ---------------------------------------------------------------------------
# pluggable "think" functions (the member's own model)
# ---------------------------------------------------------------------------
def scripted_agent_fn(lines):
    """Deterministic agent for tests + the live LOOP proof: yields the next canned line each
    call, then DONE_MARKER. Proves the loop mechanics without any LLM dependency."""
    it = iter(lines)
    def fn(_context: str) -> str:
        return next(it, DONE_MARKER)
    return fn


def claude_cli_agent_fn(*, extra_args=None):
    """The member's OWN Claude via `claude -p`. Each call is one non-interactive completion."""
    def fn(context: str) -> str:
        cmd = ["claude", "-p", context] + (extra_args or [])
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=240)
        return (out.stdout or "").strip() or DONE_MARKER
    return fn


def ollama_agent_fn(endpoint: str, model: str):
    """An OpenAI-shaped chat endpoint (ollama / ZAI / any). Kept dependency-light (urllib)."""
    def fn(context: str) -> str:
        body = json.dumps({"model": model, "messages": [{"role": "user", "content": context}],
                           "max_tokens": 200}).encode()
        req = urllib.request.Request(endpoint.rstrip("/") + "/chat/completions", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            d = json.loads(r.read())
        return (d["choices"][0]["message"]["content"] or "").strip() or DONE_MARKER
    return fn


# ---------------------------------------------------------------------------
# coordination commit: POST /rooms/<id>/turn
# ---------------------------------------------------------------------------
def post_turn(broker_base: str, room: str, role: str, content: str, token: str,
              *, timeout: float = 10.0) -> dict:
    """Commit one turn to the broker (the gated source of truth). Returns the verdict dict."""
    url = f"{broker_base.rstrip('/')}/rooms/{room}/turn"
    body = json.dumps({"role": role, "content": content}).encode()
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except Exception:  # noqa: BLE001
            return {"status": e.code, "reason": f"http_{e.code}"}


# ---------------------------------------------------------------------------
# the member-agent loop
# ---------------------------------------------------------------------------
class MeshAgent:
    """One member's autonomous participant. Fully injectable so the orchestration is testable
    offline: pass a fake `client` + `post_fn` + a scripted `agent_fn`."""

    def __init__(self, *, role: str, agent_fn: Callable[[str], str], room: str, token: str,
                 broker_base: str, subject: str = WORK_SUBJECT, max_turns: int = 4,
                 initiator: bool = False, post_fn: Optional[Callable] = None,
                 client: Any = None, overall_timeout: float = 90.0,
                 ready_timeout: float = 8.0, ready_interval: float = 0.4, goal: str = "",
                 roster: Optional[list] = None):
        self.role = role
        self.agent_fn = agent_fn
        self.room = room
        self.token = token
        self.broker_base = broker_base
        self.subject = subject
        self.max_turns = max_turns
        self.initiator = initiator                 # the role that opens the conversation
        self.post_fn = post_fn or (lambda room, role, content, token:
                                   post_turn(self.broker_base, room, role, content, token))
        self.client = client                        # injectable NatsWsClient (or fake)
        self.overall_timeout = overall_timeout
        self.ready_timeout = ready_timeout          # <=0 skips the handshake (unit tests)
        self.ready_interval = ready_interval
        self.goal = goal                            # optional shared task the agents work on
        # multi-party (N>2): a full role roster turns the loop from pairwise ping-pong into a
        # round-robin. roster[0] opens; each turn k is spoken by roster[k % n]. None => pairwise.
        self.roster = list(roster) if roster else None
        self.multiparty = bool(self.roster and len(self.roster) > 1)
        self.n = len(self.roster) if self.roster else 2
        self.my_index = self.roster.index(role) if (self.roster and role in self.roster) else 0
        self._peers_ready: set = set()
        self._all_ready = asyncio.Event()
        self.turns = 0
        self.committed = []                         # verdicts from post_fn (for tests/inspection)
        self._done = asyncio.Event()
        self._peer_ready = asyncio.Event()

    def _wrap(self, text: str, turn: Optional[int] = None) -> str:
        d = {"role": self.role, "text": text}
        if turn is not None:
            d["turn"] = turn                        # multi-party round-robin index
        return json.dumps(d)

    def _goal_preamble(self) -> str:
        """Prefix the agent's think-prompt with the room's shared task, if one was given."""
        if not self.goal:
            return ""
        return (f"SHARED TASK for this room (collaborate with the other member to deliver it):\n{self.goal}\n\n"
                f"Keep EACH turn SHORT — a few sentences or a tight bulleted list — and converge "
                f"quickly toward a concrete, agreed plan. Do not restate the whole plan every turn.\n\n")

    async def _think(self, prompt: str) -> str:
        """Run the (synchronous, possibly slow) model call OFF the event loop. A blocking
        agent_fn -- claude -p / ollama can take 30-120s -- called inline would freeze the loop
        past the websockets keepalive-ping timeout (~20s), and the mesh socket gets force-closed
        (1011 keepalive ping timeout) mid-dialogue, silently killing it. Offloading to a thread
        keeps the reader + PONG + keepalive alive during inference. (Confirmed root cause of the
        claude-model cross-process dialogue failure; scripted worked only because it's instant.)"""
        return await asyncio.get_event_loop().run_in_executor(None, self.agent_fn, prompt)

    async def _handle(self, _subject: str, payload: str):
        try:
            msg = json.loads(payload)
        except Exception:  # noqa: BLE001 -- ignore non-JSON chatter on the subject
            return
        if msg.get("role") == self.role:            # my own echo -- ignore
            return
        if str(msg.get("text", "")) == READY_MARKER:  # peer is subscribed -> handshake done
            if not self._peer_ready.is_set():
                self._peer_ready.set()
                # ACK once: guarantees the peer also confirms even if it already stopped its
                # own beacon loop (its reader is still live). Closes the cross-process window
                # where one side's beacon expired before the other had subscribed.
                await self.client.publish(self.subject, self._wrap(READY_MARKER))
            return
        if DONE_MARKER in str(msg.get("text", "")):  # the other side finished
            self._done.set()
            return
        if self.turns >= self.max_turns:            # I'm done contributing -> announce + stop
            await self.client.publish(self.subject, self._wrap(DONE_MARKER))
            self._done.set()
            return
        reply = await self._think(self._goal_preamble() +
                                  f"[room {self.room}] {msg.get('role')} said: {msg.get('text')}\n"
                                  f"You are '{self.role}'. Respond with your contribution.")
        await self.client.publish(self.subject, self._wrap(reply))          # communicate
        verdict = self.post_fn(self.room, self.role, reply, self.token)     # commit (coordination)
        self.committed.append(verdict)
        self.turns += 1
        if DONE_MARKER in reply:
            self._done.set()

    async def _await_peer_ready(self, *, timeout: Optional[float] = None) -> bool:
        """Beacon READY until the peer's beacon is heard, so no turn predates a subscription
        (NATS-core drops messages with no subscriber). The beacon is REPEATED, so it survives
        the subscribe race that a single un-repeated publish (like the opening) does not.
        Returns True once the peer is confirmed subscribed, False if we gave up after `timeout`
        (default `ready_timeout`). The initiator gates its opening on a True return so a lost,
        never-retried opening can't strand the responder at zero turns."""
        if self.ready_timeout <= 0:
            return True
        loop = asyncio.get_event_loop()
        deadline = loop.time() + (self.ready_timeout if timeout is None else timeout)
        while not self._peer_ready.is_set() and loop.time() < deadline:
            await self.client.publish(self.subject, self._wrap(READY_MARKER))
            try:
                await asyncio.wait_for(self._peer_ready.wait(), self.ready_interval)
            except asyncio.TimeoutError:
                pass
        return self._peer_ready.is_set()

    # --- multi-party (N>2) round-robin path (additive; the pairwise path below is unchanged) ---
    async def _await_all_peers_ready(self, *, timeout: float) -> bool:
        """Beacon READY until ALL n-1 peers are heard, so the opener's single turn-0 publish
        can't be dropped for a not-yet-subscribed member. Best-effort past `timeout`."""
        if self.ready_timeout <= 0 or self.n <= 1:
            return True
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while not self._all_ready.is_set() and loop.time() < deadline:
            await self.client.publish(self.subject, self._wrap(READY_MARKER))
            try:
                await asyncio.wait_for(self._all_ready.wait(), self.ready_interval)
            except asyncio.TimeoutError:
                pass
        return self._all_ready.is_set()

    async def _handle_mp(self, _subject: str, payload: str):
        """Round-robin handler: each content message carries its turn index k; the member at
        roster[(k+1) % n] speaks turn k+1, until n*max_turns turns. Only the single next
        speaker replies (no cascade), so it scales past two members."""
        try:
            msg = json.loads(payload)
        except Exception:  # noqa: BLE001
            return
        role = msg.get("role")
        if role == self.role:
            return
        text = str(msg.get("text", ""))
        if text == READY_MARKER:
            if role:
                newly = role not in self._peers_ready
                self._peers_ready.add(role)
                if len(self._peers_ready) >= self.n - 1:
                    self._all_ready.set()
                if newly and self._all_ready.is_set():   # ACK a late peer once I've stopped beaconing
                    await self.client.publish(self.subject, self._wrap(READY_MARKER))
            return
        k = msg.get("turn")
        if not isinstance(k, int):
            return
        total = self.n * self.max_turns
        nxt = k + 1
        if nxt >= total:
            self._done.set()
            return
        if self.roster[nxt % self.n] != self.role:       # not my turn -> stay quiet (no cascade)
            return
        reply = await self._think(self._goal_preamble() +
                                  f"[room {self.room}] {role} (turn {k}) said: {text}\n"
                                  f"You are '{self.role}', one of {self.n} members {self.roster}. "
                                  f"Add YOUR distinct contribution as turn {nxt} -- build on the others, don't repeat.")
        await self.client.publish(self.subject, self._wrap(reply, turn=nxt))
        self.committed.append(self.post_fn(self.room, self.role, reply, self.token))
        self.turns += 1
        if nxt + 1 >= total:
            self._done.set()

    async def _run_multiparty(self):
        await self.client.connect()
        await self.client.subscribe(self.subject)
        reader = asyncio.ensure_future(self.client.run(self._handle_mp))
        allr = await self._await_all_peers_ready(timeout=self.overall_timeout)
        if self.my_index == 0 and allr:                  # roster[0] opens turn 0
            opening = await self._think(self._goal_preamble() +
                f"You are '{self.role}', opening a room of {self.n} members {self.roster}. "
                f"Propose the first step (this is turn 0).")
            await self.client.publish(self.subject, self._wrap(opening, turn=0))
            self.committed.append(self.post_fn(self.room, self.role, opening, self.token))
            self.turns += 1
            if self.n * self.max_turns <= 1:
                self._done.set()
        try:
            await asyncio.wait_for(self._done.wait(), timeout=self.overall_timeout)
        except asyncio.TimeoutError:
            pass
        reader.cancel()
        await self.client.close()
        return {"role": self.role, "turns": self.turns, "committed": self.committed, "roster": self.roster}

    async def run(self):
        if self.multiparty:
            return await self._run_multiparty()
        await self.client.connect()
        await self.client.subscribe(self.subject)
        reader = asyncio.ensure_future(self.client.run(self._handle))
        # Confirm the peer is subscribed BEFORE the initiator publishes its (single, un-repeated)
        # opening. The initiator waits up to overall_timeout so cross-process / two-machine launch
        # skew can't expire the short handshake and strand a lost opening; the responder's live
        # reader ACKs a late beacon, so this resolves as soon as both sides are up. The responder
        # only needs a best-effort pass, since it merely replies to an opening it receives.
        confirm_timeout = self.overall_timeout if self.initiator else None
        confirmed = await self._await_peer_ready(timeout=confirm_timeout)
        if self.initiator and confirmed:            # open the conversation
            opening = await self._think(self._goal_preamble() +
                                        f"You are '{self.role}' opening room {self.room}. "
                                        f"Propose the first step.")
            await self.client.publish(self.subject, self._wrap(opening))
            self.committed.append(self.post_fn(self.room, self.role, opening, self.token))
            self.turns += 1
        try:
            await asyncio.wait_for(self._done.wait(), timeout=self.overall_timeout)
        except asyncio.TimeoutError:
            pass
        reader.cancel()
        await self.client.close()
        return {"role": self.role, "turns": self.turns, "committed": self.committed}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Autonomous member-agent for an agent-room.")
    ap.add_argument("--broker", required=True)
    ap.add_argument("--door", required=True)
    ap.add_argument("--room", required=True)
    ap.add_argument("--role", required=True)
    ap.add_argument("--token-file", default="sessions/ic_agent.json")
    ap.add_argument("--initiator", action="store_true")
    ap.add_argument("--max-turns", type=int, default=4)
    ap.add_argument("--model", choices=["claude", "ollama", "scripted"], default="claude")
    ap.add_argument("--ollama-endpoint", default="http://localhost:11434/v1")
    ap.add_argument("--ollama-model", default="qwen2.5:3b-instruct")
    ap.add_argument("--overall-timeout", type=float, default=90.0,
                    help="max seconds to wait for the peer's reply / dialogue completion. Bump well "
                         "above 90 for slow models -- claude -p per turn can be ~30-120s, so a "
                         "multi-turn claude dialogue needs e.g. 600 or the responder times out first.")
    ap.add_argument("--ready-timeout", type=float, default=15.0,
                    help="seconds the READY handshake beacons before a best-effort proceed (the "
                         "initiator extends this to --overall-timeout to confirm the peer subscribed).")
    ap.add_argument("--goal", default="",
                    help="a shared task the agents collaborate on (injected into each agent's "
                         "prompt), e.g. --goal 'plan an Immersive Commons event: theme, format, promo'.")
    ap.add_argument("--roster", default="",
                    help="comma-separated FULL role order for a MULTI-PARTY (N>2) room, e.g. "
                         "--roster organizer,promoter,budget. roster[0] opens; agents speak round-robin "
                         "(each message carries its turn index). Omit for classic 2-agent pairwise mode.")
    args = ap.parse_args(argv)

    token = json.load(open(args.token_file))["agent_token"]
    grant = mesh_attach.fetch_grant(args.broker, args.room, token)
    client = NatsWsClient(mesh_attach.door_url(args.door, grant))
    if args.model == "claude":
        agent_fn = claude_cli_agent_fn()
    elif args.model == "ollama":
        agent_fn = ollama_agent_fn(args.ollama_endpoint, args.ollama_model)
    else:
        agent_fn = scripted_agent_fn([f"{args.role} contribution 1", f"{args.role} contribution 2"])
    agent = MeshAgent(role=args.role, agent_fn=agent_fn, room=args.room, token=token,
                      broker_base=args.broker, max_turns=args.max_turns, initiator=args.initiator,
                      client=client, ready_timeout=args.ready_timeout,
                      overall_timeout=args.overall_timeout, goal=args.goal,
                      roster=[r.strip() for r in args.roster.split(",") if r.strip()] or None)
    result = asyncio.run(agent.run())
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
