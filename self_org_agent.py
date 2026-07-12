#!/usr/bin/env python3
"""self_org_agent.py -- SELF-ORGANIZING agents: the team negotiates a task ALLOCATION from who
is present + their capabilities, and RE-negotiates (best-fit) when a new agent joins.

This is the emergent-coordination layer above the fixed-roster round-robin: instead of the
operator assigning roles, the agents decide who-does-what, and re-decide when the composition
changes.

Protocol over the room's mesh (committed turns are the durable record + what the dashboard shows):
  hello {type:hello, role, cap}          -- capability card, beaconed on join
  alloc {type:alloc, by, version, map}   -- the facilitator's task->owner map (LLM-synthesized)
  work  {type:work, role, text}          -- an agent's contribution to its currently-allocated task

The FACILITATOR (--facilitator, the opener) synthesizes the allocation from the capability cards
it has seen; a NEW hello card (a join) fires a re-synthesis (re-balance -- it may REASSIGN existing
work for best fit). Every agent executes whatever task the current allocation gives it, and
re-executes if a newer allocation changes its assignment. Emergent because the allocation itself is
the agents' own LLM reasoning over the live roster; self-organizing because a join reshapes it.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Optional

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import mesh_attach  # noqa: E402
from mesh_agent import (NatsWsClient, post_turn, claude_cli_agent_fn,  # noqa: E402
                        ollama_agent_fn, WORK_SUBJECT)

_JSON_OBJ = re.compile(r"\{.*\}", re.DOTALL)


def extract_json_map(text: str) -> dict:
    """Pull the first {...} object out of an LLM reply and keep only str->str pairs. Tolerant of
    prose/markdown fences around it; returns {} if nothing parseable."""
    if not text:
        return {}
    m = _JSON_OBJ.search(text)
    if not m:
        return {}
    try:
        obj = json.loads(m.group(0))
    except Exception:  # noqa: BLE001
        return {}
    if not isinstance(obj, dict):
        return {}
    return {str(k): str(v) for k, v in obj.items() if isinstance(v, (str, int, float))}


class SelfOrgAgent:
    def __init__(self, *, role: str, capabilities: str, agent_fn: Callable[[str], str], room: str,
                 token: str, broker_base: str, client: Any, facilitator: bool = False, goal: str = "",
                 post_fn: Optional[Callable] = None, overall_timeout: float = 600.0,
                 settle_s: float = 6.0, subject: str = WORK_SUBJECT):
        self.role = role
        self.capabilities = capabilities
        self.agent_fn = agent_fn
        self.room = room
        self.token = token
        self.broker_base = broker_base
        self.client = client
        self.facilitator = facilitator
        self.goal = goal
        self.subject = subject
        self.overall_timeout = overall_timeout
        self.settle_s = settle_s
        self.post_fn = post_fn or (lambda room, role, content, token:
                                   post_turn(self.broker_base, room, role, content, token))
        self.cards = {role: capabilities}      # role -> capability card (incl. self)
        self.allocation: Optional[dict] = None  # current {role: task}
        self.alloc_version = 0
        self._executed_version = 0             # last allocation version I've acted on
        self._roster_changed = asyncio.Event()
        self._done = asyncio.Event()
        self.turns = 0
        self.committed = []

    def _card(self) -> str:
        return json.dumps({"type": "hello", "role": self.role, "cap": self.capabilities})

    async def _think(self, prompt: str) -> str:
        # off the event loop so a slow claude -p can't starve the websocket keepalive (1011 fix)
        return await asyncio.get_event_loop().run_in_executor(None, self.agent_fn, prompt)

    async def _handle(self, _subject: str, payload: str):
        try:
            msg = json.loads(payload)
        except Exception:  # noqa: BLE001
            return
        if not isinstance(msg, dict) or msg.get("role") == self.role:
            return
        t = msg.get("type")
        if t == "hello":
            r = msg.get("role")
            if r:
                is_new = r not in self.cards
                self.cards[r] = msg.get("cap", "")
                if is_new and self.facilitator:
                    self._roster_changed.set()      # a join -> re-negotiate
        elif t == "alloc":
            v = int(msg.get("version", 0) or 0)
            if v > self.alloc_version:               # a newer allocation to act on
                self.allocation = msg.get("map", {}) or {}
                self.alloc_version = v

    async def _synthesize_allocation(self, version: int):
        cards = "\n".join(f"- {r}: {c or '(no card)'}" for r, c in sorted(self.cards.items()))
        prior = f"\nPrevious allocation (v{version-1}): {json.dumps(self.allocation)}" if self.allocation else ""
        prompt = (f"You are the FACILITATOR of a self-organizing agent team.\n"
                  f"GOAL: {self.goal}\n\nMEMBERS PRESENT NOW + capabilities:\n{cards}{prior}\n\n"
                  f"Allocate the work: give EACH present member the ONE sub-task they are BEST fit for, "
                  f"together fully covering the goal. If a member just joined, REBALANCE -- you may "
                  f"reassign existing sub-tasks for better fit. Output ONLY a JSON object mapping "
                  f"role -> a one-line task. No prose, no markdown fence.")
        amap = extract_json_map(await self._think(prompt))
        if not amap:
            return
        self.allocation = amap
        self.alloc_version = version
        await self.client.publish(self.subject, json.dumps(
            {"type": "alloc", "role": self.role, "version": version, "map": amap}))
        self.committed.append(self.post_fn(self.room, self.role,
                              f"[allocation v{version} by {self.role}] " + json.dumps(amap), self.token))
        self.turns += 1

    async def _execute_my_task(self):
        my_task = (self.allocation or {}).get(self.role)
        self._executed_version = self.alloc_version
        if not my_task:
            return                                   # this allocation didn't give me a task
        text = await self._think(
            f"You are '{self.role}' on a self-organizing team. GOAL: {self.goal}\n"
            f"The team's current allocation (v{self.alloc_version}) assigned YOU: {my_task}\n"
            f"Deliver your part concisely (a few sentences or tight bullets).")
        await self.client.publish(self.subject, json.dumps({"type": "work", "role": self.role, "text": text}))
        self.committed.append(self.post_fn(self.room, self.role, f"[work: {my_task}] {text}", self.token))
        self.turns += 1

    async def run(self):
        await self.client.connect()
        await self.client.subscribe(self.subject)
        reader = asyncio.ensure_future(self.client.run(self._handle))
        # capability handshake: beacon my card so every present member learns the roster + caps
        ticks = max(1, int(self.settle_s / 0.5))
        for _ in range(ticks):
            await self.client.publish(self.subject, self._card())
            await asyncio.sleep(0.5)
        self.committed.append(self.post_fn(self.room, self.role,
                              f"[hello] {self.role} capabilities: {self.capabilities}", self.token))
        if self.facilitator:
            await self._synthesize_allocation(version=1)
        loop = asyncio.get_event_loop()
        deadline = loop.time() + self.overall_timeout
        while loop.time() < deadline and not self._done.is_set():
            if self.allocation is not None and self.alloc_version > self._executed_version:
                await self._execute_my_task()        # (re)do my part for the current allocation
            if self.facilitator and self._roster_changed.is_set():
                self._roster_changed.clear()
                await asyncio.sleep(1.5)              # let the newcomer's card + hello settle
                await self._synthesize_allocation(version=self.alloc_version + 1)
            await asyncio.sleep(0.5)
        reader.cancel()
        await self.client.close()
        return {"role": self.role, "turns": self.turns, "allocation": self.allocation,
                "alloc_version": self.alloc_version, "committed": self.committed}


def _main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Self-organizing agent (negotiated task allocation).")
    ap.add_argument("--broker", required=True)
    ap.add_argument("--door", required=True)
    ap.add_argument("--room", required=True)
    ap.add_argument("--role", required=True)
    ap.add_argument("--token-file", default="sessions/ic_agent.json")
    ap.add_argument("--capabilities", default="", help="what this agent is good at / has access to")
    ap.add_argument("--facilitator", action="store_true", help="this agent synthesizes the allocation")
    ap.add_argument("--goal", default="")
    ap.add_argument("--model", choices=["claude", "ollama"], default="claude")
    ap.add_argument("--overall-timeout", type=float, default=600.0)
    ap.add_argument("--settle", type=float, default=6.0)
    ap.add_argument("--ollama-endpoint", default="http://localhost:11434/v1")
    ap.add_argument("--ollama-model", default="qwen2.5:3b-instruct")
    args = ap.parse_args(argv)

    token = json.load(open(args.token_file))["agent_token"]
    grant = mesh_attach.fetch_grant(args.broker, args.room, token)
    client = NatsWsClient(mesh_attach.door_url(args.door, grant))
    agent_fn = (claude_cli_agent_fn() if args.model == "claude"
                else ollama_agent_fn(args.ollama_endpoint, args.ollama_model))
    agent = SelfOrgAgent(role=args.role, capabilities=args.capabilities, agent_fn=agent_fn,
                         room=args.room, token=token, broker_base=args.broker, client=client,
                         facilitator=args.facilitator, goal=args.goal,
                         overall_timeout=args.overall_timeout, settle_s=args.settle)
    print(json.dumps(asyncio.run(agent.run()), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
