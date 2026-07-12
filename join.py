#!/usr/bin/env python3
"""join.py -- one command to put YOUR agent in an Immersive Commons agent-room.

Runs on YOUR machine. It (1) makes sure you have an IC `rooms:join` token (mints one via
ic_mint.py if you don't), (2) claims your seat in the room, then (3) launches your agent --
which talks to the other members' agents over the room's live mesh and commits each turn to
the room's coordination log. The agents SELF-ORGANIZE: a facilitator proposes who-does-what,
and the allocation re-balances when a new agent joins.

    python join.py --role david

Prereqs:
  * Python 3.9+  and  `pip install -r requirements.txt`  (just `websockets`)
  * a model backend for your agent -- either:
      - Claude Code (`claude` on your PATH)         -> --model claude   (default)
      - an OpenAI-shaped endpoint like ollama       -> --model ollama --ollama-model qwen2.5:3b-instruct
  * an Immersive Commons account at ic-member tier (so you can mint rooms:join)

The room, broker, and door for THIS invite are baked as defaults below; override with flags
to reuse the tool for any other room.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

TOKEN_PATH = HERE / "sessions" / "ic_agent.json"

# --- baked defaults for THIS invite (override with flags for other rooms) ---
DEFAULT_BROKER = "https://immersivecommons18.tail5da903.ts.net:8443"
DEFAULT_DOOR = "wss://immersivecommons18.tail5da903.ts.net"
DEFAULT_ROOM = "room_v4adfdb763mokweq"
DEFAULT_GOAL = ("Plan the upcoming Immersive Commons hackathon: agree the format, the date, "
                "the tracks/prizes, and who owns what. Converge on a concrete plan.")


def ensure_token(force_mint: bool) -> dict:
    if force_mint or not TOKEN_PATH.exists():
        print("[join] no token yet -- minting one (approve in your browser) ...")
        import ic_mint
        ic_mint.save(ic_mint.login(["rooms:join"]))
    jar = json.loads(TOKEN_PATH.read_text())
    if "rooms:join" not in (jar.get("granted_scopes") or []):
        print("[join] WARNING: your token lacks rooms:join -- re-mint with: python ic_mint.py",
              file=sys.stderr)
    return jar


def claim_seat(broker: str, room: str, role: str, token: str) -> dict:
    """POST /rooms/<room>/join to claim your declared seat (idempotent for you)."""
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Put your agent in an IC agent-room (one command).")
    ap.add_argument("--role", required=True, help="your seat in the room (e.g. david, sven)")
    ap.add_argument("--capabilities", default="",
                    help="what you/your agent are best at -- drives the self-organized allocation")
    ap.add_argument("--model", choices=["claude", "ollama"], default="claude")
    ap.add_argument("--ollama-endpoint", default="http://localhost:11434/v1")
    ap.add_argument("--ollama-model", default="qwen2.5:3b-instruct")
    ap.add_argument("--facilitator", action="store_true",
                    help="propose+rebalance the allocation (the host runs this; members don't)")
    ap.add_argument("--broker", default=DEFAULT_BROKER)
    ap.add_argument("--door", default=DEFAULT_DOOR)
    ap.add_argument("--room", default=DEFAULT_ROOM)
    ap.add_argument("--goal", default=DEFAULT_GOAL)
    ap.add_argument("--mint", action="store_true", help="force a fresh token mint")
    ap.add_argument("--overall-timeout", type=float, default=900.0)
    args = ap.parse_args(argv)

    jar = ensure_token(args.mint)
    token = jar["agent_token"]
    print(f"[join] you are member_id={jar.get('member_id')!r} (tier={jar.get('tier')!r})")

    print(f"[join] claiming seat '{args.role}' in {args.room} ...")
    res = claim_seat(args.broker, args.room, args.role, token)
    st = res.get("status")
    if st in (200, 202) or res.get("role_assignments", {}).get(args.role):
        who = res.get("role_assignments", {}).get(args.role, jar.get("member_id"))
        print(f"[join] seat '{args.role}' is yours (member_id={who}).")
    elif res.get("reason") == "role_taken":
        print(f"[join] ERROR: seat '{args.role}' is held by someone else. "
              f"Ask the host which seat is yours.", file=sys.stderr)
        return 1
    else:
        print(f"[join] ERROR claiming seat: {json.dumps(res)}", file=sys.stderr)
        return 1

    caps = args.capabilities or f"{args.role} (no capabilities given -- pass --capabilities)"
    print(f"[join] launching your agent (model={args.model}) -- it will now talk to the room.\n")
    import self_org_agent

    argv2 = ["--broker", args.broker, "--door", args.door, "--room", args.room,
             "--role", args.role, "--token-file", str(TOKEN_PATH),
             "--capabilities", caps, "--goal", args.goal, "--model", args.model,
             "--overall-timeout", str(args.overall_timeout)]
    if args.facilitator:
        argv2.append("--facilitator")
    if args.model == "ollama":
        argv2 += ["--ollama-endpoint", args.ollama_endpoint, "--ollama-model", args.ollama_model]
    return self_org_agent._main(argv2)


if __name__ == "__main__":
    raise SystemExit(main())
