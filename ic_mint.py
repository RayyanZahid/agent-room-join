#!/usr/bin/env python3
"""ic_mint.py -- standalone Immersive Commons agent-token minter (device-code flow).

Mints a `rooms:join`-scoped IC agent token from ANY machine, with ZERO dependency on
Ray's private `life` repo -- pure stdlib HTTP against the public IC site. You approve the
grant in YOUR OWN browser (signed in to Immersive Commons as yourself), so the token is
bound to YOUR member identity. The token is written to sessions/ic_agent.json next to this
file; join.py reads it.

Flow (RFC-8628 device-code):
  1. POST /api/agent/signup/start {scopes} -> device_code + user_code + verify_url_complete
  2. you open verify_url_complete, confirm the user_code, approve  (as yourself)
  3. GET /api/agent/signup/poll?device_code=... until status=completed -> agent_token

Usage:
  python ic_mint.py                      # mint a rooms:join token, save it, print your member_id
  python ic_mint.py --scopes rooms:join  # explicit scopes
  IC_BASE_URL=http://localhost:3000 python ic_mint.py   # point at a non-prod IC
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

HERE = Path(__file__).resolve().parent
TOKEN_PATH = HERE / "sessions" / "ic_agent.json"
DEFAULT_BASE = "https://www.immersivecommons.com"


def base_url() -> str:
    return (os.environ.get("IC_BASE_URL") or DEFAULT_BASE).rstrip("/")


def _request(method: str, path: str, body: dict | None = None, timeout: float = 20.0):
    url = f"{base_url()}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            return e.code, {}
    except Exception as e:  # noqa: BLE001
        print(f"[ic_mint] network error reaching {url}: {e}", file=sys.stderr)
        raise


def login(scopes: list[str]) -> dict:
    status, grant = _request("POST", "/api/agent/signup/start", {"scopes": scopes,
                                                                 "client_name": "agent-room-join"})
    if status != 200 or not isinstance(grant, dict) or "device_code" not in grant:
        raise SystemExit(f"[ic_mint] start failed (HTTP {status}): {grant}")

    verify = grant.get("verify_url_complete") or grant.get("verify_url")
    user_code = grant.get("user_code")
    print("\n" + "=" * 64)
    print("  APPROVE THIS IN YOUR BROWSER (signed in to Immersive Commons AS YOU):")
    print(f"    open:  {verify}")
    if user_code:
        print(f"    code:  {user_code}")
    print("=" * 64 + "\n[ic_mint] waiting for you to approve ...", flush=True)

    device_code = grant["device_code"]
    interval = max(2, int(grant.get("interval") or 5))
    deadline = time.monotonic() + int(grant.get("expires_in") or 900)
    while True:
        if time.monotonic() >= deadline:
            raise SystemExit("[ic_mint] approval window (15 min) elapsed -- re-run.")
        time.sleep(interval)
        st, res = _request("GET", f"/api/agent/signup/poll?device_code={quote(device_code, safe='')}")
        if st == 410:
            raise SystemExit("[ic_mint] device code expired/used -- re-run.")
        state = (res or {}).get("status")
        if state == "pending":
            print("  ... still waiting", flush=True)
            continue
        if state == "cancelled":
            raise SystemExit("[ic_mint] you declined the request in the browser.")
        if state == "completed":
            token = res.get("agent_token")
            if not token:
                raise SystemExit("[ic_mint] completed but no token -- re-run.")
            return {
                "agent_token": token,
                "prefix": token[:12],
                "member_id": res.get("member_id"),
                "member_name": res.get("member_name"),
                "user_id": res.get("user_id"),
                "tier": res.get("tier"),
                "granted_scopes": res.get("granted_scopes") or [],
                "base_url": base_url(),
            }


def save(jar: dict) -> None:
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = TOKEN_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(jar, indent=2))
    tmp.replace(TOKEN_PATH)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Mint an IC rooms:join agent token (device-code).")
    ap.add_argument("--scopes", default="rooms:join", help="comma-separated (default rooms:join)")
    args = ap.parse_args(argv)
    scopes = [s.strip() for s in args.scopes.split(",") if s.strip()]
    jar = login(scopes)
    save(jar)
    print(f"\n[ic_mint] OK. token minted for member_id={jar.get('member_id')!r} "
          f"tier={jar.get('tier')!r} scopes={jar.get('granted_scopes')}")
    print(f"[ic_mint] saved -> {TOKEN_PATH}")
    if "rooms:join" not in (jar.get("granted_scopes") or []):
        print("[ic_mint] WARNING: rooms:join was NOT granted -- you may not be ic-member tier yet.",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
