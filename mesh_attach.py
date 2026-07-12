#!/usr/bin/env python3
"""mesh_attach.py -- member-side mesh-join client (piece #3).

The half that runs on a MEMBER'S OWN machine. Given their IC agent token, the broker's HTTP
base, and the public wss door, it:

  1. fetch_grant()  -- POST /rooms/<id>/mesh_grant with the IC bearer, get the signed grant,
  2. door_url()     -- fold the grant into the door's wss URL (?room=..&grant=<b64>),
  3. probe_mesh()   -- connect THROUGH the door and run a minimal NATS-over-websocket
                       handshake + a SUB/PUB round-trip, proving the member actually reached
                       the room's live mesh end-to-end (grant accepted, relay working).

Nothing here needs the tailnet or raw NATS creds: the IC token + the public door are the
whole surface. door_url() is pure and round-trips exactly with mesh_door.parse_grant_query
(hermetically tested). fetch_grant/probe_mesh do real IO and are exercised in the live ic18
proof, not the unit suite.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import urllib.request
from pathlib import Path
from typing import Optional

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))


class MeshAttachError(Exception):
    """A member-side attach failure (grant refused, door refused, mesh unreachable)."""


# --- 1. fetch the grant from the broker -------------------------------------
def fetch_grant(broker_base_url: str, room_id: str, ic_token: str, *, timeout: float = 10.0) -> dict:
    """POST /rooms/<id>/mesh_grant with the IC bearer; return the grant payload dict.

    Raises MeshAttachError on any non-200 (surfacing the broker's reason so a member sees
    'wrong_scope' / 'not_a_member' / 'not_live', not a bare HTTP error)."""
    url = f"{broker_base_url.rstrip('/')}/rooms/{room_id}/mesh_grant"
    req = urllib.request.Request(url, method="POST",
                                 headers={"Authorization": f"Bearer {ic_token}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:  # noqa: PERF203
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            body = {"reason": f"http_{e.code}"}
        raise MeshAttachError(f"grant refused ({e.code}): {body.get('reason')}") from e
    except Exception as e:  # noqa: BLE001
        raise MeshAttachError(f"broker unreachable: {e}") from e
    grant = body.get("grant")
    if not isinstance(grant, dict):
        raise MeshAttachError(f"broker returned no grant: {body.get('reason', body)}")
    return grant


# --- 2. build the door URL (pure -- round-trips with mesh_door) --------------
def door_url(door_base_url: str, grant: dict) -> str:
    """Fold a grant into the door's wss URL: <door>?room=<room_id>&grant=<urlsafe-b64-json>.
    Pure + inverse of mesh_door.parse_grant_query. Uses the grant's OWN room_id so the URL
    can't disagree with the signed binding."""
    room = grant["room_id"]
    b64 = base64.urlsafe_b64encode(json.dumps(grant).encode("utf-8")).decode("ascii").rstrip("=")
    sep = "&" if "?" in door_base_url else "?"
    return f"{door_base_url}{sep}room={room}&grant={b64}"


# --- 3. prove the mesh is reachable through the door ------------------------
async def probe_mesh(door_ws_url: str, *, subject: str = "attach.probe", timeout: float = 10.0) -> dict:
    """Connect through the door and run a minimal NATS-over-ws handshake + SUB/PUB round-trip.
    Returns {"ok": True, "echo": <payload>} on a full round-trip. Raises MeshAttachError with
    the door's close reason if the grant is refused or the mesh is unreachable.

    NATS wire protocol (text frames): server greets with INFO; we CONNECT, SUB, PUB, then read
    back our own MSG -- proof the member is a live participant on the room's mesh."""
    import websockets

    payload = "hello-from-member"

    def _txt(frame) -> str:
        # NATS-over-websocket uses BINARY frames; normalize to str for the text protocol.
        return frame.decode("utf-8", "replace") if isinstance(frame, (bytes, bytearray)) else str(frame)

    try:
        async with websockets.connect(door_ws_url, subprotocols=["nats"],
                                      open_timeout=timeout, close_timeout=timeout) as ws:
            info = _txt(await asyncio.wait_for(ws.recv(), timeout))   # INFO {...}
            if not info.startswith("INFO"):
                raise MeshAttachError(f"unexpected greeting (not NATS INFO): {info[:80]}")
            # send as bytes -- NATS ws expects binary frames
            await ws.send(b'CONNECT {"verbose":false,"pedantic":false}\r\n')
            await ws.send(f"SUB {subject} 1\r\n".encode())
            await ws.send(f"PUB {subject} {len(payload)}\r\n{payload}\r\n".encode())
            # read frames until our MSG comes back (tolerate interleaved PING)
            deadline = asyncio.get_event_loop().time() + timeout
            buf = ""
            while asyncio.get_event_loop().time() < deadline:
                buf += _txt(await asyncio.wait_for(ws.recv(), timeout))
                if "PING\r\n" in buf:
                    await ws.send(b"PONG\r\n")
                    buf = buf.replace("PING\r\n", "")
                if f"MSG {subject}" in buf and payload in buf:
                    return {"ok": True, "echo": payload, "subject": subject}
            raise MeshAttachError("no MSG round-trip within timeout (door up but mesh silent)")
    except websockets.exceptions.ConnectionClosed as e:
        raise MeshAttachError(f"door closed connection: {e.code} {getattr(e, 'reason', '')}") from e
    except MeshAttachError:
        raise
    except Exception as e:  # noqa: BLE001 -- incl. InvalidStatus/InvalidStatusCode across versions
        raise MeshAttachError(f"attach failed: {type(e).__name__}: {e}") from e


# --- CLI: the full member flow ----------------------------------------------
def _main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Member-side mesh-join: fetch grant + attach.")
    ap.add_argument("--broker-url", required=True, help="e.g. http://100.106.25.37:8787")
    ap.add_argument("--door-url", required=True, help="e.g. wss://rooms.example.ic/mesh")
    ap.add_argument("--room", required=True)
    ap.add_argument("--token", required=True, help="the member's IC agent token")
    ap.add_argument("--probe", action="store_true", help="also run the NATS round-trip probe")
    args = ap.parse_args(argv)
    try:
        grant = fetch_grant(args.broker_url, args.room, args.token)
        url = door_url(args.door_url, grant)
        print(json.dumps({"grant_ok": True, "cotal_name": grant.get("cotal_name"),
                          "expires_at_iso": grant.get("expires_at_iso"), "door_url": url}))
        if args.probe:
            res = asyncio.run(probe_mesh(url))
            print(json.dumps({"probe": res}))
    except MeshAttachError as e:
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
