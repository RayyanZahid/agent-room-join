#!/usr/bin/env python3
"""native_client.py -- NATS client for Cotal-native rooms (creds auth over the door's /native).

This is the transport David's design plan asked for, replacing the grant-path client's papered
seams with the real primitives (Cotal 0.11.3 auth-mode mesh):

  durable + replay   your minted cred carries its OWN pre-authorized replay consumer on the
                     space's chat stream -- a late joiner (or a reconnect) replays history
                     server-side; NO ready-beacons, NO "everyone online at once".
  push delivery      the consumer is bound with a deliver_subject under your private inbox,
                     so new turns are PUSHED the instant they land (no polling).
  presence           heartbeat your own key in the presence KV; read everyone's last state.
  attribution        you can ONLY publish as yourself (the subject carries your NKEY and the
                     broker enforces it) -- live messages are as trustworthy as committed ones.
  isolation          your cred subscribes ONLY your room's channel (server-side ACL).

Auth: the .creds bundle from `POST /rooms/<id>/native_creds` (IC bearer -> minted cred).
The server nonce is signed with the cred's ed25519 seed -- needs the `cryptography` package
(added to requirements.txt).

Transport: NATS protocol over a websocket to the door's `/native` path (a dumb ws<->tcp pipe;
the auth-mode nats itself has no ws listener yet). Everything here is protocol-level NATS --
if/when Cotal ships a native ws listener, only the URL changes.
"""
from __future__ import annotations

import asyncio
import base64
import json
import secrets as _secrets
import time
from typing import Optional


# --- creds file parsing + nkeys signing --------------------------------------
def parse_creds(creds_text: str) -> tuple[str, str]:
    """Extract (jwt, seed) from a NATS .creds file. Never logs either."""
    try:
        jwt = creds_text.split("-----BEGIN NATS USER JWT-----")[1] \
                        .split("------END NATS USER JWT------")[0].strip()
    except IndexError:
        raise ValueError("not a creds file: missing USER JWT block")
    seed = ""
    for ln in creds_text.splitlines():
        ln = ln.strip()
        if ln.startswith("SU") and " " not in ln and len(ln) > 40:
            seed = ln
            break
    if not seed:
        raise ValueError("not a creds file: missing user nkey seed")
    return jwt, seed


def jwt_claims(jwt: str) -> dict:
    p = jwt.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))


def _seed_raw(seed: str) -> bytes:
    """nkeys seed -> 32-byte ed25519 private seed (base32; 2-byte type prefix + 2-byte CRC)."""
    raw = base64.b32decode(seed + "=" * (-len(seed) % 8))
    body = raw[2:-2]
    if len(body) != 32:
        raise ValueError(f"unexpected nkeys seed payload length {len(body)}")
    return body


def sign_nonce(seed: str, nonce: str) -> str:
    """Sign the server nonce; return NATS-style url-safe base64 (unpadded)."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    key = Ed25519PrivateKey.from_private_bytes(_seed_raw(seed))
    return base64.urlsafe_b64encode(key.sign(nonce.encode())).decode().rstrip("=")


# --- byte-level NATS frame parser (reply-aware; payload-safe) -----------------
def parse_frames_bytes(buf: bytes):
    """Parse complete NATS frames from a byte buffer. Events:
        ("msg", subject, sid, reply, payload_bytes) | ("ping",) | ("pong",) |
        ("info", json_str) | ("err", text)
    HMSG payloads are returned with headers stripped. Partial frames stay in the remainder."""
    events = []
    i, n = 0, len(buf)
    while True:
        nl = buf.find(b"\r\n", i)
        if nl == -1:
            break
        line = buf[i:nl]
        up = line.upper()
        if up.startswith(b"MSG ") or up.startswith(b"HMSG "):
            parts = line.split()
            is_h = up.startswith(b"HMSG")
            try:
                total = int(parts[-1])
                hdr = int(parts[-2]) if is_h else 0
            except (ValueError, IndexError):
                i = nl + 2
                continue
            start, end = nl + 2, nl + 2 + total
            if end + 2 > n:
                break                                     # payload not fully buffered
            base = 5 if is_h else 4                        # tokens incl. reply
            reply = parts[3].decode() if len(parts) == base + 1 else ""
            events.append(("msg", parts[1].decode(), parts[2].decode(), reply,
                           bytes(buf[start + hdr:end])))
            i = end + 2
        elif up == b"PING":
            events.append(("ping",)); i = nl + 2
        elif up == b"PONG":
            events.append(("pong",)); i = nl + 2
        elif up.startswith(b"INFO"):
            events.append(("info", line[5:].decode("utf-8", "replace"))); i = nl + 2
        elif up.startswith(b"-ERR"):
            events.append(("err", line[5:].decode("utf-8", "replace").strip())); i = nl + 2
        else:
            i = nl + 2                                    # +OK / unknown -> skip
    return events, buf[i:]


def ack_stream_seq(reply: str) -> Optional[int]:
    """Stream sequence from a JetStream delivery's $JS.ACK reply subject (v1: 9 tokens,
    sseq at index 5; v2 with domain: 11+ tokens, sseq at index 7). None if not an ACK."""
    t = reply.split(".")
    if len(t) < 9 or t[0] != "$JS" or t[1] != "ACK":
        return None
    try:
        return int(t[5]) if len(t) == 9 else int(t[7])
    except ValueError:
        return None


# --- the client ---------------------------------------------------------------
class NativeClient:
    """Persistent creds-authenticated NATS client over the door's /native websocket.
    One reader task dispatches: PING->PONG, MSG->per-sid queues. request() rides a private
    inbox under the cred's OWN prefix (`_INBOX_<NKEY>.`) -- the default `_INBOX.` is DENIED
    by the cred's ACL (default-deny; learned live)."""

    def __init__(self, ws_url: str, creds_text: str, *, name: str = "room-native",
                 timeout: float = 20.0):
        self.ws_url = ws_url
        self.jwt, self._seed = parse_creds(creds_text)
        self.nkey = jwt_claims(self.jwt).get("sub", "")
        if not self.nkey:
            raise ValueError("cred JWT has no sub (user nkey)")
        self.inbox_prefix = f"_INBOX_{self.nkey}"
        self.name = name
        self.timeout = timeout
        self._ws = None
        self._buf = b""
        self._sid = 0
        self._queues: dict[str, asyncio.Queue] = {}
        self._reader: Optional[asyncio.Task] = None
        self._pong = asyncio.Event()
        self.last_err: str = ""

    # -- lifecycle --------------------------------------------------------------
    async def connect(self):
        import websockets
        self._ws = await websockets.connect(self.ws_url, open_timeout=self.timeout,
                                            close_timeout=5, max_size=2 ** 22)
        # Wake the door (it defers the mesh dial until a first message so pooled transports
        # don't age out the server's auth window; the sentinel is eaten, never forwarded).
        await self._ws.send(b"NATIVE-DIAL")
        # INFO carries the auth nonce; sign it with the cred's seed.
        raw = await asyncio.wait_for(self._ws.recv(), self.timeout)
        self._buf += raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode()
        events, self._buf = parse_frames_bytes(self._buf)
        info = next((e for e in events if e[0] == "info"), None)
        if info is None:
            raise RuntimeError("native mesh did not greet with INFO")
        nonce = json.loads(info[1]).get("nonce", "")
        connect = {"protocol": 1, "verbose": False, "pedantic": False, "headers": False,
                   "lang": "py-room-native", "version": "1.0.0", "name": self.name,
                   "jwt": self.jwt, "sig": sign_nonce(self._seed, nonce)}
        await self._ws.send(("CONNECT " + json.dumps(connect) + "\r\nPING\r\n").encode())
        self._reader = asyncio.create_task(self._read_loop())
        # PONG = authenticated; -ERR (authorization) surfaces via last_err
        try:
            await asyncio.wait_for(self._pong.wait(), self.timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f"native mesh auth failed: {self.last_err or 'no PONG'}")

    async def close(self):
        if self._reader is not None:
            self._reader.cancel()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:  # noqa: BLE001
                pass

    async def _read_loop(self):
        try:
            async for raw in self._ws:
                self._buf += raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode()
                events, self._buf = parse_frames_bytes(self._buf)
                for ev in events:
                    if ev[0] == "msg":
                        q = self._queues.get(ev[2])
                        if q is not None:
                            q.put_nowait(ev)
                    elif ev[0] == "ping":
                        await self._ws.send(b"PONG\r\n")
                    elif ev[0] == "pong":
                        self._pong.set()
                    elif ev[0] == "err":
                        self.last_err = ev[1]
        except Exception:  # noqa: BLE001 -- a closed socket ends the loop; caller reconnects
            pass
        finally:
            # unblock every waiter: a dead socket must never leave q.get() hanging forever
            # (that would silently strand the listener instead of triggering its reconnect).
            for q in list(self._queues.values()):
                q.put_nowait(("closed", "", "", "", b""))

    # -- core verbs ---------------------------------------------------------------
    async def subscribe(self, subject: str) -> tuple[str, asyncio.Queue]:
        self._sid += 1
        sid = str(self._sid)
        q: asyncio.Queue = asyncio.Queue()
        self._queues[sid] = q
        await self._ws.send(f"SUB {subject} {sid}\r\n".encode())
        return sid, q

    async def publish(self, subject: str, payload: bytes, reply: str = ""):
        head = f"PUB {subject} {reply + ' ' if reply else ''}{len(payload)}\r\n".encode()
        await self._ws.send(head + payload + b"\r\n")

    async def flush(self, timeout: float = 5.0):
        """PING/PONG round trip -- guarantees the server has processed prior sends. This is
        the honest replacement for the old client's `sleep(0.4) and hope` publish."""
        self._pong.clear()
        await self._ws.send(b"PING\r\n")
        await asyncio.wait_for(self._pong.wait(), timeout)

    async def request(self, subject: str, payload: bytes, timeout: float = 5.0) -> bytes:
        inbox = f"{self.inbox_prefix}.rq.{_secrets.token_hex(4)}"
        sid, q = await self.subscribe(inbox)
        try:
            await self.publish(subject, payload, reply=inbox)
            ev = await asyncio.wait_for(q.get(), timeout)
            if ev[0] == "closed":
                raise RuntimeError("connection closed during request")
            return ev[4]
        finally:
            self._queues.pop(sid, None)

    # -- JetStream / room helpers ---------------------------------------------------
    async def ensure_chat_consumer(self, *, space: str, channel: str,
                                   deliver_subject: str, start_seq: Optional[int] = None):
        """(Re)bind the cred's OWN replay consumer as a PUSH consumer to `deliver_subject`.
        start_seq=None -> full history replay (late join); N -> resume from N (reconnect).
        The consumer name + filter are pinned by the cred's ACL -- we cannot (and need not)
        read anything else."""
        stream = f"CHAT_{space}"
        cname = f"chathist_local-{self.nkey}"
        filt = f"cotal.{space}.chat.*.*.{channel}"
        try:
            await self.request(f"$JS.API.CONSUMER.DELETE.{stream}.{cname}", b"", timeout=3)
        except asyncio.TimeoutError:
            pass                                          # absent consumer is fine
        cfg = {"name": cname, "ack_policy": "none", "filter_subject": filt,
               "deliver_subject": deliver_subject,
               "inactive_threshold": 300_000_000_000}     # 5 min: dies after we disconnect
        if start_seq and start_seq > 0:
            cfg.update({"deliver_policy": "by_start_sequence", "opt_start_seq": start_seq})
        else:
            cfg["deliver_policy"] = "all"
        req = {"stream_name": stream, "config": cfg}
        resp = await self.request(f"$JS.API.CONSUMER.CREATE.{stream}.{cname}.{filt}",
                                  json.dumps(req).encode(), timeout=8)
        doc = json.loads(resp.decode("utf-8", "replace"))
        if doc.get("error"):
            raise RuntimeError(f"consumer create failed: {doc['error']}")
        return doc

    def chat_subject(self, space: str, channel: str) -> str:
        """The ONLY chat subject this cred may publish: its own-NKEY room subject."""
        return f"cotal.{space}.chat.local.{self.nkey}.{channel}"

    async def publish_chat(self, *, space: str, channel: str, text: str, sender: str):
        # COTAL WIRE SCHEMA (types.d.ts CotalMessage): parts[] is what native cotal clients
        # render -- a message without it crashes their console (hit live). `text` is kept as a
        # legacy mirror so older room.py listeners still read us.
        body = json.dumps({"id": _secrets.token_hex(16), "ts": int(time.time() * 1000),
                           "space": space, "from": {"id": f"local.{self.nkey}", "name": sender},
                           "channel": channel,
                           "parts": [{"kind": "text", "text": text}],
                           "text": text}).encode()
        await self.publish(self.chat_subject(space, channel), body)
        await self.flush()

    # -- presence -------------------------------------------------------------------
    async def presence_put(self, *, space: str, doc: dict):
        await self.publish(f"$KV.cotal_presence_{space}.local.{self.nkey}",
                           json.dumps(doc).encode())

    async def presence_snapshot(self, *, space: str, settle_s: float = 1.5) -> list[dict]:
        """Everyone's LAST presence heartbeat: ephemeral last-per-subject push consumer over
        the presence KV stream (create allowed by every agent cred; auto-expires)."""
        stream = f"KV_cotal_presence_{space}"
        cname = f"who-{_secrets.token_hex(4)}"
        filt = f"$KV.cotal_presence_{space}.>"
        deliver = f"{self.inbox_prefix}.who.{_secrets.token_hex(4)}"
        sid, q = await self.subscribe(deliver)
        req = {"stream_name": stream,
               "config": {"name": cname, "ack_policy": "none", "filter_subject": filt,
                          "deliver_policy": "last_per_subject", "deliver_subject": deliver,
                          "inactive_threshold": 10_000_000_000}}
        resp = await self.request(f"$JS.API.CONSUMER.CREATE.{stream}.{cname}.{filt}",
                                  json.dumps(req).encode(), timeout=8)
        doc = json.loads(resp.decode("utf-8", "replace"))
        if doc.get("error"):
            raise RuntimeError(f"presence consumer failed: {doc['error']}")
        out = []
        try:
            while True:
                ev = await asyncio.wait_for(q.get(), settle_s)
                try:
                    row = json.loads(ev[4].decode("utf-8", "replace"))
                except ValueError:
                    continue
                row["_key"] = ev[1]                        # $KV.<bucket>.<key>
                out.append(row)
        except asyncio.TimeoutError:
            pass                                           # quiet = snapshot complete
        finally:
            self._queues.pop(sid, None)
        return out


__all__ = ["NativeClient", "parse_creds", "jwt_claims", "sign_nonce",
           "parse_frames_bytes", "ack_stream_seq"]
