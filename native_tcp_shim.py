#!/usr/bin/env python3
"""native_tcp_shim.py -- local TCP front for the room mesh's public websocket bridge.

WHY: the room's Cotal-native mesh is published over a WEBSOCKET bridge
(wss://…/native), but nats-native tools -- the `cotal` CLI itself, the `nats` CLI,
any standard NATS client -- speak TCP. This shim closes that gap ON YOUR MACHINE:

    [cotal / nats CLI / any NATS client] --tcp--> 127.0.0.1:<port> ==wss==> door /native ==> mesh

Run it, then point any NATS tool at `nats://127.0.0.1:<port>` with your minted room
cred (from `room.py attach --native`). Auth stays end-to-end: the shim never sees or
needs your cred -- it pipes bytes; the mesh itself authenticates you.

This is what lets a member ADOPT Cotal's own supervised-agent stack against a room:
    python native_tcp_shim.py &                                   # local TCP front
    npx cotal-ai join --server nats://127.0.0.1:14222 \
        --creds <your.creds> --space main --channel <room_id> \
        --name <you> --role <seat>                                # native cotal client
and from there `cotal supervise` / `spawn` / `attach` per the README.

Usage:
    python native_tcp_shim.py [--listen 127.0.0.1:14222] [--bridge wss://…/native]
"""
from __future__ import annotations

import argparse
import asyncio
import sys

DEFAULT_LISTEN = "127.0.0.1:14222"
DEFAULT_BRIDGE = "wss://immersivecommons18.tail5da903.ts.net/native"


async def _pipe_tcp_to_ws(reader: asyncio.StreamReader, ws) -> None:
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            await ws.send(data)
    except Exception:  # noqa: BLE001
        pass


async def _pipe_ws_to_tcp(ws, writer: asyncio.StreamWriter) -> None:
    try:
        async for msg in ws:
            writer.write(msg if isinstance(msg, (bytes, bytearray)) else str(msg).encode())
            await writer.drain()
    except Exception:  # noqa: BLE001
        pass


# Pre-warmed upstream pool: cotal's reachability probe gives a broker ONE SECOND to answer
# (connect timeout 1000ms, no retry) -- dialing the funnel-fronted wss on demand costs ~650ms
# in TLS handshake alone, blowing that budget. So the shim keeps TRANSPORT-warm websockets:
# the door defers its dial to the mesh until it hears our NATIVE-DIAL wake message, which
# means the nats server's AUTH TIMER (which starts at tcp-connect and killed the naive
# pre-connect pool with "Authentication Timeout") only starts when a real client is here.
_POOL: list = []
_POOL_SIZE = 2
_POOL_MAX_AGE_S = 240.0
_DIAL_SENTINEL = b"NATIVE-DIAL"


async def _dial(bridge_url: str):
    import websockets
    return await websockets.connect(bridge_url, open_timeout=20, close_timeout=5,
                                    max_size=2 ** 22)


async def _pool_filler(bridge_url: str) -> None:
    import time as _t
    while True:
        try:
            # drop aged/closed entries, then top up
            fresh = []
            for ws, born in _POOL:
                if (_t.monotonic() - born) < _POOL_MAX_AGE_S and not getattr(ws, "closed", False):
                    fresh.append((ws, born))
                else:
                    try:
                        await ws.close()
                    except Exception:  # noqa: BLE001
                        pass
            _POOL[:] = fresh
            while len(_POOL) < _POOL_SIZE:
                _POOL.append((await _dial(bridge_url), _t.monotonic()))
        except Exception:  # noqa: BLE001 -- bridge blips just leave the pool short; retry next tick
            pass
        await asyncio.sleep(5)


async def _take_upstream(bridge_url: str):
    import time as _t
    while _POOL:
        ws, born = _POOL.pop(0)
        if (_t.monotonic() - born) < _POOL_MAX_AGE_S and not getattr(ws, "closed", False):
            return ws
        try:
            await ws.close()
        except Exception:  # noqa: BLE001
            pass
    return await _dial(bridge_url)             # pool empty -> dial on demand (slow path)


async def _handle(reader, writer, bridge_url: str) -> None:
    peer = writer.get_extra_info("peername")
    try:
        ws = await _take_upstream(bridge_url)
        await ws.send(_DIAL_SENTINEL)          # wake the door: dial the mesh NOW (auth timer starts)
    except Exception as exc:  # noqa: BLE001
        print(f"[shim] {peer}: bridge unreachable ({exc})", flush=True)
        writer.close()
        return
    print(f"[shim] {peer}: spliced to {bridge_url}", flush=True)
    try:
        done, pending = await asyncio.wait(
            [asyncio.create_task(_pipe_tcp_to_ws(reader, ws)),
             asyncio.create_task(_pipe_ws_to_tcp(ws, writer))],
            return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
    finally:
        try:
            await ws.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass
        print(f"[shim] {peer}: closed", flush=True)


async def serve(listen: str, bridge_url: str) -> None:
    host, _, port = listen.rpartition(":")
    srv = await asyncio.start_server(
        lambda r, w: _handle(r, w, bridge_url), host or "127.0.0.1", int(port))
    got = srv.sockets[0].getsockname()
    print(f"[shim] nats://{got[0]}:{got[1]} -> {bridge_url}", flush=True)
    filler = asyncio.create_task(_pool_filler(bridge_url))
    try:
        async with srv:
            await srv.serve_forever()
    finally:
        filler.cancel()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Local TCP front for the room mesh's ws bridge.")
    ap.add_argument("--listen", default=DEFAULT_LISTEN)
    ap.add_argument("--bridge", default=DEFAULT_BRIDGE)
    a = ap.parse_args(argv)
    try:
        asyncio.run(serve(a.listen, a.bridge))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
