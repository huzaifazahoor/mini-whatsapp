"""
server.py
---------
The WebSocket server. This is the 'switchboard operator' in the middle.
No user talks to another user directly. Everything goes:

        client A  --socket-->  SERVER  --socket-->  client B

The server:
  1. accepts a socket,
  2. waits for a 'register' event to learn who the user is,
  3. stores user_id -> socket in the local ConnectionManager,
  4. loops on incoming events and routes each one by its "type",
  5. cleans up (unregister + presence offline) when the socket drops.

Run:  python server.py
"""

import asyncio
import json
import logging

import websockets

import events
from connection_manager import ConnectionManager

# --- logging: millisecond timestamps so we can eyeball latency ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d  %(name)-14s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("chat.server")

HOST = "localhost"
PORT = 8765

# One shared connection manager for this single server process.
conn_mgr = ConnectionManager()


async def handle_connection(websocket):
    """Runs once per connected client, for the whole life of that socket."""
    user_id = None
    log.info("SOCKET open  (waiting for register event)")
    try:
        async for raw in websocket:
            # Every frame is a JSON string. Parse it, then route by type.
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("bad JSON: %r", raw)
                continue

            etype = msg.get("type")

            # ---- first message must be 'register' so we know who this is ----
            if etype == events.REGISTER:
                user_id = msg.get("user_id")
                conn_mgr.register(user_id, websocket)
                # tell the newcomer who is already online
                await websocket.send(
                    json.dumps(
                        {
                            "type": events.SYSTEM,
                            "text": f"registered as {user_id}. online: {conn_mgr.online_users()}",
                        }
                    )
                )
                # tell everyone else this user just came online
                await events.broadcast_presence(conn_mgr, user_id, "online")
                # flush any messages that arrived while they were offline
                await events.deliver_pending(conn_mgr, user_id)
                continue

            # ignore anything sent before registering
            if user_id is None:
                await websocket.send(
                    json.dumps(
                        {
                            "type": events.SYSTEM,
                            "text": "please register first",
                        }
                    )
                )
                continue

            # ---- route the rest by type. same pipe, different events ----
            if etype == events.CHAT:
                await events.handle_chat(conn_mgr, user_id, msg)
            elif etype == events.TYPING:
                await events.handle_typing(conn_mgr, user_id, msg)
            elif etype == events.READ:
                await events.handle_read(conn_mgr, user_id, msg)
            else:
                log.warning("unknown event type=%r from=%s", etype, user_id)

    except websockets.ConnectionClosed:
        # Normal: the client went away (closed app, network died, etc.)
        log.info("SOCKET closed user=%s", user_id)
    finally:
        # Cleanup is critical. If we skip this, the map keeps a dead socket
        # and we would wrongly think the user is still online.
        if user_id is not None:
            conn_mgr.unregister(user_id)
            await events.broadcast_presence(conn_mgr, user_id, "offline")


async def main():
    log.info("starting chat server on ws://%s:%d", HOST, PORT)
    async with websockets.serve(handle_connection, HOST, PORT):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("server stopped")
