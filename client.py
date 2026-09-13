"""
client.py
---------
A tiny command-line chat client so you can test the server with two terminals.

Run (in separate terminals):
    python client.py alice
    python client.py bob

Commands once running:
    /to <user>        set who you are messaging     e.g. /to bob
    <anything else>   send that text to current recipient
                      (a 'typing' event is sent automatically just before)
    /read <msg_id>    manually send a read receipt (usually automatic)
    /who              ask nothing; just prints your recipient
    /quit             leave

What you will SEE (the point of the demo):
    - end-to-end latency printed on every message you receive
    - 'X is typing...' before a message arrives
    - 'blue tick' (read) confirmations bouncing back to the sender
    - presence lines when someone goes online / offline
"""

import asyncio
import json
import sys
import time
import uuid

import websockets

URI = "ws://localhost:8765"


def now_ms():
    return int(time.time() * 1000)


# We remember message ids we RECEIVED so we can auto-send read receipts,
# and we keep 'to' as the current recipient.
state = {"to": None, "me": None}


async def receive_loop(ws):
    """Print everything the server pushes to us, and react to it."""
    async for raw in ws:
        msg = json.loads(raw)
        etype = msg.get("type")

        if etype == "chat":
            # measure how long the message took, end to end
            sent_at = msg.get("sent_at")
            e2e = (now_ms() - sent_at) if sent_at else "?"
            frm = msg["from"]
            print(
                f"\n[{frm}] {msg['text']}   (end-to-end {e2e}ms, msg_id={msg['msg_id']})"
            )

            # auto blue-tick: tell the sender we have seen it
            await ws.send(
                json.dumps(
                    {
                        "type": "read",
                        "to": frm,
                        "msg_id": msg["msg_id"],
                    }
                )
            )

        elif etype == "typing":
            print(f"\n... {msg['from']} is typing ...")

        elif etype == "read":
            print(f"\n[blue tick] {msg['from']} read your msg {msg['msg_id']}")

        elif etype == "presence":
            print(f"\n[presence] {msg['user']} is {msg['status']}")

        elif etype == "system":
            print(f"\n[system] {msg['text']}")

        print("> ", end="", flush=True)


async def send_loop(ws):
    """Read your typing from the keyboard and send events to the server."""
    while True:
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            break
        line = line.strip()
        if not line:
            continue

        if line == "/quit":
            await ws.close()
            break

        if line.startswith("/to "):
            state["to"] = line[4:].strip()
            print(f"[client] now messaging {state['to']}")
            continue

        if line == "/who":
            print(f"[client] current recipient: {state['to']}")
            continue

        if line.startswith("/read "):
            msg_id = line[6:].strip()
            await ws.send(
                json.dumps({"type": "read", "to": state["to"], "msg_id": msg_id})
            )
            continue

        # otherwise it is a chat message to the current recipient
        if not state["to"]:
            print("[client] set a recipient first:  /to bob")
            continue

        # 1) send a typing event first (rides the same socket)
        await ws.send(json.dumps({"type": "typing", "to": state["to"]}))

        # 2) then the actual chat message, stamped with send time + an id
        msg_id = uuid.uuid4().hex[:8]
        await ws.send(
            json.dumps(
                {
                    "type": "chat",
                    "to": state["to"],
                    "text": line,
                    "msg_id": msg_id,
                    "sent_at": now_ms(),
                }
            )
        )
        print(f"[client] sent msg_id={msg_id} to {state['to']}")


async def main():
    if len(sys.argv) < 2:
        print("usage: python client.py <your_user_id>")
        return
    state["me"] = sys.argv[1]

    async with websockets.connect(URI) as ws:
        # first thing: register so the server knows who we are
        await ws.send(json.dumps({"type": "register", "user_id": state["me"]}))
        print(f"[client] connected as {state['me']}. type /to <user> then chat.")
        print("> ", end="", flush=True)

        # run receiving and sending at the same time
        await asyncio.gather(receive_loop(ws), send_loop(ws))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, websockets.ConnectionClosed):
        print("\n[client] bye")
