"""Headless test: start server, connect alice + bob, verify the full flow."""

import asyncio
import json
import time
import uuid

import websockets

from server import main as server_main

URI = "ws://localhost:8765"


async def recv_until(ws, want_type, timeout=3):
    async def _r():
        async for raw in ws:
            m = json.loads(raw)
            if m.get("type") == want_type:
                return m

    return await asyncio.wait_for(_r(), timeout)


async def drain(ws, n, timeout=3):
    out = []

    async def _r():
        async for raw in ws:
            out.append(json.loads(raw))
            if len(out) >= n:
                return

    try:
        await asyncio.wait_for(_r(), timeout)
    except asyncio.TimeoutError:
        pass
    return out


async def run():
    server = asyncio.create_task(server_main())
    await asyncio.sleep(0.5)

    alice = await websockets.connect(URI)
    await alice.send(json.dumps({"type": "register", "user_id": "alice"}))
    await recv_until(alice, "system")

    bob = await websockets.connect(URI)
    await bob.send(json.dumps({"type": "register", "user_id": "bob"}))
    await recv_until(bob, "system")

    # alice should have received bob's presence=online
    pres = await recv_until(alice, "presence")
    assert pres["user"] == "bob" and pres["status"] == "online", pres
    print("PASS presence online:", pres)

    # alice types, then sends a chat to bob
    await alice.send(json.dumps({"type": "typing", "to": "bob"}))
    typ = await recv_until(bob, "typing")
    assert typ["from"] == "alice", typ
    print("PASS typing event:", typ)

    mid = uuid.uuid4().hex[:8]
    await alice.send(
        json.dumps(
            {
                "type": "chat",
                "to": "bob",
                "text": "hi bob",
                "msg_id": mid,
                "sent_at": int(time.time() * 1000),
            }
        )
    )
    chat = await recv_until(bob, "chat")
    assert chat["text"] == "hi bob" and chat["msg_id"] == mid, chat
    e2e = int(time.time() * 1000) - chat["sent_at"]
    print(f"PASS chat delivered: '{chat['text']}' end-to-end ~{e2e}ms")

    # bob sends read receipt -> alice should get blue tick
    await bob.send(json.dumps({"type": "read", "to": "alice", "msg_id": mid}))
    tick = await recv_until(alice, "read")
    assert tick["from"] == "bob" and tick["msg_id"] == mid, tick
    print("PASS read receipt (blue tick):", tick)

    # offline case: alice messages carol (not connected) -> gets a system notice
    await alice.send(
        json.dumps(
            {
                "type": "chat",
                "to": "carol",
                "text": "hello?",
                "msg_id": "x",
                "sent_at": int(time.time() * 1000),
            }
        )
    )
    sysmsg = await recv_until(alice, "system")
    assert "offline" in sysmsg["text"].lower(), sysmsg
    print("PASS offline notice:", sysmsg["text"])

    # bob leaves -> alice should see presence offline
    await bob.close()
    pres_off = await recv_until(alice, "presence")
    assert pres_off["user"] == "bob" and pres_off["status"] == "offline", pres_off
    print("PASS presence offline:", pres_off)

    await alice.close()
    server.cancel()
    print("\nALL CHECKS PASSED")


asyncio.run(run())
