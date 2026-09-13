"""Test offline flow: message to offline carol -> Redis -> deliver on reconnect -> queue cleared."""

import asyncio
import json
import time
import uuid

import redis.asyncio as aioredis
import websockets

from server import main as server_main

URI = "ws://localhost:8765"


async def recv_type(ws, want, timeout=3):
    async def _r():
        async for raw in ws:
            m = json.loads(raw)
            if m.get("type") == want:
                return m

    return await asyncio.wait_for(_r(), timeout)


async def run():
    # clean slate
    r = aioredis.Redis(decode_responses=True)
    await r.delete("pending:carol")

    server = asyncio.create_task(server_main())
    await asyncio.sleep(0.5)

    # alice connects
    alice = await websockets.connect(URI)
    await alice.send(json.dumps({"type": "register", "user_id": "alice"}))
    await recv_type(alice, "system")

    # alice messages carol, who is OFFLINE -> should be queued in Redis
    mid = uuid.uuid4().hex[:8]
    await alice.send(
        json.dumps(
            {
                "type": "chat",
                "to": "carol",
                "text": "hey carol, you were offline",
                "msg_id": mid,
                "sent_at": int(time.time() * 1000),
            }
        )
    )
    sysmsg = await recv_type(alice, "system")
    assert "queued" in sysmsg["text"].lower(), sysmsg
    print("PASS offline -> queued:", sysmsg["text"])

    # verify it is actually sitting in Redis
    qlen = await r.llen("pending:carol")
    ttl = await r.ttl("pending:carol")
    assert qlen == 1, f"expected 1 in queue, got {qlen}"
    assert ttl > 0, f"expected a TTL, got {ttl}"
    print(f"PASS message in Redis: queue_len={qlen}, ttl={ttl}s")

    # send a second one while still offline -> queue grows to 2
    await alice.send(
        json.dumps(
            {
                "type": "chat",
                "to": "carol",
                "text": "second message",
                "msg_id": uuid.uuid4().hex[:8],
                "sent_at": int(time.time() * 1000),
            }
        )
    )
    await recv_type(alice, "system")
    qlen = await r.llen("pending:carol")
    assert qlen == 2, f"expected 2, got {qlen}"
    print(f"PASS second queued: queue_len={qlen}")

    # NOW carol connects -> should receive BOTH messages, in order
    carol = await websockets.connect(URI)
    await carol.send(json.dumps({"type": "register", "user_id": "carol"}))

    first = await recv_type(carol, "chat")
    second = await recv_type(carol, "chat")
    assert first["text"] == "hey carol, you were offline", first
    assert second["text"] == "second message", second
    print(
        f"PASS delivered on reconnect (in order): '{first['text']}' | '{second['text']}'"
    )
    print(f"     offline_wait on first msg: {first.get('offline_wait_ms')}ms")

    # queue must be CLEARED after delivery
    await asyncio.sleep(0.2)
    qlen_after = await r.llen("pending:carol")
    assert qlen_after == 0, f"expected empty queue, got {qlen_after}"
    print(f"PASS queue cleared after delivery: queue_len={qlen_after}")

    # reconnect carol again -> should get NOTHING (no double delivery)
    await carol.close()
    await asyncio.sleep(0.2)
    carol2 = await websockets.connect(URI)
    await carol2.send(json.dumps({"type": "register", "user_id": "carol"}))
    got_dup = False
    try:
        await recv_type(carol2, "chat", timeout=1.5)
        got_dup = True
    except asyncio.TimeoutError:
        pass
    assert not got_dup, "carol got duplicate messages on second reconnect!"
    print("PASS no duplicate delivery on second reconnect")

    await alice.close()
    await carol2.close()
    server.cancel()
    print("\nALL OFFLINE-QUEUE CHECKS PASSED")


asyncio.run(run())
