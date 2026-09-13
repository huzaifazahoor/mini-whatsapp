# Mini WhatsApp — building a chat system step by step

A real-time chat backend built from scratch to understand how messaging systems
actually work: WebSockets, an in-memory socket map, and a Redis-backed offline
queue. Every step is small, tested, and printed to the console so you can watch
messages and latency move through the system.

## What works so far

**Step 1 — real-time chat (single server)**
- Chat: A sends, B receives, all through one server (no direct A-to-B)
- Typing indicator ("B is typing...")
- Read receipt (the blue tick)
- Presence (online / offline)
- Millisecond latency printed for every message

**Step 2 — offline messages (Redis)**
- If the recipient is offline, the message is parked in a Redis queue
- One queue per receiver: `pending:<user_id>` (a Redis list)
- On reconnect: deliver everything in order, then delete the queue
- TTL safety net: a forgotten queue auto-expires (default 30 days)
- No duplicate delivery on repeated reconnects

## The core idea

Every feature rides the **same pipe** (the WebSocket) as a small JSON event.
Only the `"type"` field changes: `chat`, `typing`, `read`, `presence`. Build the
pipe once and the features fall out of it.

Four stores, each doing one job:

| Store | Holds | Why |
|-------|-------|-----|
| In-memory dict (per server) | `user_id -> live socket` | A socket is a live connection tied to one machine; it can't go in Redis |
| Redis list | pending messages for offline users | Fast append/read, cheap delete, built-in TTL |
| device (later) | full chat history | WhatsApp keeps history on your phone, not its servers |
| Postgres (later) | user accounts, contacts, groups | Durable data you query normally |

## Project layout

```
server.py             # WebSocket server + main connection loop
connection_manager.py # the LOCAL in-memory socket map
events.py             # handlers: chat, typing, read, presence, deliver_pending
pending_store.py      # all Redis logic for the offline queue
client.py             # command-line test client
docker-compose.yml    # runs Redis (with AOF disk persistence)
smoke_test.py         # automated test for step 1
smoke_test_offline.py # automated test for step 2 (needs Redis running)
requirements.txt
```

## Setup

```bash
python -m venv venv
# Windows: venv\Scripts\activate    macOS/Linux: source venv/bin/activate
pip install -r requirements.txt

docker compose up -d      # start Redis
```

## Run it

```bash
# terminal 1
python server.py

# terminal 2
python client.py alice

# terminal 3
python client.py bob
```

In alice's terminal:
```
/to bob
hello bob
```

Client commands: `/to <user>` set recipient, then type to send, `/quit` to leave.

## See the offline queue for yourself

1. Start the server and connect only **alice**.
2. `/to carol` then send a message. Carol isn't connected, so it gets queued.
3. Look inside Redis while she's offline:
   ```bash
   docker exec -it whatsapp-redis redis-cli
   LRANGE pending:carol 0 -1   # the waiting messages
   TTL   pending:carol         # seconds until auto-expiry
   ```
4. Now connect **carol** (`python client.py carol`). All queued messages land
   at once, each showing how long it waited.
5. Run `LRANGE pending:carol 0 -1` again — the queue is empty (delivered, then cleared).

## Run the automated tests

```bash
python smoke_test.py          # step 1: chat, typing, read, presence, offline notice
python smoke_test_offline.py  # step 2: queue -> deliver on reconnect -> clear -> no duplicates
```

## Design decisions (the interesting part)

- **No A-to-B direct.** Everything goes A -> server -> B. The server is the switchboard.
- **Socket map stays local.** Each server keeps its own `user_id -> socket` dict in memory.
- **One pipe, many event types.** Features differ only by the `type` field.
- **Redis list for offline, not a SQL table.** Messages are short-lived
  (deliver then delete). Fast append/read + TTL fit better than durable rows.
- **AOF persistence on.** Trade-off: if Redis crashes, undelivered messages
  survive a restart. Costs a little speed, worth it for a chat queue.
- **TTL is a real trade-off.** Longer = better delivery but more memory;
  shorter = cheaper but messages can expire before the user returns.

## Not built yet (next steps)

1. **Cross-server routing** — many servers behind a load balancer. Sender's
   server looks up the receiver in shared Redis, then **Redis Pub/Sub** carries
   the message to the server holding that receiver's socket.
2. **Presence cleanup** — heartbeats/TTL so a phone that dies silently stops
   showing "online".
3. **Groups** — fan-out over the member list (fan-out on write for small
   groups, fan-out on read for huge ones).
4. **Self-message guard** — don't echo a message back to its own sender.

## Notes

- Presence is broadcast to everyone here for simplicity. Real WhatsApp only
  tells your contacts.
- You may see a harmless `MAINT_NOTIFICATIONS` warning if your redis-py client
  is newer than the Redis server. It doesn't affect anything.