"""
events.py
---------
Every feature rides the SAME pipe (the WebSocket) as a small JSON event.
The only thing that changes is the "type" field. That is the key idea:

    chat, typing, read receipt, presence  ->  all just events on one socket.

Message shapes (JSON) used in this project:

  client -> server:
    {"type":"register", "user_id":"alice"}
    {"type":"chat",   "to":"bob", "text":"hi", "msg_id":"...", "sent_at": <ms>}
    {"type":"typing", "to":"bob"}
    {"type":"read",   "to":"alice", "msg_id":"..."}   # 'I have seen your msg'

  server -> client:
    {"type":"chat",   "from":"alice", "text":"hi", "msg_id":"...", "sent_at":<ms>}
    {"type":"typing", "from":"alice"}
    {"type":"read",   "from":"bob", "msg_id":"..."}   # blue tick
    {"type":"presence","user":"alice","status":"online"|"offline"}
    {"type":"system", "text":"..."}                    # notices / errors
"""

import json
import logging
import time

import pending_store  # NEW: Redis-backed offline queue

log = logging.getLogger("chat.events")

# --- event type constants (avoid magic strings scattered everywhere) ---
REGISTER = "register"
CHAT = "chat"
TYPING = "typing"
READ = "read"
PRESENCE = "presence"
SYSTEM = "system"


def now_ms():
    """Current wall-clock time in milliseconds. Used for latency math."""
    return int(time.time() * 1000)


async def _send(websocket, payload):
    """Serialize a dict to JSON and push it down one socket."""
    await websocket.send(json.dumps(payload))


# ---------------------------------------------------------------------------
# CHAT: route one message from sender to the recipient (if on this server)
# ---------------------------------------------------------------------------
async def handle_chat(conn_mgr, sender_id, msg):
    to = msg.get("to")
    text = msg.get("text", "")
    msg_id = msg.get("msg_id")
    sent_at = msg.get("sent_at")  # when the client says it sent this (ms)

    server_recv = now_ms()
    # Latency from the client pressing enter until the server received it.
    in_latency = (server_recv - sent_at) if sent_at else None
    log.info(
        "CHAT recv   from=%s to=%s msg_id=%s  client->server=%sms",
        sender_id,
        to,
        msg_id,
        in_latency,
    )

    recipient_ws = conn_mgr.get_socket(to)

    if recipient_ws is None:
        # Recipient is OFFLINE -> park the message in their Redis queue.
        # It waits there until they reconnect (or the TTL expires).
        await pending_store.add_pending(
            to,
            {
                "type": CHAT,
                "from": sender_id,
                "text": text,
                "msg_id": msg_id,
                "sent_at": sent_at,
                "queued_at": now_ms(),  # when it went into the queue
            },
        )
        log.info("CHAT queued recipient=%s is OFFLINE, stored in Redis", to)
        sender_ws = conn_mgr.get_socket(sender_id)
        if sender_ws:
            await _send(
                sender_ws,
                {
                    "type": SYSTEM,
                    "text": f"{to} is offline. Message queued, will deliver on reconnect.",
                },
            )
        return

    # Recipient is online on THIS server -> push straight down their socket.
    await _send(
        recipient_ws,
        {
            "type": CHAT,
            "from": sender_id,
            "text": text,
            "msg_id": msg_id,
            "sent_at": sent_at,  # forwarded so receiver can measure end-to-end
            "server_fwd_at": now_ms(),  # when the server forwarded it
        },
    )
    log.info(
        "CHAT fwd    to=%s msg_id=%s  server processing=%sms",
        to,
        msg_id,
        now_ms() - server_recv,
    )


# ---------------------------------------------------------------------------
# DELIVER PENDING: called right after a user reconnects. Flush their queue.
# ---------------------------------------------------------------------------
async def deliver_pending(conn_mgr, user_id):
    """
    On reconnect: read everything waiting for this user, push it down their
    socket in order, then delete the queue so it is never delivered twice.
    """
    messages = await pending_store.get_pending(user_id)
    if not messages:
        return

    ws = conn_mgr.get_socket(user_id)
    if ws is None:
        # They vanished again in the split second after registering. Leave the
        # queue intact so it delivers next time.
        log.warning("DELIVER skip user=%s socket gone, keeping queue", user_id)
        return

    delivered = 0
    for msg in messages:
        msg["delivered_at"] = now_ms()
        # end-to-end latency = now minus when the sender originally sent it
        if msg.get("sent_at"):
            msg["offline_wait_ms"] = now_ms() - msg["sent_at"]
        await _send(ws, msg)
        delivered += 1

    # Only clear AFTER we have pushed them all. Deliver, then delete.
    await pending_store.clear_pending(user_id)
    log.info("DELIVER done user=%s delivered=%d, queue cleared", user_id, delivered)


# ---------------------------------------------------------------------------
# TYPING: forward a lightweight 'X is typing' event. No storage, fire & forget.
# ---------------------------------------------------------------------------
async def handle_typing(conn_mgr, sender_id, msg):
    to = msg.get("to")
    recipient_ws = conn_mgr.get_socket(to)
    if recipient_ws is None:
        # No point queuing a typing event. If they are offline, it is meaningless.
        log.debug("TYPING drop from=%s to=%s (recipient offline)", sender_id, to)
        return
    await _send(recipient_ws, {"type": TYPING, "from": sender_id})
    log.info("TYPING fwd  from=%s to=%s", sender_id, to)


# ---------------------------------------------------------------------------
# READ: the 'blue tick'. Recipient tells the sender 'I saw msg_id'.
# ---------------------------------------------------------------------------
async def handle_read(conn_mgr, sender_id, msg):
    to = msg.get("to")  # the ORIGINAL sender, who gets the blue tick
    msg_id = msg.get("msg_id")
    original_sender_ws = conn_mgr.get_socket(to)
    if original_sender_ws is None:
        log.debug("READ drop  reader=%s original_sender=%s offline", sender_id, to)
        return
    await _send(original_sender_ws, {"type": READ, "from": sender_id, "msg_id": msg_id})
    log.info(
        "READ fwd    reader=%s -> original_sender=%s msg_id=%s", sender_id, to, msg_id
    )


# ---------------------------------------------------------------------------
# PRESENCE: tell others that a user went online / offline.
# ---------------------------------------------------------------------------
async def broadcast_presence(conn_mgr, user_id, status):
    payload = {"type": PRESENCE, "user": user_id, "status": status}
    peers = conn_mgr.everyone_except(user_id)
    for peer_id, peer_ws in peers:
        try:
            await _send(peer_ws, payload)
        except Exception as e:
            # One dead peer must not stop us telling everyone else.
            log.debug("presence send failed to %s: %s", peer_id, e)
    log.info("PRESENCE    %s is now %s  (told %d peers)", user_id, status, len(peers))
