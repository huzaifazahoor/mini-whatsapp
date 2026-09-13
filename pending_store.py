"""
pending_store.py
----------------
All Redis logic for the OFFLINE pending queue lives here, in one place.

The design we reasoned out:
  - One list per receiver, keyed by their user_id:   pending:<user_id>
  - Each list item is one full message (as JSON).
  - Offline write:  RPUSH the message onto the receiver's list + set a TTL.
  - Reconnect:      read the whole list, deliver in order, then DELETE it.
  - TTL is the safety net: if the user never comes back, Redis auto-expires
    the list so it does not live in memory forever.

Why a Redis LIST and not a DB table?
  Fast append, fast read-all, cheap delete, and built-in TTL. These messages
  are short-lived (deliver then delete), so a durable SQL table is overkill.
  AOF persistence (set in docker-compose) is what keeps them safe if Redis
  restarts.
"""

import json
import logging

import redis.asyncio as redis

log = logging.getLogger("chat.pending")

REDIS_HOST = "localhost"
REDIS_PORT = 6379

# How long an undelivered queue is allowed to live before Redis drops it.
# Trade-off: longer = better delivery but more memory; shorter = cheaper but
# more dropped messages. 30 days here. Lower it to test expiry quickly.
PENDING_TTL_SECONDS = 60 * 60 * 24 * 30

_client = None


def _key(user_id):
    """The Redis key that holds one user's waiting messages."""
    return f"pending:{user_id}"


async def get_client():
    """One shared async Redis client for the whole server process."""
    global _client
    if _client is None:
        # decode_responses=True so we get str back, not bytes.
        _client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        await _client.ping()  # fail fast if Redis is not up
        log.info("connected to Redis at %s:%d", REDIS_HOST, REDIS_PORT)
    return _client


async def add_pending(user_id, message):
    """
    Append one message to the receiver's queue and (re)set the TTL.
    Called when the recipient is OFFLINE.
    """
    client = await get_client()
    key = _key(user_id)
    # RPUSH appends to the end of the list -> messages stay in arrival order.
    await client.rpush(key, json.dumps(message))
    # Refresh the TTL every time we add, so an active-but-offline queue
    # does not expire mid-way. Countdown restarts on each new message.
    await client.expire(key, PENDING_TTL_SECONDS)
    length = await client.llen(key)
    log.info(
        "PENDING add user=%s  queue_len=%d  ttl=%ds",
        user_id,
        length,
        PENDING_TTL_SECONDS,
    )


async def get_pending(user_id):
    """Return every waiting message for a user, in order, as a list of dicts."""
    client = await get_client()
    key = _key(user_id)
    # LRANGE 0 -1 = the whole list.
    raw_items = await client.lrange(key, 0, -1)
    messages = [json.loads(item) for item in raw_items]
    log.info("PENDING read user=%s  found=%d", user_id, len(messages))
    return messages


async def clear_pending(user_id):
    """Delete the user's queue after their messages have been delivered."""
    client = await get_client()
    key = _key(user_id)
    await client.delete(key)
    log.info("PENDING clear user=%s  (queue deleted)", user_id)
