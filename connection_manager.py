"""
connection_manager.py
---------------------
This holds the LOCAL in-memory map for ONE server:

        user_id  ->  the live WebSocket connection object

Why local (a plain dict) and NOT Redis or Postgres?
A socket is a live network connection that physically lives inside THIS
server's process. You cannot serialize it and store it somewhere else.
It only exists on the machine that is holding it open.

(The "which server is user X on" fact is the thing that goes in Redis later,
 when we have more than one server. That is a different map. Not this one.)
"""

import logging

log = logging.getLogger("chat.connmgr")


class ConnectionManager:
    def __init__(self):
        # The one and only local map. Keys are user_ids (str),
        # values are the websocket connection objects.
        self._sockets = {}

    def register(self, user_id, websocket):
        """Called when a user connects. Store their socket."""
        self._sockets[user_id] = websocket
        log.info(
            "REGISTER   user=%s  |  online now=%d %s",
            user_id,
            len(self._sockets),
            self.online_users(),
        )

    def unregister(self, user_id):
        """Called when a user disconnects. Remove their socket."""
        if user_id in self._sockets:
            del self._sockets[user_id]
            log.info(
                "UNREGISTER user=%s  |  online now=%d %s",
                user_id,
                len(self._sockets),
                self.online_users(),
            )

    def get_socket(self, user_id):
        """Return the socket for a user, or None if they are not on this server."""
        return self._sockets.get(user_id)

    def is_online(self, user_id):
        """True if this user currently has a live socket on this server."""
        return user_id in self._sockets

    def online_users(self):
        """List of every user_id connected to this server right now."""
        return list(self._sockets.keys())

    def everyone_except(self, user_id):
        """
        Every (user_id, socket) pair except the given user.
        Used to broadcast presence ('alice is online') to others.
        NOTE: real WhatsApp only tells your CONTACTS, not everyone.
        We broadcast to all here just to keep the demo simple.
        """
        return [(uid, ws) for uid, ws in self._sockets.items() if uid != user_id]
