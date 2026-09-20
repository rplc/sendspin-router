# Changelog

## 0.3.1

- Fixed static ESP connections for aiosendspin 9.1.1.
- `connect_to_client()` now receives the complete `ws://host:port/sendspin` URL.
- Static clients use port 8928 by default.
- Static clients use `retry_initial_connection=True` and `retry_indefinitely=True`.
- Each static client gets its own background connection task, so offline clients do not block other clients.
- Kept the router's own Sendspin server on port 8927.
