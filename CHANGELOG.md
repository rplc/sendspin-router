# Changelog

## 0.3.2

- Fix static Sendspin client connections for aiosendspin 9.1.1: `connect_to_client()` is a regular method taking a full WebSocket URL.
- Use the correct Louder client port `8928` in the example configuration.
- Enable aiosendspin-managed initial/retry connections for configured static clients.
- Cleanly stop group streams and the Sendspin server on shutdown.
- Clear the in-memory client registry after shutdown.
