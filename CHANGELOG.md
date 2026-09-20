# Changelog

## 0.3.3
- Use aiosendspin 9.1.1 native `SendspinGroup.start_stream()` / `PushStream` API.
- Feed configured PCM FIFOs through `prepare_audio()` + `commit_audio()`.
- Mirror router logical groups into native Sendspin groups.
- Automatically create a group's PushStream when its first client joins.
- Static Louder client default port is 8928; Sendspin server remains on 8927.
- Static client matching prefers the URL registered by aiosendspin.
- More robust SIGINT shutdown handling.


## 0.3.2

- Fix static Sendspin client connections for aiosendspin 9.1.1: `connect_to_client()` is a regular method taking a full WebSocket URL.
- Use the correct Louder client port `8928` in the example configuration.
- Enable aiosendspin-managed initial/retry connections for configured static clients.
- Cleanly stop group streams and the Sendspin server on shutdown.
- Clear the in-memory client registry after shutdown.
