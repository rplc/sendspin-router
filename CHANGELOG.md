## 0.5.0

- New: PCM source state now distinguishes `available` (PCM is flowing) from
  `playing` (audio activity detected). Activity detection samples only a small
  fraction of 16-bit PCM frames, with hysteresis, to keep Raspberry Pi 3B+ CPU
  usage negligible.
- New: ioBroker mirrors source `Playing` and adds configurable per-group
  `VolumeProfile` selects. `Balanced` uses normal Sendspin group volume; other
  profiles can set individual client volumes.

# Changelog

## 0.4.0

MQTT control API and ioBroker integration reworked.

- Fix: group volume and mute now reach the players (via the Sendspin player
  group role). Before, they only changed a number in the published state.
- Fix: retained state is only published when it changed, instead of all
  four topics every 2 seconds. Together with the new ioBroker script this
  removes the "setState more than 1000 times per minute" flood that made
  the javascript adapter stop the script, so no commands were forwarded.
- Fix: native Sendspin groups are reconciled idempotently. Covers groups
  whose last client left (stale group/stream kept before, silence after
  rejoin), clients listed under `groups[].members` in YAML (never grouped
  natively before), clients moving between groups (stayed listed in the old
  group) and clients reconnecting.
- Fix: a client removed from its group no longer gets auto-assigned back to
  the default group 2 seconds later.
- Fix: PCM FIFO reader no longer busy-loops while no writer is attached,
  only delivers whole frames, and never blocks shutdown on an idle writer.
  `sources.<id>.available` now means "audio is flowing".
- Fix: all configured PCM FIFOs are drained continuously, also while no
  group uses the source. Before, an unused source's player (e.g. Mopidy)
  blocked on the full pipe and did not play at all. Readers run in their own
  thread pool and survive read errors; a missing FIFO is logged once.
- Fix: PCM reads are paced to realtime (like Snapserver). Writers faster
  than realtime (Mopidy `filesink`, radio streams catching up) no longer run
  ahead of playback or race through playlists while no group listens.
- Fix: systemd unit no longer depends on a local `mosquitto.service`; the
  broker is remote and the router reconnects on its own. `TimeoutStopSec=15`.
- Fix: MQTT reconnects automatically (broker restart no longer requires a
  router restart); new retained `availability` topic with last will.
- Fix: commands and the periodic refresh are serialized, payloads are
  validated (`"mute":"false"` no longer means `true`), retained commands
  are ignored.
- New: `command/client/<id>/set_group|set_volume|set_mute`.
- New: real `playback_state`, live client/group volume and mute in state.
- New: config validation at startup (unknown stream/group references,
  duplicate ids, clients in several groups).
- Removed: `router.active_source`, `command/router/set_active_source` and
  `state/router.active_source`. They never affected audio.
- Removed: unused FastAPI REST API (`api.py`, never started, already broken)
  and its dependencies.
- Removed from state: `offset_us` and `capabilities` (always 0 / empty).
- State JSON uses `id` for clients and groups, as documented.
- ioBroker script rewritten: one writable state per control with proper
  ack handling, writes only on change, serialized processing, command
  errors logged, legacy objects cleaned up.
- ioBroker: `Assign.Client` / `Assign.Group` selects for moving clients
  between groups from Lovelace. `GROUP_RULES` no longer undo a manual
  "no group" assignment.
- ioBroker: daily job deletes objects of clients the router has not
  reported for 24 h.

## 0.3.6

- Fix live PCM feeding: do not call `PushStream.sleep_to_limit_buffer()` after every FIFO chunk.
- Keep the realtime FIFO reader continuously feeding `PushStream`.
- Fix diagnostic logging of PCM read intervals.
- Retain the v0.3.4 audio timing diagnostics for `prepare_audio()` and `commit_audio()`.

## 0.3.4
- Add PCM/PushStream timing diagnostics for troubleshooting realtime playback.
- Log FIFO chunk size, audio duration, read-to-read interval, sink delivery time,
  `commit_audio()` duration/result, and `sleep_to_limit_buffer()` duration.
- Keep the v0.3.3 audio behaviour unchanged; diagnostics are rate-limited to avoid
  flooding the Pi 3B+ log.


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
