# MQTT API

The MQTT API is event-driven. State topics are retained and only published
when their content changed, so subscribers never need to poll. After every
(re)connect to the broker the router publishes its complete state once.

Base topic is configurable (`mqtt.base_topic`), default `sendspin/router`.

## Topic tree

```text
sendspin/router/
├── availability                 retained, "online" / "offline" (last will)
├── state/                       retained JSON
│   ├── clients
│   ├── groups
│   ├── sources
│   └── router
├── event/                       not retained
│   ├── command_applied
│   └── command_error
└── command/                     JSON payloads, must NOT be retained
    ├── group/<group_id>/
    │   ├── set_members
    │   ├── set_volume
    │   ├── set_mute
    │   └── set_stream
    └── client/<client_id>/
        ├── set_group
        ├── set_volume
        └── set_mute
```

## `availability`

Plain string `online` or `offline`. `offline` is set by the broker via the
MQTT last will if the router disappears without saying goodbye.

## `state/clients`

JSON object keyed by Sendspin client ID:

```json
{
  "wohnzimmer-1-sendspin": {
    "id": "wohnzimmer-1-sendspin",
    "name": "Wohnzimmer 1",
    "available": true,
    "roles": ["player@v1"],
    "group_id": "wohnzimmer",
    "volume": 80,
    "mute": false
  }
}
```

- `available`: the client currently has a connection to the router.
- `group_id`: the logical group the client belongs to, or `null`.
- `volume` / `mute`: the player's own volume, `null` if the client has no
  player role (yet).

## `state/groups`

JSON object keyed by group ID:

```json
{
  "wohnzimmer": {
    "id": "wohnzimmer",
    "name": "Wohnzimmer",
    "members": ["wohnzimmer-1-sendspin"],
    "volume": 70,
    "mute": false,
    "stream": "spotify",
    "playback_state": "playing"
  }
}
```

- `members`: the source of truth for group membership. A client belongs to
  at most one group.
- `volume`: the group volume as defined by Sendspin (average of the member
  players' volumes). Changing it shifts all players by the same amount, so
  their relative balance is kept.
- `mute`: `true` only when all players of the group are muted.
- `stream`: source ID this group plays, or `null`.
- `playback_state`: `playing` while a Sendspin stream is running for the
  group, else `stopped`. A running stream whose source is silent (upstream
  player paused) is still `playing`; see `state/sources` → `available`.

## `state/sources`

JSON object keyed by source ID. Sources come from the YAML config.

```json
{
  "spotify": {
    "id": "spotify",
    "name": "Spotify",
    "uri": "pipe:///run/snapserver/spotify.pcm",
    "sample_rate": 48000,
    "channels": 2,
    "bit_depth": 16,
    "available": true
  }
}
```

`available` is `true` while PCM data is actually flowing, i.e. the upstream
player is playing. All configured FIFOs are read continuously, also those no
group is using (the audio is then discarded), so this works for every source
and upstream players never block on a full pipe.

## `state/router`

```json
{"version": "0.4.0"}
```

## Events

Every command produces exactly one event:

```json
{"command": "group/wohnzimmer/set_volume"}
```

on `event/command_applied`, or

```json
{"command": "group/wohnzimmer/set_volume", "error": "Invalid volume: 'loud'"}
```

on `event/command_error`. The new state is published before
`command_applied`.

## Commands

All payloads are JSON objects. Retained command messages are ignored,
because they would be replayed on every reconnect.

### `group/<group_id>/set_members`

```json
{"members": ["wohnzimmer-1-sendspin", "wohnzimmer-2-sendspin"]}
```

Replaces the member list. Clients listed here are removed from any other
group. Clients no longer listed end up in no group.

### `group/<group_id>/set_volume`

```json
{"volume": 65}
```

0..100, forwarded to the member players' hardware volume.

### `group/<group_id>/set_mute`

```json
{"mute": true}
```

Mutes or unmutes all member players.

### `group/<group_id>/set_stream`

```json
{"source": "spotify"}
```

Use `null` to stop playback for the group:

```json
{"source": null}
```

Each group has its own independent stream, so different groups can play
different sources at the same time.

### `client/<client_id>/set_group`

```json
{"group": "bad"}
```

Moves one client to another group (removing it from its current one).
`null` removes it from all groups.

### `client/<client_id>/set_volume` / `client/<client_id>/set_mute`

```json
{"volume": 40}
```

```json
{"mute": false}
```

Changes a single player, e.g. to balance speakers within a group.

## Removed

`command/router/set_active_source` and `state/router.active_source` were
removed in 0.4.0. They never affected audio; use the per-group `set_stream`.
