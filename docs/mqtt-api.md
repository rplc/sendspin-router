# MQTT API

The MQTT API is event-driven. State topics are retained, so ioBroker does not need to poll.

## Topic tree

```text
sendspin/router/
├── state/
│   ├── clients
│   ├── groups
│   ├── sources
│   └── router
├── event/
│   ├── client_added
│   ├── client_removed
│   ├── client_changed
│   ├── group_changed
│   ├── source_changed
│   └── router_changed
└── command/
    ├── router/set_active_source
    └── group/<group_id>/
        ├── set_members
        ├── set_volume
        ├── set_mute
        └── set_stream
```

## `state/clients`

Retained JSON object keyed by Sendspin client ID:

```json
{
  "wohnzimmer-1-sendspin": {
    "id": "wohnzimmer-1-sendspin",
    "name": "Wohnzimmer 1",
    "available": true,
    "roles": ["player"],
    "group_id": "wohnzimmer",
    "volume": 80,
    "mute": false,
    "offset_us": 0,
    "capabilities": {}
  }
}
```

`offset_us` is reserved for the actual Sendspin time-sync offset once exposed by the backend. It should not be treated as a manually configurable delay.

## `state/groups`

Retained JSON object keyed by group ID:

```json
{
  "wohnzimmer": {
    "id": "wohnzimmer",
    "name": "Wohnzimmer",
    "members": ["wohnzimmer-1-sendspin"],
    "volume": 70,
    "mute": false,
    "stream": "mopidy",
    "playback_state": "stopped"
  }
}
```

## `state/sources`

Retained JSON object keyed by source ID. Sources are loaded dynamically from YAML.

```json
{
  "mopidy": {
    "id": "mopidy",
    "name": "Mopidy",
    "uri": "pipe:///run/snapserver/mopidy.pcm?...",
    "available": true
  }
}
```

## `state/router`

Retained router state:

```json
{
  "active_source": "mopidy",
  "version": "0.2.0"
}
```

## Commands

All command payloads are JSON.

### Group members

Topic:

```text
sendspin/router/command/group/wohnzimmer/set_members
```

Payload:

```json
{"members":["wohnzimmer-1-sendspin","wohnzimmer-2-sendspin"]}
```

### Group volume

```text
sendspin/router/command/group/wohnzimmer/set_volume
```

```json
{"volume":65}
```

### Group mute

```text
sendspin/router/command/group/wohnzimmer/set_mute
```

```json
{"mute":true}
```

### Group stream

This is the future routing command and is intentionally accepted by the control model now.

```text
sendspin/router/command/group/wohnzimmer/set_stream
```

```json
{"source":"spotify"}
```

Use `null` to detach a group:

```json
{"source":null}
```

### Global active source

For the MVP where one source can feed multiple groups:

```text
sendspin/router/command/router/set_active_source
```

```json
{"source":"spotify"}
```

The source IDs and groups are not hard-coded in the MQTT layer. They come from YAML, so adding/removing entries changes the published state automatically.
