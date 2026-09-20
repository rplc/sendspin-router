# Sendspin Router

Headless Sendspin server/router for a Raspberry Pi, intended for an ioBroker-controlled multi-room audio setup.

## Current target

- Raspberry Pi 3B+ / Debian-based OS
- Python 3.13
- aiosendspin 9.1.1
- MQTT for state/events/commands
- Sendspin clients discovered dynamically via Sendspin/mDNS
- Configurable groups and PCM pipe sources
- No GUI
- No Music Assistant

Python 3.13 is intentional: aiosendspin 9.1.1 explicitly supports Python 3.12 and 3.13.

## Install on the Pi

Recommended development/deployment location for this setup:

```text
/home/pi/sendspin-router
```

This avoids needing `sudo` for normal `git pull`, configuration and virtual-environment work.

```bash
cd ~
git clone <YOUR_GIT_REPOSITORY_URL> sendspin-router
cd ~/sendspin-router

python3 --version
python3 -m venv .venv
.venv/bin/pip3 install --upgrade pip
.venv/bin/pip3 install -r requirements.lock
.venv/bin/pip3 install -e .
cp config/config.example.yaml config/config.yaml
```

The editable install is important: it creates the `sendspin-router` command inside `.venv/bin`.

Test:

```bash
.venv/bin/sendspin-router --help
```

or after activation:

```bash
source .venv/bin/activate
sendspin-router --help
```

## MQTT

The router uses MQTT as its primary control/state API.

Base topic is configurable; default:

```text
sendspin/router
```

### Retained state

```text
sendspin/router/state
sendspin/router/state/clients
sendspin/router/state/groups
sendspin/router/state/sources
sendspin/router/state/router
```

Each is retained, so ioBroker receives the current state immediately after subscribing.

### Events

```text
sendspin/router/event/#
```

Events are not retained.

### Commands

```text
sendspin/router/command/group/<group_id>/set_members
sendspin/router/command/group/<group_id>/set_volume
sendspin/router/command/group/<group_id>/set_mute
sendspin/router/command/group/<group_id>/set_stream
sendspin/router/command/router/set_active_source
```

Command payloads are JSON.

Examples:

```json
{"members":["wohnzimmer-1-sendspin","wohnzimmer-2-sendspin"]}
```

```json
{"volume":65}
```

```json
{"mute":false}
```

```json
{"source":"mopidy"}
```

```json
{"source":"mopidy","enabled":true}
```

The exact topic contract is documented in `docs/mqtt-api.md`.

## Systemd

The service file runs the application as user `pi` from `/home/pi/sendspin-router`.

Install:

```bash
sudo cp systemd/sendspin-router.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sendspin-router
```

Logs:

```bash
journalctl -u sendspin-router -f
```

## Headless clients (ESP "Louder Boards")

The ESP Louder clients are configured with their fixed IP addresses under
`clients.static`. Their default Sendspin endpoint is port `8928` and
`/sendspin`.

The router uses aiosendspin's `connect_to_client(url, ...)` API with
`retry_initial_connection=true` and `retry_indefinitely=true`. This means
the router automatically waits for an ESP that is currently offline and
reconnects it after a network/device restart.

Each static client can specify a default group. Once the client completes
the Sendspin handshake, the router can assign it to that group.

The router itself continues to listen on port `8927` and advertise as a
Sendspin server for GUI clients such as Android apps.

## Per-group audio routing

Each group has its own `stream` (see `docs/mqtt-api.md` — `set_stream`),
and each group gets its own independent subscription to a source's PCM feed.
That means "Wohnzimmer + Bad play Mopidy while Schlafzimmer plays Spotify"
works out of the box, simultaneously — there's no single global source
feeding everything. `router.active_source` still exists for the simpler
"one source, many groups" case, but per-group `stream` is the primary
mechanism now.

## Important — verify the aiosendspin integration on your Pi

This iteration wires the configured PCM pipes into aiosendspin and applies
stream-to-group routing, but two pieces of that (in `sendspin_backend.py`,
each marked `UNVERIFIED` in a docstring) had to be written from the aiosendspin
9.1.1 source history rather than a stable, documented API — the project
moves fast enough that I couldn't confirm exact method names/signatures for:

1. `connect_static_clients()` — actively dialing a headless client at a known host/port.
2. `attach_group_audio()` / `feed_group()` — creating a per-group audio feed and pushing PCM into it.

Both are written defensively (they try a couple of plausible method names and
log clearly instead of crashing if none match), but they may simply do
nothing useful until corrected. Please run this on the Pi, inside the venv,
and send me the output so the calls can be pinned down exactly:

```bash
.venv/bin/python -c "
import inspect
from aiosendspin.server.server import SendspinServer
from aiosendspin.server.push_stream import PushStream
for cls in (SendspinServer, PushStream):
    print(f'--- {cls.__name__} ---')
    for name, member in inspect.getmembers(cls):
        if not name.startswith('_') and (inspect.iscoroutinefunction(member) or inspect.isfunction(member)):
            try:
                print(name, inspect.signature(member))
            except (TypeError, ValueError):
                print(name)
"
```

Everything else (config, MQTT, per-group routing model, PCM FIFO reading) is
regular Python I could write and test directly, and has unit tests in `tests/`.

### Source URIs

The three configured source URIs are intentionally just the FIFO paths, e.g. `pipe:///run/snapserver/chromecast.pcm`. Sample format is represented separately in YAML, so the Snapserver query parameters are not needed by the router configuration.

### ioBroker

`iobroker/sendspin-router.js` mirrors the retained MQTT state into `0_userdata.0.SendspinRouter.*` and creates writable command states. Room policy is intentionally kept in ioBroker: `GROUP_RULES` maps discovered Sendspin client names/IDs to groups. MQTT state is event-driven; the only small timer is used to discover newly-created command objects inside ioBroker, not to poll the Sendspin router.
