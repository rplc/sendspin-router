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

## Important

This iteration deliberately keeps the aiosendspin integration isolated in `sendspin_backend.py`. The next implementation step is wiring the configured PCM pipes into aiosendspin PushStreams and applying stream-to-group routing using the concrete 9.1.1 server API.

### Source URIs

The three configured source URIs are intentionally just the FIFO paths, e.g. `pipe:///run/snapserver/chromecast.pcm`. Sample format is represented separately in YAML, so the Snapserver query parameters are not needed by the router configuration.

### ioBroker

`iobroker/sendspin-router.js` mirrors the retained MQTT state into `0_userdata.0.SendspinRouter.*` and creates writable command states. Room policy is intentionally kept in ioBroker: `GROUP_RULES` maps discovered Sendspin client names/IDs to groups. MQTT state is event-driven; the only small timer is used to discover newly-created command objects inside ioBroker, not to poll the Sendspin router.
