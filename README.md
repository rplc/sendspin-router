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


### Source activity detection

`state/sources` now distinguishes two concepts:

- `available`: PCM bytes are currently flowing from the upstream writer.
- `playing`: a lightweight sampled peak detector sees actual audio activity.

This matters for continuously running sources such as Spotify or Chromecast,
which can keep writing silence into their FIFOs while paused. The detector
checks only one frame out of every 20 and uses start/stop hysteresis, so it is
designed to be negligible on a Raspberry Pi 3B+.

### ioBroker volume profiles

The ioBroker script contains optional per-group `VOLUME_PROFILES`. For example,
`Balanced` uses normal Sendspin group volume, while `Kochen` and
`Hintergrundbeschallung` can set individual client volumes. Client keys are
Sendspin client IDs; on ESP clients these may be MAC addresses. Edit the
`VOLUME_PROFILES` object at the top of `iobroker/sendspin-router.js` to match
the actual clients and desired levels.

## MQTT

The router uses MQTT as its only control/state API. Base topic is
configurable; default `sendspin/router`.

- `state/clients`, `state/groups`, `state/sources`, `state/router`:
  retained JSON, published only on change.
- `availability`: retained `online`/`offline` (MQTT last will).
- `event/command_applied`, `event/command_error`: one per command.
- `command/group/<group_id>/set_members|set_volume|set_mute|set_stream`
- `command/client/<client_id>/set_group|set_volume|set_mute`

Examples:

```text
sendspin/router/command/group/wohnzimmer/set_stream   {"source":"mopidy"}
sendspin/router/command/group/wohnzimmer/set_volume   {"volume":65}
sendspin/router/command/client/bad-1/set_group        {"group":"wohnzimmer"}
```

The router reconnects to the broker automatically, so restarting ioBroker
does not interrupt audio. The full contract is in `docs/mqtt-api.md`.

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

The ESP Louder Board firmware has no GUI/app, so it can't initiate pairing
or reliably show up via mDNS the way a phone/desktop client does. List such
clients under `clients.static` in `config.yaml` (host/port + which group
they belong to); the router connects to them on startup and auto-assigns
them to that group as soon as they come online. Anything else the router
sees falls back to `clients.default_group`, if set.

## Per-group audio routing

Each group has its own `stream` (see `docs/mqtt-api.md` — `set_stream`),
and each group gets its own independent subscription to a source's PCM feed.
That means "Wohnzimmer + Bad play Mopidy while Schlafzimmer plays Spotify"
works out of the box, simultaneously — there's no single global source
feeding everything.

The router reads every configured FIFO continuously, even while no group
plays that source, and discards the unused audio, just like Snapserver did.
Without that, a player such as Mopidy blocks as soon as its FIFO is full and
appears to not play at all.

Reads are paced to realtime (at most 0.2 s ahead of the audio clock). Players
that write faster than realtime, e.g. Mopidy's GStreamer `filesink` or a radio
stream catching up after a network stall, are throttled by the full pipe, so
they can neither race through a playlist nor fill the Sendspin buffer far
ahead of playback.

### Source URIs

The three configured source URIs are intentionally just the FIFO paths, e.g. `pipe:///run/snapserver/chromecast.pcm`. Sample format is represented separately in YAML, so the Snapserver query parameters are not needed by the router configuration.

### ioBroker

`iobroker/sendspin-router.js` is a script for the ioBroker javascript
adapter. The MQTT adapter instance (`mqtt.0`) must subscribe to
`sendspin/router/#`.

The script mirrors the router state into `0_userdata.0.SendspinRouter.*`.
Every control is a single writable state following the ioBroker ack
convention: a write with `ack=false` (GUI, Lovelace, other scripts) is sent
to the router as a command. The router's confirmation comes back as the same
state with `ack=true`.

| State | Lovelace entity | Command |
| --- | --- | --- |
| `Groups.<g>.Stream` | `input_select` | `set_stream` (`off` = no source) |
| `Groups.<g>.Volume` | `input_number` | `set_volume` |
| `Groups.<g>.Mute` | `input_boolean` | `set_mute` |
| `Groups.<g>.Members` | | `set_members` (JSON array) |
| `Clients.<c>.Group` | | `set_group` (empty = none) |
| `Clients.<c>.Volume` / `.Mute` | | per-player volume/mute |
| `Assign.Client` | `input_select` | picks a client, labelled "Name (Gruppe)" |
| `Assign.Group` | `input_select` | moves the picked client (`none` = no group) |

To move a client in Lovelace, pick it in `Assign.Client`. `Assign.Group`
then shows its current group. Picking another group moves the client at
once. Both selects stay set, so the confirmed result is visible: the client
label and the group select update when the router confirms. Both lists
follow the router's clients and groups automatically.

The router publishes only changes, and the script writes only states whose
value changed. The only periodic task is a daily job (04:17) that deletes
`Clients.<c>` objects of clients the router has not reported for at least
24 hours (`CLIENT_CLEANUP_AFTER_HOURS`, `CLIENT_CLEANUP_CRON`). It does
nothing while the router is offline, and a client that comes back is simply
recreated. On first start the script
deletes the objects of older script versions (`Groups.<g>.Command.*`,
`Router.ActiveSource`, ...), so the Lovelace entity names move to the new
states. Set `CLEANUP_LEGACY_OBJECTS = false` to keep them.

Group assignment happens in the router: `clients.static[].group` first, then
`clients.default_group`. The script's `GROUP_RULES` only apply to clients the
router left unassigned and that were never assigned manually.
