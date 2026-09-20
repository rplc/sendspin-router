# Sendspin Router

Headless Sendspin server/router for a small Raspberry Pi. It is intended to sit between existing audio producers (Mopidy, Spotify, Chromecast capture, etc.) and Sendspin clients such as the Louder ESP32-S3 Plus.

## Design goal

The first iteration deliberately keeps the architecture small:

```text
ioBroker / Lovelace
        |
        | HTTP API
        v
+-------------------------+
| sendspin-router         |
|                         |
| aiosendspin 9.1.1       |
| mDNS client discovery   |
| client/group registry   |
| audio routing layer     |
+-------------+-----------+
              |
              | Sendspin / mDNS
              v
      Louder ESP32 clients
```

The planned user model is:

- `Wohnzimmer` = several ESP clients
- `Bad` = one ESP client
- `Schlafzimmer` = one or more ESP clients
- one active source can be routed to multiple groups
- group volume/mute is controlled through the API
- later: multiple independent sources at the same time

## Important

This is **not** Music Assistant and has no GUI. It is intended to run headless on the Pi 3B+.

The Sendspin protocol and `aiosendspin` are still evolving. Version 9.1.1 is intentionally pinned. The code therefore has a small compatibility layer around aiosendspin instead of scattering library-specific calls throughout the application.

## Installation on the Pi

Python 3.12 is required.

```bash
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3-pip ffmpeg

git clone <your-git-repository> /opt/sendspin-router
cd /opt/sendspin-router

python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.lock
```

Or install the project itself:

```bash
python -m pip install .
```

## Configuration

Copy:

```bash
cp config/config.example.yaml config/config.yaml
```

Edit the groups and API bind address as required.

## Run

```bash
. .venv/bin/activate
sendspin-router --config config/config.yaml
```

The HTTP API defaults to `http://0.0.0.0:8790`.

Useful endpoints:

- `GET /api/v1/status`
- `GET /api/v1/clients`
- `GET /api/v1/groups`
- `PUT /api/v1/groups/{group_id}`
- `POST /api/v1/groups/{group_id}/volume`
- `POST /api/v1/groups/{group_id}/mute`
- `POST /api/v1/router/source`
- `POST /api/v1/router/stop`

## systemd

After testing manually:

```bash
sudo cp systemd/sendspin-router.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sendspin-router
journalctl -u sendspin-router -f
```

## Current iteration scope

The first package focuses on the server lifecycle, configuration, API, persistent group definitions and Sendspin client registry. The audio producer adapter is deliberately isolated in `audio/` so that Mopidy/Spotify/Chromecast can be connected without changing the Sendspin/control layer.

The exact high-level source/PushStream API is isolated in `sendspin_backend.py`; this avoids baking assumptions about unstable aiosendspin internals into the rest of the project.
