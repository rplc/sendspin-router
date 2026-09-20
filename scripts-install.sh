#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! command -v python3.12 >/dev/null 2>&1; then
  echo "python3.12 is required. Install python3.12, python3.12-venv and python3-pip first." >&2
  exit 1
fi

python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.lock

if [ ! -f config/config.yaml ]; then
  cp config/config.example.yaml config/config.yaml
fi

echo "Installed. Edit config/config.yaml and run:"
echo "  . .venv/bin/activate"
echo "  sendspin-router --config config/config.yaml"
