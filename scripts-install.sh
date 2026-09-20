#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Python:"
python3 --version

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

.venv/bin/pip3 install --upgrade pip
.venv/bin/pip3 install -r requirements.lock
.venv/bin/pip3 install -e .

if [[ ! -f config/config.yaml ]]; then
  cp config/config.example.yaml config/config.yaml
  echo "Created config/config.yaml from example."
fi

echo
echo "Installation complete."
echo "Test with:"
echo "  .venv/bin/sendspin-router --help"
echo
echo "Then edit:"
echo "  config/config.yaml"
