from __future__ import annotations

import argparse
import asyncio
import logging

import uvicorn

from .api import create_app
from .audio import AudioRouter
from .config import load_config
from .sendspin_backend import SendspinBackend


async def run(config_path: str) -> None:
    config = load_config(config_path)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    backend = SendspinBackend(config)
    audio = AudioRouter()
    await backend.start()

    app = create_app(backend, audio)
    server = uvicorn.Server(
        uvicorn.Config(app, host=config.server.api_host, port=config.server.api_port, log_level="info")
    )
    try:
        await server.serve()
    finally:
        await backend.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Headless Sendspin router")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
