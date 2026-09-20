from __future__ import annotations

import argparse
import asyncio
import logging

from .app import RouterApp
from .config import load_config


async def run(config_path: str) -> None:
    config = load_config(config_path)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = RouterApp(config)
    await app.start()
    try:
        await asyncio.Event().wait()
    finally:
        await app.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Headless Sendspin router")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
