from __future__ import annotations

import argparse
import asyncio
import logging

from . import __version__
from .app import RouterApp
from .config import load_config


async def run(config_path: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = load_config(config_path)
    app = RouterApp(config)
    await app.start()
    try:
        await asyncio.Event().wait()
    finally:
        await app.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Headless Sendspin router")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args()
    try:
        asyncio.run(run(args.config))
    except KeyboardInterrupt:
        # asyncio.run() normally cancels the main task first, which lets run()
        # execute its finally block. This catch only prevents a second traceback
        # if SIGINT arrives while the event loop is already shutting down.
        pass


if __name__ == "__main__":
    main()
