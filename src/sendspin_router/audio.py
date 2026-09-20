from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .models import SourceConfig

_LOG = logging.getLogger(__name__)


@dataclass
class SourceRuntime:
    config: SourceConfig
    task: asyncio.Task[None] | None = None


class AudioRouter:
    """Owns source definitions and the future PCM -> PushStream bridge."""

    def __init__(self, sources: list[SourceConfig]) -> None:
        self.sources = {source.source_id: SourceRuntime(source) for source in sources}

    def state(self) -> dict[str, dict]:
        return {
            source_id: {
                "id": runtime.config.source_id,
                "name": runtime.config.name,
                "uri": runtime.config.uri,
                "sample_rate": runtime.config.sample_rate,
                "channels": runtime.config.channels,
                "bit_depth": runtime.config.bit_depth,
                "available": runtime.config.available,
            }
            for source_id, runtime in self.sources.items()
        }

    async def start(self) -> None:
        # Pipe handling is deliberately not started yet. This keeps v0.2
        # focused on the deployment/control plane before we attach the actual
        # aiosendspin PushStream.
        _LOG.info("Configured %d audio source(s)", len(self.sources))

    async def stop(self) -> None:
        for runtime in self.sources.values():
            if runtime.task:
                runtime.task.cancel()
        await asyncio.gather(
            *(r.task for r in self.sources.values() if r.task),
            return_exceptions=True,
        )
