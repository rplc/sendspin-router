from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

_LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class AudioSource:
    source_id: str
    name: str
    description: str = ""


class AudioRouter:
    """Producer-independent audio routing facade.

    Sources are deliberately abstract here. Existing Mopidy/Spotify/HDMI
    pipelines can stay untouched. The next iteration will attach their PCM
    output to aiosendspin PushStream(s).
    """

    def __init__(self) -> None:
        self.sources = {
            "mopidy": AudioSource("mopidy", "Mopidy", "Existing Mopidy output"),
            "spotify": AudioSource("spotify", "Spotify", "Existing Spotify output"),
            "chromecast": AudioSource("chromecast", "Chromecast", "Existing HDMI capture output"),
        }
        self.active_source: str | None = None
        self.target_groups: list[str] = []

    async def select(self, source_id: str | None, groups: list[str]) -> dict:
        if source_id is not None and source_id not in self.sources:
            raise ValueError(f"Unknown source: {source_id}")
        self.active_source = source_id
        self.target_groups = list(dict.fromkeys(groups))
        _LOG.info("Selected source=%s groups=%s", source_id, self.target_groups)
        return self.state()

    async def stop(self) -> dict:
        self.active_source = None
        self.target_groups = []
        return self.state()

    def state(self) -> dict:
        return {
            "active_source": self.active_source,
            "target_groups": self.target_groups,
            "sources": [{"source_id": s.source_id, "name": s.name, "description": s.description} for s in self.sources.values()],
        }
