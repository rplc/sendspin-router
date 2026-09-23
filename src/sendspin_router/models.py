from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ClientInfo:
    """Snapshot of one Sendspin client as published on ``state/clients``."""

    client_id: str
    name: str
    available: bool = False
    roles: list[str] = field(default_factory=list)
    group_id: str | None = None
    volume: int | None = None
    mute: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.client_id,
            "name": self.name,
            "available": self.available,
            "roles": list(self.roles),
            "group_id": self.group_id,
            "volume": self.volume,
            "mute": self.mute,
        }


@dataclass
class GroupState:
    """A logical router group (room). ``members`` is the source of truth for
    which Sendspin clients belong to it."""

    group_id: str
    name: str
    members: list[str] = field(default_factory=list)
    volume: int = 100
    mute: bool = False
    stream: str | None = None


@dataclass
class SourceConfig:
    source_id: str
    name: str
    uri: str
    sample_rate: int = 48000
    channels: int = 2
    bit_depth: int = 16

    @property
    def frame_size(self) -> int:
        """Bytes per interleaved PCM frame (one sample for every channel)."""
        return max(1, self.bit_depth // 8) * max(1, self.channels)
