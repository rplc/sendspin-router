from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ClientInfo:
    client_id: str
    name: str
    available: bool = True
    roles: list[str] = field(default_factory=list)
    group_id: str | None = None
    volume: int | None = None
    mute: bool | None = None
    offset_us: int = 0
    capabilities: dict[str, Any] = field(default_factory=dict)


@dataclass
class GroupState:
    group_id: str
    name: str
    members: list[str] = field(default_factory=list)
    volume: int = 100
    mute: bool = False
    stream: str | None = None
    playback_state: str = "stopped"


@dataclass
class SourceConfig:
    source_id: str
    name: str
    uri: str
    sample_rate: int = 48000
    channels: int = 2
    bit_depth: int = 16
    available: bool = False
