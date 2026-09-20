from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class ClientInfo:
    client_id: str
    name: str
    host: str | None = None
    port: int | None = None
    available: bool = False
    roles: list[str] = field(default_factory=list)
    group_id: str | None = None


@dataclass(slots=True)
class GroupState:
    group_id: str
    name: str
    members: list[str] = field(default_factory=list)
    volume: int = 70
    mute: bool = False
    playback_state: str = "stopped"


@dataclass(slots=True)
class RouterState:
    active_source: str | None = None
    target_groups: list[str] = field(default_factory=list)
