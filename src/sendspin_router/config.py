from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    name: str = "Home Sendspin"
    id: str = "home-sendspin"
    sendspin_port: int = 8927
    api_host: str = "0.0.0.0"
    api_port: int = 8790


class GroupConfig(BaseModel):
    id: str
    name: str
    members: list[str] = Field(default_factory=list)
    volume: int = 70
    mute: bool = False


class RouterConfig(BaseModel):
    active_source: str | None = None


class AppConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    groups: list[GroupConfig] = Field(default_factory=list)
    router: RouterConfig = Field(default_factory=RouterConfig)


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open("r", encoding="utf-8") as f:
        data: dict[str, Any] = yaml.safe_load(f) or {}
    return AppConfig.model_validate(data)
