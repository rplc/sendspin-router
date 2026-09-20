from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .models import GroupState, SourceConfig


@dataclass
class ServerConfig:
    name: str
    server_id: str
    sendspin_port: int


@dataclass
class MqttConfig:
    host: str
    port: int
    username: str | None
    password: str | None
    base_topic: str
    client_id: str


@dataclass
class RouterConfig:
    active_source: str | None


@dataclass
class SendspinConfig:
    identity_file: str
    pairing_store: str


@dataclass
class AppConfig:
    server: ServerConfig
    mqtt: MqttConfig
    groups: list[GroupState]
    sources: list[SourceConfig]
    router: RouterConfig
    sendspin: SendspinConfig


def _require(data: dict[str, Any], key: str) -> Any:
    if key not in data:
        raise ValueError(f"Missing configuration key: {key}")
    return data[key]


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    server_raw = _require(raw, "server")
    mqtt_raw = _require(raw, "mqtt")
    router_raw = raw.get("router", {})
    sendspin_raw = raw.get("sendspin", {})

    groups = [
        GroupState(
            group_id=str(item["id"]),
            name=str(item.get("name", item["id"])),
            members=list(item.get("members", [])),
            volume=int(item.get("volume", 100)),
            mute=bool(item.get("mute", False)),
            stream=item.get("stream"),
        )
        for item in raw.get("groups", [])
    ]

    sources = [
        SourceConfig(
            source_id=str(item["id"]),
            name=str(item.get("name", item["id"])),
            uri=str(item["uri"]),
            sample_rate=int(item.get("sample_rate", 48000)),
            channels=int(item.get("channels", 2)),
            bit_depth=int(item.get("bit_depth", 16)),
        )
        for item in raw.get("sources", [])
    ]

    return AppConfig(
        server=ServerConfig(
            name=str(server_raw.get("name", "Sendspin Router")),
            server_id=str(server_raw.get("id", "sendspin-router")),
            sendspin_port=int(server_raw.get("sendspin_port", 8927)),
        ),
        mqtt=MqttConfig(
            host=str(mqtt_raw.get("host", "127.0.0.1")),
            port=int(mqtt_raw.get("port", 1883)),
            username=mqtt_raw.get("username"),
            password=mqtt_raw.get("password"),
            base_topic=str(mqtt_raw.get("base_topic", "sendspin/router")).rstrip("/"),
            client_id=str(mqtt_raw.get("client_id", "sendspin-router")),
        ),
        groups=groups,
        sources=sources,
        router=RouterConfig(active_source=router_raw.get("active_source")),
        sendspin=SendspinConfig(
            identity_file=str(sendspin_raw.get("identity_file", "data/sendspin_identity.key")),
            pairing_store=str(sendspin_raw.get("pairing_store", "data/sendspin_pairings.json")),
        ),
    )
