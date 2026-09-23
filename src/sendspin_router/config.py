from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .models import GroupState, SourceConfig

_LOG = logging.getLogger(__name__)


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
class SendspinConfig:
    identity_file: str
    pairing_store: str


@dataclass
class StaticClientConfig:
    """A headless client (e.g. an ESP "Louder Board") reachable at a known
    host/port that the router must connect to actively, since it has no GUI
    to initiate pairing itself."""

    host: str
    port: int
    group: str | None


@dataclass
class ClientsConfig:
    default_group: str | None
    static: list[StaticClientConfig]


@dataclass
class AppConfig:
    server: ServerConfig
    mqtt: MqttConfig
    groups: list[GroupState]
    sources: list[SourceConfig]
    sendspin: SendspinConfig
    clients: ClientsConfig


def _require(data: dict[str, Any], key: str) -> Any:
    if key not in data:
        raise ValueError(f"Missing configuration key: {key}")
    return data[key]


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    server_raw = _require(raw, "server")
    mqtt_raw = _require(raw, "mqtt")
    if (raw.get("router") or {}).get("active_source") is not None:
        _LOG.warning(
            "router.active_source is no longer supported and is ignored; "
            "use the per-group 'stream' setting instead"
        )
    sendspin_raw = raw.get("sendspin") or {}
    clients_raw = raw.get("clients") or {}

    groups = [
        GroupState(
            group_id=str(item["id"]),
            name=str(item.get("name", item["id"])),
            members=[str(m) for m in item.get("members") or []],
            volume=int(item.get("volume", 100)),
            mute=bool(item.get("mute", False)),
            stream=item.get("stream"),
        )
        for item in raw.get("groups") or []
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
        for item in raw.get("sources") or []
    ]

    static_clients = [
        StaticClientConfig(
            host=str(item["host"]),
            port=int(item.get("port", 8928)),
            group=item.get("group"),
        )
        for item in clients_raw.get("static") or []
    ]

    config = AppConfig(
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
        sendspin=SendspinConfig(
            identity_file=str(sendspin_raw.get("identity_file", "data/sendspin_identity.key")),
            pairing_store=str(sendspin_raw.get("pairing_store", "data/sendspin_pairings.json")),
        ),
        clients=ClientsConfig(
            default_group=clients_raw.get("default_group"),
            static=static_clients,
        ),
    )
    _validate(config)
    return config


def _validate(config: AppConfig) -> None:
    """Fail fast on references that would otherwise only break at runtime."""
    source_ids = [s.source_id for s in config.sources]
    group_ids = [g.group_id for g in config.groups]
    for kind, ids in (("source", source_ids), ("group", group_ids)):
        duplicates = {i for i in ids if ids.count(i) > 1}
        if duplicates:
            raise ValueError(f"Duplicate {kind} id(s): {', '.join(sorted(duplicates))}")

    for group in config.groups:
        if group.stream is not None and group.stream not in source_ids:
            raise ValueError(f"Group '{group.group_id}' uses unknown stream '{group.stream}'")
        if not 0 <= group.volume <= 100:
            raise ValueError(f"Group '{group.group_id}' volume must be 0..100")

    seen: dict[str, str] = {}
    for group in config.groups:
        for member in group.members:
            if member in seen:
                raise ValueError(
                    f"Client '{member}' is listed in groups '{seen[member]}' and "
                    f"'{group.group_id}'; a client can only belong to one group"
                )
            seen[member] = group.group_id

    if config.clients.default_group and config.clients.default_group not in group_ids:
        raise ValueError(f"clients.default_group '{config.clients.default_group}' is unknown")
    for static in config.clients.static:
        if static.group and static.group not in group_ids:
            raise ValueError(f"Static client {static.host} uses unknown group '{static.group}'")
