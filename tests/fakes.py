"""Minimal stand-ins mimicking aiosendspin 9.1.1 group semantics.

Only what SendspinBackend touches is modelled: every client is always in
exactly one native group, ``add_client`` pulls a client out of its old group,
``remove_client``/``ungroup`` put it into a fresh solo group, and a group whose
last client leaves stops its stream.
"""

from __future__ import annotations

import itertools

from sendspin_router.config import (
    AppConfig,
    ClientsConfig,
    MqttConfig,
    SendspinConfig,
    ServerConfig,
    StaticClientConfig,
)
from sendspin_router.models import GroupState, SourceConfig

_ids = itertools.count(1)


class FakeStream:
    def __init__(self) -> None:
        self.is_stopped = False
        self.live = None
        self.chunks: list[bytes] = []
        self.fail = False

    def set_live_source(self, live: bool) -> None:
        self.live = live

    def prepare_audio(self, chunk: bytes, _fmt) -> None:
        if self.fail:
            raise RuntimeError("boom")
        self.chunks.append(chunk)

    async def commit_audio(self) -> int:
        return 0

    def stop(self) -> None:
        self.is_stopped = True


class FakePlayerRole:
    def __init__(self, volume: int = 100, muted: bool = False) -> None:
        self.volume = volume
        self.muted = muted
        self.sent: list[tuple[str, object]] = []

    def set_player_volume(self, volume: int) -> None:
        self.sent.append(("volume", volume))  # device echoes later

    def set_player_mute(self, muted: bool) -> None:
        self.sent.append(("mute", muted))


class FakePlayerGroupRole:
    def __init__(self, group: FakeGroup) -> None:
        self.group = group
        self.sent: list[tuple[str, object]] = []

    def _players(self) -> list[FakePlayerRole]:
        return [c.player for c in self.group.clients]

    def get_player_clients(self):
        return list(self.group.clients)

    def get_group_volume(self) -> int:
        players = self._players()
        return round(sum(p.volume for p in players) / len(players)) if players else 100

    def get_group_muted(self) -> bool:
        players = self._players()
        return bool(players) and all(p.muted for p in players)

    def set_group_volume(self, level: int) -> None:
        self.sent.append(("volume", level))

    def set_group_muted(self, muted: bool) -> None:
        self.sent.append(("mute", muted))


class FakeGroup:
    def __init__(self, *clients: FakeClient) -> None:
        self.group_id = f"native-{next(_ids)}"
        self.clients: list[FakeClient] = list(clients)
        self.stream: FakeStream | None = None
        self.role = FakePlayerGroupRole(self)
        for client in clients:
            client.group = self

    def group_role(self, family: str):
        return self.role if family == "player" else None

    def start_stream(self) -> FakeStream:
        if self.stream is not None:
            self.stream.stop()
        self.stream = FakeStream()
        return self.stream

    async def stop(self) -> bool:
        if self.stream is None:
            return False
        self.stream.stop()
        self.stream = None
        return True

    async def add_client(self, client: FakeClient) -> None:
        if client in self.clients:
            return
        await client.ungroup()
        self.clients.append(client)
        client.group = self

    async def remove_client(self, client: FakeClient) -> None:
        if client not in self.clients:
            return
        self.clients.remove(client)
        if not self.clients:
            await self.stop()
        FakeGroup(client)


class FakeClient:
    def __init__(self, client_id: str, name: str | None = None) -> None:
        self.client_id = client_id
        self.name = name or client_id
        self.is_connected = True
        self.available = True
        self.active_roles = ()
        self.player = FakePlayerRole()
        self.group: FakeGroup = FakeGroup(self)

    def roles_by_family(self, family: str):
        return [self.player] if family == "player" else []

    async def ungroup(self) -> None:
        await self.group.remove_client(self)


class FakeServer:
    def __init__(self) -> None:
        self._clients: dict[str, FakeClient] = {}
        self.urls: dict[str, str] = {}

    def add(self, client_id: str, url: str | None = None) -> FakeClient:
        client = FakeClient(client_id)
        self._clients[client_id] = client
        if url:
            self.urls[client_id] = url
        return client

    def remove(self, client_id: str) -> None:
        self._clients.pop(client_id)

    @property
    def clients(self) -> list[FakeClient]:
        return list(self._clients.values())

    def get_client(self, client_id: str) -> FakeClient | None:
        return self._clients.get(client_id)

    def get_client_url(self, client_id: str) -> str | None:
        return self.urls.get(client_id)


def make_config(
    *,
    groups: list[GroupState] | None = None,
    default_group: str | None = None,
    static: list[StaticClientConfig] | None = None,
) -> AppConfig:
    return AppConfig(
        server=ServerConfig(name="Test", server_id="test", sendspin_port=8927),
        mqtt=MqttConfig(
            host="127.0.0.1", port=1883, username=None, password=None,
            base_topic="sendspin/router", client_id="test",
        ),
        groups=groups
        if groups is not None
        else [
            GroupState(group_id="wohnzimmer", name="Wohnzimmer"),
            GroupState(group_id="bad", name="Bad"),
        ],
        sources=[
            SourceConfig(source_id="spotify", name="Spotify", uri="pipe:///tmp/spotify.pcm"),
            SourceConfig(source_id="mopidy", name="Mopidy", uri="pipe:///tmp/mopidy.pcm"),
        ],
        sendspin=SendspinConfig(identity_file="x", pairing_store="y"),
        clients=ClientsConfig(default_group=default_group, static=static or []),
    )
