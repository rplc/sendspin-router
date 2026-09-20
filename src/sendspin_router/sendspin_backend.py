from __future__ import annotations

import asyncio
import base64
import logging
from pathlib import Path
from typing import Any

from .config import AppConfig
from .models import ClientInfo, GroupState

_LOG = logging.getLogger(__name__)


class SendspinBackend:
    """Sendspin server integration isolated from the router/control layer."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.server: Any | None = None
        self.clients: dict[str, ClientInfo] = {}
        self.groups: dict[str, GroupState] = {g.group_id: g for g in config.groups}
        self._started = False
        self._unsubscribe_events = None

    async def start(self) -> None:
        from aiosendspin.noise.keys import Identity
        from aiosendspin.noise.trust_store import FileServerPairingStore
        from aiosendspin.server.server import SendspinServer

        identity_path = Path(self.config.sendspin.identity_file)
        pairing_path = Path(self.config.sendspin.pairing_store)
        identity_path.parent.mkdir(parents=True, exist_ok=True)
        pairing_path.parent.mkdir(parents=True, exist_ok=True)

        if identity_path.exists():
            raw = base64.urlsafe_b64decode(identity_path.read_text(encoding="ascii") + "===")
            identity = Identity.from_private_bytes(raw)
        else:
            identity = Identity.generate()
            identity_path.write_text(identity.private_b64u, encoding="ascii")
            identity_path.chmod(0o600)
            _LOG.info("Created persistent Sendspin identity: %s", identity_path)

        pairing_store = await FileServerPairingStore.open(pairing_path)
        self.server = SendspinServer(
            loop=asyncio.get_running_loop(),
            identity=identity,
            server_name=self.config.server.name,
            pairing_store=pairing_store,
            allow_unencrypted=True,
            allow_noncompliant_clients=True,
        )
        self._unsubscribe_events = self.server.add_event_listener(self._on_event)
        await self.server.start_server(
            port=self.config.server.sendspin_port,
            discover_clients=True,
        )
        self._started = True
        _LOG.info("Sendspin server started: %s (id=%s)", self.config.server.name, self.server.id)

    async def stop(self) -> None:
        if self.server is None:
            return
        if self._unsubscribe_events:
            self._unsubscribe_events()
            self._unsubscribe_events = None
        await self.server.close()
        self.server = None
        self._started = False

    def _on_event(self, _server: Any, event: Any) -> None:
        cid = getattr(event, "client_id", None)
        if not cid or self.server is None:
            return
        event_name = event.__class__.__name__
        if event_name == "ClientRemovedEvent":
            self.clients.pop(cid, None)
            return
        client = self.server.get_client(cid)
        if client is None:
            return
        previous = self.clients.get(cid)
        self.clients[cid] = ClientInfo(
            client_id=cid,
            name=str(getattr(client, "name", None) or cid),
            available=bool(getattr(client, "is_connected", False)),
            roles=[str(getattr(r, "role_name", None) or r.__class__.__name__) for r in getattr(client, "active_roles", []) or []],
            group_id=previous.group_id if previous else None,
            volume=previous.volume if previous else None,
            mute=previous.mute if previous else None,
            offset_us=previous.offset_us if previous else 0,
            capabilities={},
        )

    async def refresh_clients(self) -> None:
        if self.server is None:
            return
        for client in self.server.clients:
            cid = client.client_id
            previous = self.clients.get(cid)
            self.clients[cid] = ClientInfo(
                client_id=cid,
                name=str(getattr(client, "name", None) or cid),
                available=bool(getattr(client, "is_connected", False)),
                roles=[str(getattr(r, "role_name", None) or r.__class__.__name__) for r in getattr(client, "active_roles", []) or []],
                group_id=previous.group_id if previous else None,
                volume=previous.volume if previous else None,
                mute=previous.mute if previous else None,
                offset_us=previous.offset_us if previous else 0,
                capabilities={},
            )

    def status(self) -> dict[str, Any]:
        return {"started": self._started, "clients": len(self.clients), "groups": len(self.groups), "server_id": self.server.id if self.server else None}

    def list_clients(self) -> dict[str, dict[str, Any]]:
        return {cid: vars(client).copy() for cid, client in self.clients.items()}

    def list_groups(self) -> dict[str, dict[str, Any]]:
        return {gid: vars(group).copy() for gid, group in self.groups.items()}

    async def set_group_members(self, group_id: str, members: list[str]) -> None:
        group = self._group(group_id)
        old = set(group.members)
        group.members = list(dict.fromkeys(str(x) for x in members))
        for cid in old | set(group.members):
            if cid in self.clients:
                self.clients[cid].group_id = group_id if cid in group.members else None

    async def set_group_volume(self, group_id: str, volume: int) -> None:
        self._group(group_id).volume = max(0, min(100, int(volume)))

    async def set_group_mute(self, group_id: str, mute: bool) -> None:
        self._group(group_id).mute = bool(mute)

    async def set_group_stream(self, group_id: str, source_id: str | None) -> None:
        if source_id is not None and source_id not in {s.source_id for s in self.config.sources}:
            raise ValueError(f"Unknown source: {source_id}")
        self._group(group_id).stream = source_id

    async def set_active_source(self, source_id: str | None) -> None:
        if source_id is not None and source_id not in {s.source_id for s in self.config.sources}:
            raise ValueError(f"Unknown source: {source_id}")
        self.config.router.active_source = source_id

    def _group(self, group_id: str) -> GroupState:
        try:
            return self.groups[group_id]
        except KeyError as exc:
            raise ValueError(f"Unknown group: {group_id}") from exc
