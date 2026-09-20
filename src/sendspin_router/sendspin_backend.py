from __future__ import annotations

import asyncio
import logging
from typing import Any

from .config import AppConfig
from .models import ClientInfo, GroupState

_LOG = logging.getLogger(__name__)


class SendspinBackend:
    """Isolation layer around the concrete aiosendspin server API."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.server: Any | None = None
        self.clients: dict[str, ClientInfo] = {}
        self.groups: dict[str, GroupState] = {
            g.group_id: g for g in config.groups
        }
        self._started = False

    async def start(self) -> None:
        # This import path is confirmed by current Sendspin examples.
        from aiosendspin.server.server import SendspinServer

        self.server = SendspinServer(
            loop=asyncio.get_running_loop(),
            server_id=self.config.server.server_id,
            server_name=self.config.server.name,
        )
        await self.server.start_server(port=self.config.server.sendspin_port)
        self._started = True
        _LOG.info(
            "Sendspin server started on port %s",
            self.config.server.sendspin_port,
        )

    async def stop(self) -> None:
        if self.server is None:
            return
        stop = getattr(self.server, "stop_server", None) or getattr(self.server, "stop", None)
        if stop:
            result = stop()
            if asyncio.iscoroutine(result):
                await result
        self._started = False

    def status(self) -> dict[str, Any]:
        return {
            "started": self._started,
            "clients": len(self.clients),
            "groups": len(self.groups),
        }

    def list_clients(self) -> dict[str, dict[str, Any]]:
        return {cid: vars(client).copy() for cid, client in self.clients.items()}

    def list_groups(self) -> dict[str, dict[str, Any]]:
        return {gid: vars(group).copy() for gid, group in self.groups.items()}

    async def refresh_clients(self) -> None:
        """Best-effort registry refresh.

        The exact internal client collection is deliberately isolated here.
        Once aiosendspin's public event API is wired, this method can be
        replaced without changing the MQTT contract.
        """
        if self.server is None:
            return

        candidates = None
        for attr in ("clients", "_clients"):
            obj = getattr(self.server, attr, None)
            if isinstance(obj, dict):
                candidates = obj.values()
                break

        if candidates is None:
            return

        for client in candidates:
            cid = str(
                getattr(client, "client_id", None)
                or getattr(client, "id", None)
                or ""
            )
            if not cid:
                continue
            self.clients[cid] = ClientInfo(
                client_id=cid,
                name=str(getattr(client, "name", cid)),
                available=bool(getattr(client, "available", True)),
                roles=list(getattr(client, "supported_roles", []) or []),
            )

    async def set_group_members(self, group_id: str, members: list[str]) -> None:
        group = self._group(group_id)
        group.members = list(dict.fromkeys(str(x) for x in members))
        await self._apply_group_members(group)

    async def set_group_volume(self, group_id: str, volume: int) -> None:
        group = self._group(group_id)
        group.volume = max(0, min(100, int(volume)))
        await self._apply_group_state(group)

    async def set_group_mute(self, group_id: str, mute: bool) -> None:
        group = self._group(group_id)
        group.mute = bool(mute)
        await self._apply_group_state(group)

    async def set_group_stream(self, group_id: str, source_id: str | None) -> None:
        group = self._group(group_id)
        if source_id is not None and source_id not in {s.source_id for s in self.config.sources}:
            raise ValueError(f"Unknown source: {source_id}")
        group.stream = source_id

    async def set_active_source(self, source_id: str | None) -> None:
        if source_id is not None and source_id not in {s.source_id for s in self.config.sources}:
            raise ValueError(f"Unknown source: {source_id}")
        self.config.router.active_source = source_id

    async def _apply_group_state(self, group: GroupState) -> None:
        obj = self._find_group_object(group.group_id)
        if obj is None:
            _LOG.debug("No live Sendspin group object for %s yet", group.group_id)
            return

        for attr, value in (("volume", group.volume), ("mute", group.mute)):
            setter = getattr(obj, f"set_{attr}", None)
            if setter:
                result = setter(value)
                if asyncio.iscoroutine(result):
                    await result

    async def _apply_group_members(self, group: GroupState) -> None:
        obj = self._find_group_object(group.group_id)
        if obj is None:
            _LOG.debug("No live Sendspin group object for %s yet", group.group_id)
            return

        setter = getattr(obj, "set_members", None)
        if setter:
            result = setter(group.members)
            if asyncio.iscoroutine(result):
                await result

    def _find_group_object(self, group_id: str) -> Any | None:
        if self.server is None:
            return None
        for attr in ("groups", "_groups"):
            groups = getattr(self.server, attr, None)
            if isinstance(groups, dict):
                return groups.get(group_id)
        return None

    def _group(self, group_id: str) -> GroupState:
        try:
            return self.groups[group_id]
        except KeyError as exc:
            raise ValueError(f"Unknown group: {group_id}") from exc
