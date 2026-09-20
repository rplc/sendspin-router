from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import asdict
from typing import Any

from .config import AppConfig
from .models import ClientInfo, GroupState

_LOG = logging.getLogger(__name__)


class SendspinBackend:
    """Small isolation layer around aiosendspin.

    The public Sendspin protocol is stable enough for the application model,
    while the Python server API is still evolving. Keeping library-specific
    access here means the HTTP/API and ioBroker-facing code stays independent.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.server: Any | None = None
        self.clients: dict[str, ClientInfo] = {}
        self.groups: dict[str, GroupState] = {
            g.id: GroupState(g.id, g.name, list(g.members), g.volume, g.mute)
            for g in config.groups
        }
        self._started = False

    async def start(self) -> None:
        from aiosendspin.server import SendspinServer

        # aiosendspin 9.x exposes the server constructor used by the current
        # Sendspin bridge/reference implementations.
        self.server = SendspinServer(
            loop=asyncio.get_running_loop(),
            server_id=self.config.server.id,
            server_name=self.config.server.name,
        )
        await self.server.start_server(port=self.config.server.sendspin_port)
        self._started = True
        _LOG.info("Sendspin server started on port %s", self.config.server.sendspin_port)

    async def stop(self) -> None:
        if self.server is None:
            return
        stop = getattr(self.server, "stop_server", None) or getattr(self.server, "stop", None)
        if stop:
            result = stop()
            if inspect.isawaitable(result):
                await result
        self._started = False

    def status(self) -> dict[str, Any]:
        return {
            "started": self._started,
            "clients": len(self.clients),
            "groups": [asdict(g) for g in self.groups.values()],
            "aiosendspin": getattr(__import__("aiosendspin"), "__version__", "unknown"),
        }

    def list_clients(self) -> list[dict[str, Any]]:
        return [asdict(c) for c in self.clients.values()]

    def list_groups(self) -> list[dict[str, Any]]:
        return [asdict(g) for g in self.groups.values()]

    async def refresh_clients(self) -> None:
        """Refresh our registry from aiosendspin's live server object.

        This intentionally tolerates minor internal naming changes. It is
        only a discovery/read path; playback operations remain explicit.
        """
        if self.server is None:
            return
        candidates = []
        for attr in ("clients", "_clients"):
            obj = getattr(self.server, attr, None)
            if isinstance(obj, dict):
                candidates = list(obj.values())
                break
        for client in candidates:
            cid = str(getattr(client, "client_id", ""))
            if not cid:
                continue
            self.clients[cid] = ClientInfo(
                client_id=cid,
                name=str(getattr(client, "name", cid)),
                available=bool(getattr(client, "available", True)),
                roles=list(getattr(client, "supported_roles", []) or []),
            )

    async def set_group_volume(self, group_id: str, volume: int) -> None:
        group = self._group(group_id)
        group.volume = max(0, min(100, int(volume)))
        await self._apply_group_state(group)

    async def set_group_mute(self, group_id: str, mute: bool) -> None:
        group = self._group(group_id)
        group.mute = bool(mute)
        await self._apply_group_state(group)

    async def set_group_members(self, group_id: str, members: list[str]) -> None:
        group = self._group(group_id)
        group.members = list(dict.fromkeys(members))
        await self._apply_group_members(group)

    async def _apply_group_state(self, group: GroupState) -> None:
        # The protocol models volume/mute as group state. Exact helper names
        # vary across aiosendspin revisions, so this adapter tries the public
        # server/group surface first and otherwise leaves the state persisted.
        obj = self._find_group_object(group.group_id)
        if obj is None:
            _LOG.debug("Group %s has no live aiosendspin object yet", group.group_id)
            return
        for name, value in (("volume", group.volume), ("mute", group.mute)):
            setter = getattr(obj, f"set_{name}", None)
            if setter:
                result = setter(value)
                if inspect.isawaitable(result):
                    await result

    async def _apply_group_members(self, group: GroupState) -> None:
        obj = self._find_group_object(group.group_id)
        if obj is None:
            _LOG.debug("Group %s membership saved; live mapping will be applied when API is resolved", group.group_id)
            return
        setter = getattr(obj, "set_members", None)
        if setter:
            result = setter(group.members)
            if inspect.isawaitable(result):
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
