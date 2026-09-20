from __future__ import annotations

import asyncio
import base64
import logging
from pathlib import Path
from typing import Any

from .config import AppConfig
from .models import ClientInfo, GroupState

_LOG = logging.getLogger(__name__)


def _client_host(client: Any) -> str | None:
    """Best-effort extraction of a connected client's remote host/IP.

    aiosendspin does not (as far as I could verify) document a single
    canonical attribute name for this, so we try a few plausible ones.
    Used only to match a connected client back to a `clients.static` config
    entry -- if none of these match on your installed version, static
    clients still get created/connected, they just won't be auto-matched by
    IP for group assignment (you can still assign them via MQTT set_members).
    """
    for attr in ("remote_address", "remote_ip", "address", "host", "ip"):
        value = getattr(client, attr, None)
        if not value:
            continue
        if isinstance(value, (tuple, list)) and value:
            return str(value[0])
        return str(value)
    return None


class SendspinBackend:
    """Sendspin server integration isolated from the router/control layer."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.server: Any | None = None
        self.clients: dict[str, ClientInfo] = {}
        self.groups: dict[str, GroupState] = {g.group_id: g for g in config.groups}
        self._started = False
        self._unsubscribe_events = None
        self._static_by_host = {c.host: c for c in config.clients.static}
        self._default_group = config.clients.default_group
        # group_id -> whatever aiosendspin object represents "this group's
        # audio feed" (a PushStream, most likely). See attach_group_audio().
        self._group_streams: dict[str, Any] = {}
        # Logical router group id -> native aiosendspin SendspinGroup.
        self._native_groups: dict[str, Any] = {}

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

        await self.connect_static_clients()

    async def stop(self) -> None:
        if self.server is None:
            return

        if self._unsubscribe_events:
            try:
                self._unsubscribe_events()
            except Exception:
                _LOG.exception("Failed to unsubscribe Sendspin event listener")
            finally:
                self._unsubscribe_events = None

        # Stop all group streams before closing the Sendspin server.
        for group_id in list(self._group_streams):
            await self.detach_group_audio(group_id)

        try:
            await self.server.close()
        except Exception:
            _LOG.exception("Failed to close Sendspin server cleanly")
        finally:
            self.server = None
            self._started = False
            self.clients.clear()


    # ------------------------------------------------------------------
    # Static (headless) client connection
    # ------------------------------------------------------------------

    async def connect_static_clients(self) -> None:
        """Connect configured headless clients by their fixed Sendspin URL.

        aiosendspin 9.1.1 exposes ``connect_to_client(url)`` as a regular
        method that starts the connection internally and returns ``None``.
        It is therefore deliberately *not* awaited and must not be wrapped
        in ``asyncio.create_task``.  The library owns the reconnect loop when
        ``retry_indefinitely`` is enabled.
        """
        if self.server is None:
            return

        for client_cfg in self._static_by_host.values():
            url = f"ws://{client_cfg.host}:{client_cfg.port}/sendspin"
            _LOG.info(
                "Connecting static Sendspin client '%s' (group=%s)",
                url,
                client_cfg.group or "none",
            )
            try:
                self.server.connect_to_client(
                    url,
                    retry_initial_connection=True,
                    retry_indefinitely=True,
                )
            except Exception:
                _LOG.exception("Failed to start connection for static client %s", url)


    async def _auto_assign_group(self, client: Any, info: ClientInfo) -> None:
        """Put a newly-seen client into its configured/default group."""
        host = _client_host(client)
        static_cfg = None
        if self.server is not None:
            url = self.server.get_client_url(info.client_id)
            if url:
                static_cfg = next(
                    (cfg for cfg in self._static_by_host.values()
                     if url == f"ws://{cfg.host}:{cfg.port}/sendspin"),
                    None,
                )
        if static_cfg is None and host:
            static_cfg = self._static_by_host.get(host)
        group_id = static_cfg.group if static_cfg else self._default_group
        if not group_id or group_id not in self.groups:
            return
        group = self.groups[group_id]
        if info.client_id in group.members:
            return
        _LOG.info(
            "Auto-assigning client '%s' (%s) to group '%s'",
            info.client_id,
            host or "unknown host",
            group_id,
        )
        try:
            await self.set_group_members(group_id, [*group.members, info.client_id])
        except Exception:
            _LOG.exception("Auto-assign of '%s' to group '%s' failed", info.client_id, group_id)

    # ------------------------------------------------------------------
    # Client/group bookkeeping
    # ------------------------------------------------------------------

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
            info = ClientInfo(
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
            self.clients[cid] = info
            if info.group_id is None:
                await self._auto_assign_group(client, info)

    def status(self) -> dict[str, Any]:
        return {"started": self._started, "clients": len(self.clients), "groups": len(self.groups), "server_id": self.server.id if self.server else None}

    def list_clients(self) -> dict[str, dict[str, Any]]:
        return {cid: vars(client).copy() for cid, client in self.clients.items()}

    def list_groups(self) -> dict[str, dict[str, Any]]:
        return {gid: vars(group).copy() for gid, group in self.groups.items()}

    async def set_group_members(self, group_id: str, members: list[str]) -> None:
        """Update the logical group and mirror it to aiosendspin's native group."""
        group = self._group(group_id)
        desired = list(dict.fromkeys(str(x) for x in members))
        old = set(group.members)

        if self.server is None:
            group.members = desired
            return

        # Capture the old logical assignment before changing it. This matters when
        # a client is moved from one logical group to another and the destination
        # has not yet got a native Sendspin group.
        previous_logical = {
            cid: (self.clients[cid].group_id if cid in self.clients else None)
            for cid in desired
        }

        desired_clients = [self.server.get_client(cid) for cid in desired]
        desired_clients = [client for client in desired_clients if client is not None]

        group.members = desired
        for cid in old | set(desired):
            if cid in self.clients:
                self.clients[cid].group_id = group_id if cid in desired else None

        if not desired_clients:
            self._native_groups.pop(group_id, None)
            if group_id in self._group_streams:
                await self.detach_group_audio(group_id)
            return

        native_group = self._native_groups.get(group_id)
        if native_group is None:
            # Choose a client that was not already assigned to another logical
            # group. If every desired client belongs elsewhere, explicitly move
            # the first one to a fresh solo group and use that as our anchor.
            anchor = next(
                (c for c in desired_clients if previous_logical.get(c.client_id) in (None, group_id)),
                desired_clients[0],
            )
            if previous_logical.get(anchor.client_id) not in (None, group_id):
                await anchor.ungroup()
            native_group = anchor.group
            self._native_groups[group_id] = native_group

        # Add desired members. aiosendspin removes each client from its old native
        # group first and, if this group is already playing, joins its active stream.
        for client in desired_clients:
            if client not in native_group.clients:
                await native_group.add_client(client)

        # Members that left the logical group are placed into fresh Sendspin solo
        # groups, matching aiosendspin's normal grouping semantics.
        for cid in old - set(desired):
            client = self.server.get_client(cid)
            if client is not None and client in native_group.clients:
                await client.ungroup()

        if group.stream and group_id not in self._group_streams:
            await self.attach_group_audio(group_id, group.stream)

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

    # ------------------------------------------------------------------
    # Audio: feeding PCM into aiosendspin's native SendspinGroup
    # ------------------------------------------------------------------

    async def attach_group_audio(self, group_id: str, source_id: str) -> None:
        if self.server is None:
            return
        source = next((s for s in self.config.sources if s.source_id == source_id), None)
        if source is None:
            raise ValueError(f"Unknown source: {source_id}")

        await self.detach_group_audio(group_id)
        native_group = self._native_groups.get(group_id)
        if native_group is None or not native_group.clients:
            _LOG.info(
                "Group '%s' has no connected/registered Sendspin client yet; "
                "PushStream will be created when the first client joins",
                group_id,
            )
            return

        stream = native_group.start_stream()
        # FIFO-backed sources are realtime. This keeps the Sendspin startup lead
        # at the client's minimum buffer instead of accumulating unnecessary latency.
        stream.set_live_source(True)
        self._group_streams[group_id] = stream
        _LOG.info(
            "Group '%s' now streaming source '%s' via native Sendspin group %s",
            group_id,
            source_id,
            native_group.group_id,
        )

    async def detach_group_audio(self, group_id: str) -> None:
        stream = self._group_streams.pop(group_id, None)
        if stream is None:
            return
        try:
            stream.stop()
        except Exception:
            _LOG.exception("Failed to stop push stream for group '%s'", group_id)

    async def feed_group(self, group_id: str, pcm_chunk: bytes) -> None:
        """Prepare one raw PCM chunk and commit it to the group's PushStream."""
        stream = self._group_streams.get(group_id)
        if stream is None or not pcm_chunk:
            return
        group_cfg = self._group(group_id)
        source_id = group_cfg.stream
        if source_id is None:
            return
        source = next((s for s in self.config.sources if s.source_id == source_id), None)
        if source is None:
            return

        try:
            from aiosendspin.audio.format import AudioFormat

            audio_format = AudioFormat(
                sample_rate=source.sample_rate,
                bit_depth=source.bit_depth,
                channels=source.channels,
            )
            stream.prepare_audio(pcm_chunk, audio_format)
            await stream.commit_audio()
            # Keep the live stream close to realtime and avoid building an
            # unbounded server-side lead if an upstream FIFO bursts.
            await stream.sleep_to_limit_buffer(500_000)
        except Exception:
            _LOG.exception("PushStream audio commit failed for group '%s'", group_id)


