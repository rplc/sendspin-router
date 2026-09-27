from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .config import AppConfig, StaticClientConfig
from .models import ClientInfo, GroupState, SourceConfig

_LOG = logging.getLogger(__name__)

_UNSET = object()


@dataclass
class _GroupStream:
    """The PushStream currently feeding one logical group."""

    native_group: Any
    stream: Any
    source: SourceConfig
    audio_format: Any
    failed: bool = False


def _player_roles(client: Any) -> list[Any]:
    try:
        return list(client.roles_by_family("player"))
    except Exception:
        return []


def _static_url(cfg: StaticClientConfig) -> str:
    return f"ws://{cfg.host}:{cfg.port}/sendspin"


class SendspinBackend:
    """aiosendspin integration: clients, native groups, volume and audio feeds.

    The logical group membership (``GroupState.members``) is the single source
    of truth. ``reconcile()`` idempotently makes aiosendspin's native groups and
    PushStreams match it, so it is safe to call after every change and
    periodically (clients connecting, reconnecting or being cleaned up).

    Not thread-safe and not re-entrant: callers must serialize access (the
    router app holds one lock around commands and refreshes).
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.server: Any | None = None
        self.groups: dict[str, GroupState] = {g.group_id: g for g in config.groups}
        self.clients: dict[str, ClientInfo] = {}
        self._sources: dict[str, SourceConfig] = {s.source_id: s for s in config.sources}
        self._native_groups: dict[str, Any] = {}
        self._streams: dict[str, _GroupStream] = {}
        self._seen_clients: set[str] = set()
        self._last_live: dict[tuple[str, str, str], Any] = {}
        self._unsubscribe_events: Callable[[], None] | None = None
        # Called (synchronously, from the event loop) whenever aiosendspin
        # reports a client/group change, so the app can refresh immediately.
        self.on_change: Callable[[], None] | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

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
        self._unsubscribe_events = self.server.add_event_listener(self._on_server_event)
        await self.server.start_server(
            port=self.config.server.sendspin_port,
            discover_clients=True,
        )
        _LOG.info("Sendspin server started: %s (id=%s)", self.config.server.name, self.server.id)
        self._connect_static_clients()

    async def stop(self) -> None:
        if self.server is None:
            return
        if self._unsubscribe_events:
            try:
                self._unsubscribe_events()
            except Exception:
                _LOG.exception("Failed to unsubscribe Sendspin event listener")
            self._unsubscribe_events = None

        for group_id in list(self._streams):
            await self._stop_stream(group_id)
        try:
            await self.server.close()
        except Exception:
            _LOG.exception("Failed to close Sendspin server cleanly")
        finally:
            self.server = None
            self.clients.clear()
            self._native_groups.clear()

    def _connect_static_clients(self) -> None:
        """Dial configured headless clients (ESP boards without mDNS/pairing UI).

        aiosendspin 9.1.1's ``connect_to_client(url)`` is a plain method that
        starts the connection (and its retry loop) internally.
        """
        assert self.server is not None
        for cfg in self.config.clients.static:
            url = _static_url(cfg)
            _LOG.info("Connecting static Sendspin client %s (group=%s)", url, cfg.group or "-")
            try:
                self.server.connect_to_client(
                    url, retry_initial_connection=True, retry_indefinitely=True
                )
            except Exception:
                _LOG.exception("Failed to start connection for static client %s", url)

    def _on_server_event(self, _server: Any, _event: Any) -> None:
        if self.on_change is not None:
            self.on_change()

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def list_clients(self) -> dict[str, dict[str, Any]]:
        return {cid: info.to_dict() for cid, info in self.clients.items()}

    def list_groups(self, source_playing: dict[str, bool] | None = None) -> dict[str, dict[str, Any]]:
        """Return logical group state. Playback follows the selected source activity."""
        if source_playing is None:
            # Backward-compatible fallback for callers that do not have the
            # AudioRouter activity map (tests/tools). The application passes
            # the live source activity explicitly.
            source_playing = {
                g.stream: self._stream_active(gid)
                for gid, g in self.groups.items()
                if g.stream is not None
            }
        return {
            gid: {
                "id": gid,
                "name": g.name,
                "members": list(g.members),
                "volume": g.volume,
                "mute": g.mute,
                "stream": g.stream,
                "playback_state": (
                    "playing"
                    if (g.stream is not None and source_playing.get(g.stream, False))
                    else "stopped"
                ),
            }
            for gid, g in self.groups.items()
        }

    def group_of(self, client_id: str) -> str | None:
        return next((gid for gid, g in self.groups.items() if client_id in g.members), None)

    async def refresh(self) -> None:
        """Sync client snapshots, auto-assign new clients, reconcile all groups."""
        if self.server is None:
            return
        server_clients = {c.client_id: c for c in self.server.clients}

        for cid in server_clients.keys() - self._seen_clients:
            self._seen_clients.add(cid)
            self._auto_assign(cid)

        await self.reconcile()

        previous = self.clients
        self.clients = {}
        for cid, client in server_clients.items():
            old = previous.get(cid)
            players = _player_roles(client)
            live_volume = players[0].volume if players else None
            live_mute = players[0].muted if players else None
            self.clients[cid] = ClientInfo(
                client_id=cid,
                name=str(getattr(client, "name", None) or cid),
                available=bool(getattr(client, "is_connected", False)),
                roles=[str(getattr(r, "role_id", None) or type(r).__name__)
                       for r in getattr(client, "active_roles", ()) or ()],
                group_id=self.group_of(cid),
                volume=self._follow_live(("client", cid, "volume"), live_volume,
                                         old.volume if old else None),
                mute=self._follow_live(("client", cid, "mute"), live_mute,
                                       old.mute if old else None),
            )

        for gid, group in self.groups.items():
            role = self._player_group_role(gid)
            if role is None or not role.get_player_clients():
                continue
            group.volume = self._follow_live(("group", gid, "volume"),
                                             role.get_group_volume(), group.volume)
            group.mute = self._follow_live(("group", gid, "mute"),
                                           role.get_group_muted(), group.mute)

    def _follow_live(self, key: tuple[str, str, str], live: Any, current: Any) -> Any:
        """Report ``current`` unless the live device value changed since last seen.

        A command updates ``current`` immediately while the device confirms
        asynchronously; without this, the old live value would be published
        in between and the UI control would jump back and forth.
        """
        if live is None or self._last_live.get(key, _UNSET) == live:
            return current
        self._last_live[key] = live
        return live

    def _auto_assign(self, client_id: str) -> None:
        """Put a newly seen, unassigned client into its static/default group.

        Runs once per client, so a client that was deliberately removed from
        its group does not bounce back.
        """
        if self.group_of(client_id) is not None:
            return
        group_id = self._static_group_for(client_id) or self.config.clients.default_group
        if not group_id or group_id not in self.groups:
            return
        _LOG.info("Auto-assigning client '%s' to group '%s'", client_id, group_id)
        self.groups[group_id].members.append(client_id)

    def _static_group_for(self, client_id: str) -> str | None:
        assert self.server is not None
        url = self.server.get_client_url(client_id)
        if not url:
            return None
        host = urlparse(url).hostname
        for cfg in self.config.clients.static:
            if url == _static_url(cfg) or host == cfg.host:
                return cfg.group
        return None

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    async def set_group_members(self, group_id: str, members: list[str]) -> None:
        group = self._group(group_id)
        desired = list(dict.fromkeys(str(m) for m in members))
        for other_id, other in self.groups.items():
            if other_id != group_id and any(m in other.members for m in desired):
                other.members = [m for m in other.members if m not in desired]
        group.members = desired
        await self.reconcile(first=group_id)

    async def set_client_group(self, client_id: str, group_id: str | None) -> None:
        if group_id is not None:
            self._group(group_id)
        for group in self.groups.values():
            if client_id in group.members:
                group.members.remove(client_id)
        if group_id is not None:
            self.groups[group_id].members.append(client_id)
        await self.reconcile(first=group_id)

    async def set_group_volume(self, group_id: str, volume: int) -> None:
        group = self._group(group_id)
        group.volume = volume
        role = self._player_group_role(group_id)
        if role is not None:
            role.set_group_volume(volume)

    async def set_group_mute(self, group_id: str, mute: bool) -> None:
        group = self._group(group_id)
        group.mute = mute
        role = self._player_group_role(group_id)
        if role is not None:
            role.set_group_muted(mute)

    async def set_client_volume(self, client_id: str, volume: int) -> None:
        for role in _player_roles(self._client(client_id)):
            role.set_player_volume(volume)
        if client_id in self.clients:
            self.clients[client_id].volume = volume

    async def set_client_mute(self, client_id: str, mute: bool) -> None:
        for role in _player_roles(self._client(client_id)):
            role.set_player_mute(mute)
        if client_id in self.clients:
            self.clients[client_id].mute = mute

    async def set_group_stream(self, group_id: str, source_id: str | None) -> None:
        group = self._group(group_id)
        if source_id is not None and source_id not in self._sources:
            raise ValueError(f"Unknown source: {source_id}")
        group.stream = source_id
        await self._sync_stream(group_id)

    def _group(self, group_id: str) -> GroupState:
        try:
            return self.groups[group_id]
        except KeyError as exc:
            raise ValueError(f"Unknown group: {group_id}") from exc

    def _client(self, client_id: str) -> Any:
        client = self.server.get_client(client_id) if self.server is not None else None
        if client is None:
            raise ValueError(f"Unknown client: {client_id}")
        return client

    def _player_group_role(self, group_id: str) -> Any | None:
        native = self._native_groups.get(group_id)
        return native.group_role("player") if native is not None else None

    # ------------------------------------------------------------------
    # Native group reconciliation
    # ------------------------------------------------------------------

    async def reconcile(self, first: str | None = None) -> None:
        """Make all native groups match the logical membership.

        ``first`` is reconciled before the others: when clients move between
        groups, the destination must pick them up before the source group
        looks for a new anchor.
        """
        order = sorted(self.groups, key=lambda gid: gid != first)
        for group_id in order:
            try:
                await self._reconcile_group(group_id)
            except Exception:
                _LOG.exception("Reconciling group '%s' failed", group_id)

    async def _reconcile_group(self, group_id: str) -> None:
        if self.server is None:
            return
        group = self.groups[group_id]
        member_ids = set(group.members)
        # Clients that switched to an external source are moved out of their
        # group by aiosendspin itself; do not force them back in.
        members = [
            c for c in (self.server.get_client(cid) for cid in group.members)
            if c is not None and getattr(c, "available", True)
        ]
        if not members:
            if group_id in self._native_groups:
                await self._stop_stream(group_id)
                del self._native_groups[group_id]
            return

        native = self._native_groups.get(group_id)
        if native is None or not any(c in native.clients for c in members):
            anchor = members[0]
            # A native group belongs to at most one logical group. If the
            # anchor still shares its native group with foreign clients, or
            # that group is registered for another logical group (and maybe
            # streams its source), start from a fresh solo group instead.
            owned_elsewhere = any(
                n is anchor.group for gid, n in self._native_groups.items() if gid != group_id
            )
            if owned_elsewhere or any(
                c.client_id not in member_ids for c in anchor.group.clients
            ):
                await anchor.ungroup()
            native = anchor.group
            if group_id in self._native_groups:
                _LOG.info("Group '%s' moved to native Sendspin group %s",
                          group_id, native.group_id)
            self._native_groups[group_id] = native

        for client in members:
            if client not in native.clients:
                _LOG.info("Adding client '%s' to group '%s'", client.client_id, group_id)
                await native.add_client(client)
        for client in list(native.clients):
            if client.client_id not in member_ids:
                _LOG.info("Removing client '%s' from group '%s'", client.client_id, group_id)
                await client.ungroup()

        await self._sync_stream(group_id)

    # ------------------------------------------------------------------
    # Audio
    # ------------------------------------------------------------------

    def _stream_active(self, group_id: str) -> bool:
        current = self._streams.get(group_id)
        return current is not None and not current.stream.is_stopped

    async def _sync_stream(self, group_id: str) -> None:
        """Start, restart or stop the group's PushStream to match its state."""
        group = self.groups[group_id]
        native = self._native_groups.get(group_id)
        source = self._sources.get(group.stream) if group.stream else None
        if source is None or native is None:
            await self._stop_stream(group_id)
            return

        current = self._streams.get(group_id)
        if (
            current is not None
            and current.native_group is native
            and current.source is source
            and not current.stream.is_stopped
        ):
            return

        from aiosendspin.audio.format import AudioFormat

        # start_stream() replaces any previous stream of this native group.
        stream = native.start_stream()
        # FIFO-backed sources are realtime: keep the startup lead at the
        # client's minimum buffer instead of accumulating latency.
        stream.set_live_source(True)
        if current is not None and current.native_group is not native:
            current.stream.stop()
        self._streams[group_id] = _GroupStream(
            native_group=native,
            stream=stream,
            source=source,
            audio_format=AudioFormat(
                sample_rate=source.sample_rate,
                bit_depth=source.bit_depth,
                channels=source.channels,
            ),
        )
        _LOG.info("Group '%s' streaming source '%s' (native group %s)",
                  group_id, source.source_id, native.group_id)

    async def _stop_stream(self, group_id: str) -> None:
        current = self._streams.pop(group_id, None)
        if current is None:
            return
        try:
            if current.native_group is self._native_groups.get(group_id):
                await current.native_group.stop()  # also tells clients "stopped"
            else:
                current.stream.stop()
        except Exception:
            _LOG.exception("Failed to stop stream for group '%s'", group_id)
        _LOG.info("Group '%s' stopped streaming", group_id)

    async def feed_group(self, group_id: str, pcm_chunk: bytes) -> None:
        """Commit one PCM chunk (whole frames) to the group's PushStream."""
        current = self._streams.get(group_id)
        if current is None or current.stream.is_stopped or not pcm_chunk:
            return
        try:
            current.stream.prepare_audio(pcm_chunk, current.audio_format)
            await current.stream.commit_audio()
        except Exception:
            # Log once per stream; this runs ~50 times per second.
            if not current.failed:
                current.failed = True
                _LOG.exception("PushStream audio commit failed for group '%s'", group_id)
