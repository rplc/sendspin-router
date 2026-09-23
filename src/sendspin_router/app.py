from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from typing import Any

from . import __version__
from .audio import AudioRouter
from .config import AppConfig
from .mqtt import MqttController
from .sendspin_backend import SendspinBackend

_LOG = logging.getLogger(__name__)

_REFRESH_INTERVAL_S = 2.0


def _require(payload: dict, key: str) -> Any:
    if key not in payload:
        raise ValueError(f"Missing '{key}' in payload")
    return payload[key]


def _as_volume(value: Any) -> int:
    try:
        volume = round(float(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid volume: {value!r}") from exc
    return max(0, min(100, volume))


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in ("true", "1", "on"):
        return True
    if isinstance(value, str) and value.strip().lower() in ("false", "0", "off"):
        return False
    raise ValueError(f"Invalid boolean: {value!r}")


def _as_optional_id(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"Invalid id: {value!r}")
    return value


def _as_id_list(value: Any) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError("'members' must be a list of client id strings")
    return value


class RouterApp:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.backend = SendspinBackend(config)
        self.audio = AudioRouter(config.sources)
        self.mqtt = MqttController(config.mqtt, self.handle_command, self.publish_state)
        # Commands and the periodic refresh both mutate native Sendspin groups
        # across awaits; they must never interleave.
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._refresh_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.backend.on_change = self._wake.set
        await self.backend.start()
        await self.audio.start()
        async with self._lock:
            # Apply the stream assignments from YAML so playback is routed
            # without needing an MQTT command first.
            for group in self.config.groups:
                if group.stream:
                    await self._route_group_stream(group.group_id, group.stream)
            await self.backend.refresh()
        await self.mqtt.start()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="refresh")

    async def stop(self) -> None:
        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
        await self.audio.stop()
        await self.mqtt.stop()
        await self.backend.stop()

    async def _refresh_loop(self) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), _REFRESH_INTERVAL_S)
            self._wake.clear()
            try:
                async with self._lock:
                    await self.backend.refresh()
                await self.publish_state()
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOG.exception("State refresh failed")

    async def handle_command(self, command: str, payload: dict) -> None:
        try:
            async with self._lock:
                await self._dispatch(command, payload)
                await self.backend.refresh()
        except Exception as exc:
            _LOG.warning("MQTT command failed: %s %s: %s", command, payload, exc)
            await self.mqtt.publish_json(
                "event/command_error", {"command": command, "error": str(exc)}
            )
            return
        _LOG.info("MQTT command applied: %s %s", command, payload)
        await self.publish_state()
        await self.mqtt.publish_json("event/command_applied", {"command": command})

    async def _dispatch(self, command: str, payload: dict) -> None:
        parts = command.split("/")
        if len(parts) != 3 or parts[0] not in ("group", "client"):
            raise ValueError(f"Unknown command: {command}")
        target, target_id, action = parts
        backend = self.backend

        if target == "group":
            if action == "set_members":
                await backend.set_group_members(target_id, _as_id_list(_require(payload, "members")))
            elif action == "set_volume":
                await backend.set_group_volume(target_id, _as_volume(_require(payload, "volume")))
            elif action == "set_mute":
                await backend.set_group_mute(target_id, _as_bool(_require(payload, "mute")))
            elif action == "set_stream":
                source = _as_optional_id(_require(payload, "source"))
                await self._route_group_stream(target_id, source)
            else:
                raise ValueError(f"Unknown group command: {action}")
        else:
            if action == "set_group":
                await backend.set_client_group(target_id, _as_optional_id(_require(payload, "group")))
            elif action == "set_volume":
                await backend.set_client_volume(target_id, _as_volume(_require(payload, "volume")))
            elif action == "set_mute":
                await backend.set_client_mute(target_id, _as_bool(_require(payload, "mute")))
            else:
                raise ValueError(f"Unknown client command: {action}")

    async def _route_group_stream(self, group_id: str, source_id: str | None) -> None:
        """Point a group at a source: PCM fan-out (audio) plus PushStream (backend)."""
        await self.backend.set_group_stream(group_id, source_id)
        if source_id is None:
            await self.audio.unsubscribe(group_id)
        else:
            sink = functools.partial(self.backend.feed_group, group_id)
            await self.audio.subscribe(group_id, source_id, sink)

    async def publish_state(self) -> None:
        await self.mqtt.publish_json("state/clients", self.backend.list_clients(), retain=True)
        await self.mqtt.publish_json("state/groups", self.backend.list_groups(), retain=True)
        await self.mqtt.publish_json("state/sources", self.audio.state(), retain=True)
        await self.mqtt.publish_json("state/router", {"version": __version__}, retain=True)
