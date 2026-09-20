from __future__ import annotations

import asyncio
import logging

from .audio import AudioRouter
from .config import AppConfig
from .mqtt import MqttController
from .sendspin_backend import SendspinBackend

_LOG = logging.getLogger(__name__)


class RouterApp:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.backend = SendspinBackend(config)
        self.audio = AudioRouter(config.sources)
        self.mqtt = MqttController(config.mqtt, self.handle_command)
        self._refresh_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self.backend.start()
        await self.audio.start()
        await self.mqtt.start()
        await self.publish_state()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="client-refresh")

    async def stop(self) -> None:
        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
        await self.mqtt.stop()
        await self.audio.stop()
        await self.backend.stop()

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await self.backend.refresh_clients()
                await self.publish_state()
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOG.exception("Client state refresh failed")
            await asyncio.sleep(2)

    async def handle_command(self, command: str, payload: dict) -> None:
        try:
            if command == "router/set_active_source":
                await self.backend.set_active_source(payload.get("source"))
            elif command.startswith("group/"):
                parts = command.split("/")
                if len(parts) != 3:
                    raise ValueError(f"Invalid group command: {command}")
                _, group_id, action = parts
                if action == "set_members":
                    await self.backend.set_group_members(group_id, payload["members"])
                elif action == "set_volume":
                    await self.backend.set_group_volume(group_id, payload["volume"])
                elif action == "set_mute":
                    await self.backend.set_group_mute(group_id, payload["mute"])
                elif action == "set_stream":
                    await self.backend.set_group_stream(group_id, payload.get("source"))
                else:
                    raise ValueError(f"Unknown group command: {action}")
            else:
                raise ValueError(f"Unknown command: {command}")
        except Exception as exc:
            _LOG.warning("MQTT command failed: %s: %s", command, exc)
            await self.mqtt.publish_json(
                "event/command_error",
                {"command": command, "error": str(exc)},
                retain=False,
            )
            return

        await self.publish_state()
        await self.mqtt.publish_json(
            "event/command_applied",
            {"command": command},
            retain=False,
        )

    async def publish_state(self) -> None:
        await self.mqtt.publish_json(
            "state/clients",
            self.backend.list_clients(),
            retain=True,
        )
        await self.mqtt.publish_json(
            "state/groups",
            self.backend.list_groups(),
            retain=True,
        )
        await self.mqtt.publish_json(
            "state/sources",
            self.audio.state(),
            retain=True,
        )
        await self.mqtt.publish_json(
            "state/router",
            {
                "active_source": self.config.router.active_source,
                "version": "0.2.0",
            },
            retain=True,
        )
