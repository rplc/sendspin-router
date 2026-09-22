from __future__ import annotations

import asyncio
import functools
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

        # Apply any stream assignments already present in the YAML config
        # (e.g. Wohnzimmer+Bad -> Mopidy, Schlafzimmer -> Spotify) so
        # playback starts routed correctly without needing an MQTT command
        # first.
        for group in self.config.groups:
            if group.stream:
                await self._route_group_stream(group.group_id, group.stream)

        await self.publish_state()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="client-refresh")

    async def stop(self) -> None:
        if self._refresh_task:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
        await self.audio.stop()
        await self.mqtt.stop()
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
                    await self._route_group_stream(group_id, payload.get("source"))
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

    async def _route_group_stream(self, group_id: str, source_id: str | None) -> None:
        """Update both the control-plane model (backend) and the actual PCM
        fan-out (audio) for a group's stream assignment.

        This is what makes independent, simultaneous streams per group work:
        each group gets its own subscription to a source's PCM feed, so
        Wohnzimmer/Bad can play Mopidy while Schlafzimmer plays Spotify at
        the same time -- there's no single global "active source" involved.
        """
        await self.backend.set_group_stream(group_id, source_id)
        if source_id is None:
            await self.audio.unsubscribe(group_id)
            await self.backend.detach_group_audio(group_id)
        else:
            sink = functools.partial(self.backend.feed_group, group_id)
            await self.audio.subscribe(group_id, source_id, sink)
            await self.backend.attach_group_audio(group_id, source_id)

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
                "version": "0.3.6",
            },
            retain=True,
        )
