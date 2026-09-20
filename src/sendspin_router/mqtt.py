from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from aiomqtt import Client, Message

from .config import MqttConfig

_LOG = logging.getLogger(__name__)

CommandHandler = Callable[[str, dict], Awaitable[None]]


class MqttController:
    """MQTT state/event publisher and command subscriber."""

    def __init__(self, config: MqttConfig, command_handler: CommandHandler) -> None:
        self.config = config
        self.command_handler = command_handler
        self.client: Client | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        kwargs = {
            "hostname": self.config.host,
            "port": self.config.port,
            "identifier": self.config.client_id,
        }
        if self.config.username:
            kwargs["username"] = self.config.username
        if self.config.password:
            kwargs["password"] = self.config.password

        self.client = Client(**kwargs)
        await self.client.__aenter__()
        await self.client.subscribe(f"{self.config.base_topic}/command/#")
        self._task = asyncio.create_task(self._consume(), name="mqtt-command-consumer")
        _LOG.info("MQTT connected to %s:%s", self.config.host, self.config.port)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self.client:
            await self.client.__aexit__(None, None, None)
            self.client = None

    async def _consume(self) -> None:
        assert self.client is not None
        async for message in self.client.messages:
            await self._handle_message(message)

    async def _handle_message(self, message: Message) -> None:
        prefix = f"{self.config.base_topic}/command/"
        topic = str(message.topic)
        if not topic.startswith(prefix):
            return

        command = topic[len(prefix):]
        try:
            payload = json.loads(message.payload.decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            _LOG.warning("Ignoring invalid MQTT JSON on %s: %s", topic, exc)
            return

        if not isinstance(payload, dict):
            _LOG.warning("Ignoring non-object MQTT payload on %s", topic)
            return

        await self.command_handler(command, payload)

    async def publish_json(self, suffix: str, payload: dict, retain: bool = False) -> None:
        if not self.client:
            return
        topic = f"{self.config.base_topic}/{suffix.lstrip('/')}"
        await self.client.publish(
            topic,
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            qos=1,
            retain=retain,
        )
