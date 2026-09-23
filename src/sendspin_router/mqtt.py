from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aiomqtt

from .config import MqttConfig

_LOG = logging.getLogger(__name__)

CommandHandler = Callable[[str, dict], Awaitable[None]]
ConnectHandler = Callable[[], Awaitable[None]]

_RECONNECT_DELAY_S = 5
AVAILABILITY_SUFFIX = "availability"


class MqttController:
    """MQTT state/event publisher and command subscriber.

    Keeps reconnecting in the background, so the router keeps playing audio
    while the broker (e.g. ioBroker) restarts. ``availability`` is a retained
    ``online``/``offline`` topic backed by an MQTT last will.

    Retained state is only published when its payload changed; after every
    (re)connect the full state is published again via ``on_connect``.
    """

    def __init__(
        self,
        config: MqttConfig,
        command_handler: CommandHandler,
        on_connect: ConnectHandler | None = None,
    ) -> None:
        self.config = config
        self.command_handler = command_handler
        self.on_connect = on_connect
        self._client: aiomqtt.Client | None = None
        self._task: asyncio.Task[None] | None = None
        self._retained: dict[str, str] = {}  # topic -> last published payload

    def _topic(self, suffix: str) -> str:
        return f"{self.config.base_topic}/{suffix.lstrip('/')}"

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="mqtt")

    async def stop(self) -> None:
        if self._client is not None:
            try:
                await self._client.publish(
                    self._topic(AVAILABILITY_SUFFIX), "offline", qos=1, retain=True
                )
            except aiomqtt.MqttError:
                pass
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        availability = self._topic(AVAILABILITY_SUFFIX)
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self.config.host,
                    port=self.config.port,
                    username=self.config.username or None,
                    password=self.config.password or None,
                    identifier=self.config.client_id,
                    will=aiomqtt.Will(availability, "offline", qos=1, retain=True),
                ) as client:
                    self._client = client
                    self._retained.clear()
                    await client.subscribe(self._topic("command/#"), qos=1)
                    await client.publish(availability, "online", qos=1, retain=True)
                    _LOG.info("MQTT connected to %s:%s", self.config.host, self.config.port)
                    if self.on_connect is not None:
                        await self.on_connect()
                    async for message in client.messages:
                        await self._handle_message(message)
            except aiomqtt.MqttError as exc:
                _LOG.warning(
                    "MQTT connection to %s:%s lost/failed (%s); retrying in %ss",
                    self.config.host,
                    self.config.port,
                    exc,
                    _RECONNECT_DELAY_S,
                )
            finally:
                self._client = None
            await asyncio.sleep(_RECONNECT_DELAY_S)

    async def _handle_message(self, message: aiomqtt.Message) -> None:
        prefix = self._topic("command/")
        topic = str(message.topic)
        if not topic.startswith(prefix):
            return
        if message.retain:
            # A retained command would be replayed on every reconnect.
            _LOG.warning("Ignoring retained MQTT command on %s", topic)
            return

        command = topic[len(prefix):]
        raw = message.payload
        try:
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw or "")
            payload = json.loads(text or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            _LOG.warning("Ignoring invalid MQTT JSON on %s: %s", topic, exc)
            return
        if not isinstance(payload, dict):
            _LOG.warning("Ignoring non-object MQTT payload on %s", topic)
            return

        try:
            await self.command_handler(command, payload)
        except Exception:
            _LOG.exception("Unhandled error in MQTT command %s", command)

    async def publish_json(self, suffix: str, payload: Any, retain: bool = False) -> None:
        """Publish JSON; retained topics are skipped when unchanged."""
        client = self._client
        if client is None:
            return
        topic = self._topic(suffix)
        text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, sort_keys=True)
        if retain and self._retained.get(topic) == text:
            return
        try:
            await client.publish(topic, text, qos=1, retain=retain)
        except aiomqtt.MqttError as exc:
            _LOG.debug("MQTT publish to %s failed: %s", topic, exc)
            return
        if retain:
            self._retained[topic] = text
