import json
from types import SimpleNamespace

from fakes import make_config

from sendspin_router.mqtt import MqttController


class FakeClient:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []

    async def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, retain))


def _controller(handler=None):
    async def noop(command, payload):
        pass

    controller = MqttController(make_config().mqtt, handler or noop)
    controller._client = FakeClient()
    return controller


async def test_retained_state_is_only_published_on_change():
    controller = _controller()
    await controller.publish_json("state/groups", {"a": 1}, retain=True)
    await controller.publish_json("state/groups", {"a": 1}, retain=True)
    await controller.publish_json("state/groups", {"a": 2}, retain=True)
    await controller.publish_json("event/x", {"a": 1})
    await controller.publish_json("event/x", {"a": 1})
    topics = [t for t, _, _ in controller._client.published]
    assert topics == ["sendspin/router/state/groups"] * 2 + ["sendspin/router/event/x"] * 2


async def test_commands_are_parsed_and_retained_ones_ignored():
    received = []

    async def handler(command, payload):
        received.append((command, payload))

    controller = _controller(handler)

    def msg(topic, payload, retain=False):
        return SimpleNamespace(topic=topic, payload=payload, retain=retain)

    await controller._handle_message(
        msg("sendspin/router/command/group/bad/set_volume", json.dumps({"volume": 5}).encode())
    )
    await controller._handle_message(msg("sendspin/router/command/group/bad/set_mute", b"{}", True))
    await controller._handle_message(msg("sendspin/router/command/group/bad/set_mute", b"nope"))
    await controller._handle_message(msg("sendspin/router/command/group/bad/set_mute", b"[1]"))
    assert received == [("group/bad/set_volume", {"volume": 5})]
