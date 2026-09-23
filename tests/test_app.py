from fakes import FakeServer, make_config

from sendspin_router.app import RouterApp


class FakeMqtt:
    def __init__(self) -> None:
        self.published: list[tuple[str, object, bool]] = []

    async def publish_json(self, suffix, payload, retain=False):
        self.published.append((suffix, payload, retain))

    def events(self, name):
        return [p for s, p, _ in self.published if s == f"event/{name}"]


def _app() -> tuple[RouterApp, FakeMqtt, FakeServer]:
    app = RouterApp(make_config())
    app.mqtt = FakeMqtt()
    server = FakeServer()
    app.backend.server = server
    return app, app.mqtt, server


async def test_group_commands_are_applied_and_state_published():
    app, mqtt, server = _app()
    server.add("a")
    await app.handle_command("group/wohnzimmer/set_members", {"members": ["a"]})
    await app.handle_command("group/wohnzimmer/set_volume", {"volume": 42.4})
    await app.handle_command("group/wohnzimmer/set_mute", {"mute": "true"})

    assert len(mqtt.events("command_applied")) == 3
    assert mqtt.events("command_error") == []
    groups = [p for s, p, _ in mqtt.published if s == "state/groups"][-1]
    assert groups["wohnzimmer"]["members"] == ["a"]
    assert groups["wohnzimmer"]["volume"] == 42
    assert groups["wohnzimmer"]["mute"] is True


async def test_set_stream_subscribes_audio():
    app, mqtt, _ = _app()
    await app.handle_command("group/bad/set_stream", {"source": "mopidy"})
    assert app.audio._group_source == {"bad": "mopidy"}
    await app.handle_command("group/bad/set_stream", {"source": None})
    assert app.audio._group_source == {}
    await app.audio.stop()


async def test_client_set_group_command():
    app, mqtt, server = _app()
    server.add("a")
    await app.handle_command("client/a/set_group", {"group": "bad"})
    assert app.backend.groups["bad"].members == ["a"]
    clients = [p for s, p, _ in mqtt.published if s == "state/clients"][-1]
    assert clients["a"]["group_id"] == "bad"


async def test_invalid_commands_report_errors():
    app, mqtt, _ = _app()
    await app.handle_command("router/set_active_source", {"source": "spotify"})
    await app.handle_command("group/wohnzimmer/set_volume", {})
    await app.handle_command("group/wohnzimmer/set_volume", {"volume": "loud"})
    await app.handle_command("group/wohnzimmer/set_mute", {"mute": "maybe"})
    await app.handle_command("group/nope/set_mute", {"mute": True})
    await app.handle_command("group/wohnzimmer/set_members", {"members": "a"})
    errors = mqtt.events("command_error")
    assert len(errors) == 6
    assert mqtt.events("command_applied") == []
