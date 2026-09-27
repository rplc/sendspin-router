import pytest
from fakes import FakeServer, make_config

from sendspin_router.config import StaticClientConfig
from sendspin_router.models import GroupState
from sendspin_router.sendspin_backend import SendspinBackend


def _backend(**kwargs) -> tuple[SendspinBackend, FakeServer]:
    backend = SendspinBackend(make_config(**kwargs))
    server = FakeServer()
    backend.server = server
    return backend, server


async def test_new_clients_are_auto_assigned_once():
    backend, server = _backend(
        default_group="wohnzimmer",
        static=[StaticClientConfig(host="10.0.0.5", port=8928, group="bad")],
    )
    server.add("esp-bad", url="ws://10.0.0.5:8928/sendspin")
    server.add("phone")
    await backend.refresh()

    assert backend.groups["bad"].members == ["esp-bad"]
    assert backend.groups["wohnzimmer"].members == ["phone"]
    assert backend.list_clients()["esp-bad"]["group_id"] == "bad"

    # A deliberately unassigned client must not bounce back on the next refresh.
    await backend.set_client_group("phone", None)
    await backend.refresh()
    assert backend.groups["wohnzimmer"].members == []
    assert backend.list_clients()["phone"]["group_id"] is None


async def test_yaml_members_are_grouped_natively():
    backend, server = _backend(
        groups=[GroupState(group_id="wohnzimmer", name="W", members=["a", "b"])]
    )
    a, b = server.add("a"), server.add("b")
    await backend.refresh()
    assert a.group is b.group


async def test_moving_a_client_updates_both_groups():
    backend, server = _backend()
    a, b, c = server.add("a"), server.add("b"), server.add("c")
    await backend.set_group_members("wohnzimmer", ["a", "b"])
    await backend.set_group_members("bad", ["c"])

    await backend.set_client_group("b", "bad")

    assert backend.groups["wohnzimmer"].members == ["a"]
    assert backend.groups["bad"].members == ["c", "b"]
    assert b.group is c.group
    assert a.group is not b.group
    assert a.group.clients == [a]


async def test_set_members_steals_from_other_groups_and_ungroups_leavers():
    backend, server = _backend()
    a, b = server.add("a"), server.add("b")
    await backend.set_group_members("wohnzimmer", ["a", "b"])
    await backend.set_group_members("bad", ["b"])
    assert backend.groups["wohnzimmer"].members == ["a"]
    assert b not in a.group.clients


async def test_stream_restarts_when_last_client_leaves_and_rejoins():
    backend, server = _backend()
    a = server.add("a")
    await backend.set_group_members("wohnzimmer", ["a"])
    await backend.set_group_stream("wohnzimmer", "spotify")
    first = a.group.stream
    assert first is not None and first.live is True
    assert backend.list_groups()["wohnzimmer"]["playback_state"] == "playing"

    await backend.set_client_group("a", "bad")
    assert first.is_stopped
    assert backend.list_groups()["wohnzimmer"]["playback_state"] == "stopped"
    assert a.group.stream is None  # "bad" has no stream

    await backend.set_client_group("a", "wohnzimmer")
    assert a.group.stream is not None and not a.group.stream.is_stopped
    assert a.group.stream is not first


async def test_stream_recovers_when_client_disappears_and_returns():
    backend, server = _backend(
        groups=[GroupState(group_id="wohnzimmer", name="W", members=["a"], stream="spotify")]
    )
    server.add("a")
    await backend.set_group_stream("wohnzimmer", "spotify")
    await backend.refresh()
    old_stream = backend._streams["wohnzimmer"].stream

    server.remove("a")  # aiosendspin cleaned the client up
    await backend.refresh()
    assert "wohnzimmer" not in backend._streams
    assert old_stream.is_stopped

    a = server.add("a")  # reconnects as a fresh object in a solo group
    await backend.refresh()
    assert backend._streams["wohnzimmer"].native_group is a.group
    assert a.group.stream is not None


async def test_switching_and_detaching_stream():
    backend, server = _backend()
    a = server.add("a")
    await backend.set_group_members("wohnzimmer", ["a"])
    await backend.set_group_stream("wohnzimmer", "spotify")
    spotify_stream = a.group.stream

    await backend.set_group_stream("wohnzimmer", "mopidy")
    assert spotify_stream.is_stopped
    assert backend._streams["wohnzimmer"].source.source_id == "mopidy"

    await backend.set_group_stream("wohnzimmer", None)
    assert a.group.stream is None
    assert backend.list_groups()["wohnzimmer"]["stream"] is None

    with pytest.raises(ValueError):
        await backend.set_group_stream("wohnzimmer", "nope")


async def test_volume_command_is_not_reverted_by_stale_live_value():
    backend, server = _backend()
    a = server.add("a")
    a.player.volume = 70
    await backend.set_group_members("wohnzimmer", ["a"])
    await backend.refresh()
    assert backend.list_groups()["wohnzimmer"]["volume"] == 70

    await backend.set_group_volume("wohnzimmer", 40)
    assert a.group.role.sent == [("volume", 40)]
    await backend.refresh()  # device has not echoed yet
    assert backend.list_groups()["wohnzimmer"]["volume"] == 40

    a.player.volume = 55  # changed on the device itself
    await backend.refresh()
    assert backend.list_groups()["wohnzimmer"]["volume"] == 55


async def test_client_volume_and_mute_commands():
    backend, server = _backend()
    a = server.add("a")
    await backend.refresh()
    await backend.set_client_volume("a", 30)
    await backend.set_client_mute("a", True)
    assert a.player.sent == [("volume", 30), ("mute", True)]
    assert backend.list_clients()["a"]["volume"] == 30
    assert backend.list_clients()["a"]["mute"] is True
    with pytest.raises(ValueError):
        await backend.set_client_volume("ghost", 10)


async def test_feed_group_commits_audio_and_logs_failures_once(caplog):
    backend, server = _backend()
    a = server.add("a")
    await backend.set_group_members("wohnzimmer", ["a"])
    await backend.set_group_stream("wohnzimmer", "spotify")
    await backend.feed_group("wohnzimmer", b"\x00" * 8)
    assert a.group.stream.chunks == [b"\x00" * 8]

    a.group.stream.fail = True
    for _ in range(5):
        await backend.feed_group("wohnzimmer", b"\x00" * 8)
    assert caplog.text.count("PushStream audio commit failed") == 1


async def test_unknown_group_raises():
    backend, _ = _backend()
    with pytest.raises(ValueError):
        await backend.set_group_volume("nope", 10)
    with pytest.raises(ValueError):
        await backend.set_client_group("a", "nope")


async def test_moving_a_whole_group_keeps_the_destination_stream_running():
    backend, server = _backend()
    a, b = server.add("a"), server.add("b")
    await backend.set_group_members("bad", ["a", "b"])
    await backend.set_group_stream("wohnzimmer", "spotify")

    await backend.set_group_members("wohnzimmer", ["a", "b"])

    stream = backend._streams["wohnzimmer"].stream
    assert a.group is b.group
    assert not stream.is_stopped
    assert a.group.stream is stream
    assert "bad" not in backend._native_groups
    assert backend.list_groups()["wohnzimmer"]["playback_state"] == "playing"


def test_group_playback_state_follows_source_activity():
    backend, _server = _backend(
        groups=[GroupState(group_id="wohnzimmer", name="W", members=["a"], stream="spotify")]
    )
    assert backend.list_groups({"spotify": False})["wohnzimmer"]["playback_state"] == "stopped"
    assert backend.list_groups({"spotify": True})["wohnzimmer"]["playback_state"] == "playing"
