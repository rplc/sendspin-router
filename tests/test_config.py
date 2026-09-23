import pytest

from sendspin_router.config import load_config


def test_example_config_loads(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
server:
  name: Test
  id: test
  sendspin_port: 8927
mqtt:
  host: 127.0.0.1
  port: 1883
  username: null
  password: null
  base_topic: sendspin/router
  client_id: test
groups:
  - id: living
    name: Living
    members: []
    stream: mopidy
sources:
  - id: mopidy
    name: Mopidy
    uri: pipe:///tmp/mopidy.pcm?name=Mopidy&sampleformat=48000:16:2&mode=create
router:
  active_source: null
clients:
  default_group: living
  static:
    - host: "192.168.5.194"
      port: 8927
      group: living
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.groups[0].group_id == "living"
    assert config.groups[0].stream == "mopidy"
    assert config.sources[0].source_id == "mopidy"
    assert config.clients.default_group == "living"
    assert config.clients.static[0].host == "192.168.5.194"
    assert config.clients.static[0].group == "living"


def test_clients_section_optional(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
server:
  name: Test
  id: test
mqtt:
  host: 127.0.0.1
  port: 1883
  username: null
  password: null
  base_topic: sendspin/router
  client_id: test
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.clients.default_group is None
    assert config.clients.static == []


_BASE = """
server:
  name: Test
mqtt:
  host: 127.0.0.1
sources:
  - id: mopidy
    uri: pipe:///tmp/mopidy.pcm
"""


def _load(tmp_path, extra: str):
    path = tmp_path / "config.yaml"
    path.write_text(_BASE + extra, encoding="utf-8")
    return load_config(path)


def test_legacy_active_source_is_ignored(tmp_path):
    config = _load(tmp_path, "router:\n  active_source: mopidy\n")
    assert not hasattr(config, "router")


@pytest.mark.parametrize(
    "extra, message",
    [
        ("groups:\n  - id: a\n    stream: nope\n", "unknown stream"),
        ("groups:\n  - id: a\n  - id: a\n", "Duplicate group"),
        ("groups:\n  - id: a\n    members: [x]\n  - id: b\n    members: [x]\n", "only belong"),
        ("clients:\n  default_group: nope\n", "default_group"),
        ("clients:\n  static:\n    - host: 1.2.3.4\n      group: nope\n", "unknown group"),
    ],
)
def test_invalid_references_fail_fast(tmp_path, extra, message):
    with pytest.raises(ValueError, match=message):
        _load(tmp_path, extra)
