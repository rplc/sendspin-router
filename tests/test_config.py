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
sources:
  - id: mopidy
    name: Mopidy
    uri: pipe:///tmp/mopidy.pcm?name=Mopidy&sampleformat=48000:16:2&mode=create
router:
  active_source: null
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.groups[0].group_id == "living"
    assert config.sources[0].source_id == "mopidy"
