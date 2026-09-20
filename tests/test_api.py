from fastapi.testclient import TestClient

from sendspin_router.api import create_app
from sendspin_router.audio import AudioRouter
from sendspin_router.config import (
    AppConfig,
    ClientsConfig,
    MqttConfig,
    RouterConfig,
    SendspinConfig,
    ServerConfig,
)
from sendspin_router.models import GroupState, SourceConfig
from sendspin_router.sendspin_backend import SendspinBackend


def _test_config() -> AppConfig:
    return AppConfig(
        server=ServerConfig(name="Test", server_id="test", sendspin_port=8927),
        mqtt=MqttConfig(
            host="127.0.0.1",
            port=1883,
            username=None,
            password=None,
            base_topic="sendspin/router",
            client_id="test",
        ),
        groups=[GroupState(group_id="wohnzimmer", name="Wohnzimmer")],
        sources=[
            SourceConfig(source_id="mopidy", name="Mopidy", uri="pipe:///tmp/mopidy.pcm"),
            SourceConfig(source_id="spotify", name="Spotify", uri="pipe:///tmp/spotify.pcm"),
            SourceConfig(source_id="chromecast", name="Chromecast", uri="pipe:///tmp/chromecast.pcm"),
        ],
        router=RouterConfig(active_source=None),
        sendspin=SendspinConfig(identity_file="data/identity.key", pairing_store="data/pairings.json"),
        clients=ClientsConfig(default_group=None, static=[]),
    )


def test_status_and_sources():
    config = _test_config()
    backend = SendspinBackend(config)
    app = create_app(backend, AudioRouter(config.sources))
    client = TestClient(app)
    assert client.get("/api/v1/status").status_code == 200
    data = client.get("/api/v1/router/sources").json()
    assert {v["id"] for v in data.values()} == {"mopidy", "spotify", "chromecast"}
