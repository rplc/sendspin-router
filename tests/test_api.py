from fastapi.testclient import TestClient

from sendspin_router.api import create_app
from sendspin_router.audio import AudioRouter
from sendspin_router.config import AppConfig
from sendspin_router.sendspin_backend import SendspinBackend


def test_status_and_sources():
    backend = SendspinBackend(AppConfig())
    app = create_app(backend, AudioRouter())
    client = TestClient(app)
    assert client.get("/api/v1/status").status_code == 200
    data = client.get("/api/v1/router/sources").json()
    assert {x["source_id"] for x in data["sources"]} == {"mopidy", "spotify", "chromecast"}
