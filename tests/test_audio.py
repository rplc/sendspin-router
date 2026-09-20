import asyncio
import os

import pytest

from sendspin_router.audio import AudioRouter
from sendspin_router.models import SourceConfig


@pytest.mark.asyncio
async def test_subscribe_reads_pcm_from_fifo(tmp_path):
    fifo_path = tmp_path / "test.pcm"
    os.mkfifo(fifo_path)

    source = SourceConfig(
        source_id="mopidy",
        name="Mopidy",
        uri=f"pipe://{fifo_path}",
        sample_rate=48000,
        channels=2,
        bit_depth=16,
    )
    router = AudioRouter([source])
    await router.start()

    received: list[bytes] = []

    async def sink(chunk: bytes) -> None:
        received.append(chunk)

    await router.subscribe("wohnzimmer", "mopidy", sink)

    payload = b"\x01\x02" * 1000

    def _write() -> None:
        fd = os.open(fifo_path, os.O_WRONLY)
        os.write(fd, payload)
        # Keep the writer open for a moment so we can observe "available"
        # before the reader sees EOF and flips it back.
        import time

        time.sleep(0.2)
        os.close(fd)

    loop = asyncio.get_running_loop()
    write_task = loop.run_in_executor(None, _write)

    for _ in range(50):
        if received:
            break
        await asyncio.sleep(0.02)

    assert b"".join(received) == payload
    assert router.state()["mopidy"]["available"] is True

    await write_task
    await router.stop()


@pytest.mark.asyncio
async def test_unknown_source_raises():
    router = AudioRouter([])
    with pytest.raises(ValueError):
        await router.subscribe("wohnzimmer", "does-not-exist", lambda chunk: None)
