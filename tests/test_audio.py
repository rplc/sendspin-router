import asyncio
import os
import time

import pytest

from sendspin_router.audio import _MAX_LAG_S, _MAX_LEAD_S, AudioRouter, _Pacer
from sendspin_router.models import SourceConfig


def _router(fifo_path) -> AudioRouter:
    source = SourceConfig(source_id="mopidy", name="Mopidy", uri=f"pipe://{fifo_path}?x=1")
    return AudioRouter([source])


async def _wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return False


async def test_subscribe_reads_whole_frames_from_fifo(tmp_path):
    fifo_path = tmp_path / "test.pcm"
    os.mkfifo(fifo_path)
    router = _router(fifo_path)
    received: list[bytes] = []

    async def sink(chunk: bytes) -> None:
        received.append(chunk)

    await router.subscribe("wohnzimmer", "mopidy", sink)
    await asyncio.sleep(0.05)  # reader waits for a writer without failing
    assert router.state()["mopidy"]["available"] is False

    payload = bytes(range(256)) * 16  # 4096 bytes = 1024 frames of 16-bit stereo

    def _write() -> None:
        fd = os.open(fifo_path, os.O_WRONLY)
        os.write(fd, payload[:3])  # partial frame first
        time.sleep(0.05)
        os.write(fd, payload[3:])
        time.sleep(0.2)
        os.close(fd)

    write_task = asyncio.get_running_loop().run_in_executor(None, _write)
    assert await _wait_for(lambda: len(b"".join(received)) == len(payload))
    assert router.state()["mopidy"]["available"] is True
    assert b"".join(received) == payload
    assert all(len(c) % 4 == 0 for c in received)

    await write_task
    assert await _wait_for(lambda: router.state()["mopidy"]["available"] is False)
    await router.stop()


async def test_stop_returns_quickly_while_writer_is_idle(tmp_path):
    fifo_path = tmp_path / "idle.pcm"
    os.mkfifo(fifo_path)
    router = _router(fifo_path)

    async def sink(chunk: bytes) -> None:
        pass

    await router.subscribe("wohnzimmer", "mopidy", sink)
    await asyncio.sleep(0.05)
    writer = os.open(fifo_path, os.O_WRONLY | os.O_NONBLOCK)  # attached but silent
    try:
        await asyncio.sleep(0.1)
        started = time.monotonic()
        await router.stop()
        assert time.monotonic() - started < 1.0
    finally:
        os.close(writer)


async def test_resubscribe_moves_group_but_keeps_draining(tmp_path):
    a, b = tmp_path / "a.pcm", tmp_path / "b.pcm"
    os.mkfifo(a)
    os.mkfifo(b)
    router = AudioRouter([
        SourceConfig(source_id="a", name="A", uri=f"pipe://{a}"),
        SourceConfig(source_id="b", name="B", uri=f"pipe://{b}"),
    ])
    await router.start()

    async def sink(chunk: bytes) -> None:
        pass

    await router.subscribe("g", "a", sink)
    await router.subscribe("g", "b", sink)
    assert router._sources["a"].sinks == {}
    assert router._sources["b"].sinks == {"g": sink}
    await router.unsubscribe("g")
    assert router._sources["b"].sinks == {}
    assert router._sources["a"].task is not None and not router._sources["a"].task.done()
    assert router._sources["b"].task is not None and not router._sources["b"].task.done()
    await router.stop()


async def test_unused_source_is_drained_so_the_writer_never_blocks(tmp_path):
    fifo_path = tmp_path / "mopidy.pcm"
    os.mkfifo(fifo_path)
    router = _router(fifo_path)
    await router.start()  # no group subscribed at all
    await asyncio.sleep(0.05)

    def _write() -> None:
        # Blocking open + 128 KB (~0.7 s of 48 kHz stereo): both block forever
        # without a reader, because the pipe buffer is only 64 KB.
        fd = os.open(fifo_path, os.O_WRONLY)
        try:
            for _ in range(2):
                os.write(fd, b"\x00" * 65536)
        finally:
            os.close(fd)

    writer = asyncio.get_running_loop().run_in_executor(None, _write)
    await asyncio.wait_for(writer, timeout=5)
    await router.stop()


async def test_missing_pipe_is_logged_once(tmp_path, caplog):
    router = _router(tmp_path / "missing.pcm")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("sendspin_router.audio.asyncio.sleep", _fast_sleep)
        await router.start()
        await asyncio.sleep(0.1)
        await router.stop()
    assert caplog.text.count("PCM pipe not found") == 1


_real_sleep = asyncio.sleep


async def _fast_sleep(delay, *args):
    await _real_sleep(min(delay, 0.001))


def test_pacer_limits_reads_to_realtime():
    pacer = _Pacer(bytes_per_second=1000)
    assert pacer.delay_after(100, now=0.0) == 0.0  # 0.1 s ahead: within lead
    assert pacer.delay_after(900, now=0.0) == pytest.approx(1.0 - _MAX_LEAD_S)
    assert pacer.delay_after(0, now=1.0) == 0.0  # caught up
    # Writer stalled for a long time: no burst credit afterwards.
    assert pacer.delay_after(1000, now=2.0 + _MAX_LAG_S + 5) == 0.0
    assert pacer.delay_after(1000, now=2.0 + _MAX_LAG_S + 5) == pytest.approx(1.0 - _MAX_LEAD_S)


async def test_fast_writer_is_throttled_to_realtime(tmp_path):
    fifo_path = tmp_path / "radio.pcm"
    os.mkfifo(fifo_path)
    # 8 kHz mono 16 bit = 16000 bytes per second
    router = AudioRouter([SourceConfig(source_id="r", name="R", uri=f"pipe://{fifo_path}",
                                       sample_rate=8000, channels=1)])
    await router.start()
    received: list[bytes] = []
    first: list[float] = []

    async def sink(chunk: bytes) -> None:
        if not first:
            first.append(time.monotonic())
        received.append(chunk)

    await router.subscribe("g", "r", sink)
    await asyncio.sleep(0.05)

    def _write() -> None:
        fd = os.open(fifo_path, os.O_WRONLY)
        os.write(fd, b"\x00" * 16000)  # one second of audio in one go
        time.sleep(1.5)
        os.close(fd)

    writer = asyncio.get_running_loop().run_in_executor(None, _write)
    assert await _wait_for(lambda: len(b"".join(received)) == 16000, timeout=3)
    elapsed = time.monotonic() - first[0]
    assert 1.0 - _MAX_LEAD_S - 0.1 < elapsed < 1.3
    await writer
    await router.stop()


async def test_unknown_source_raises():
    router = AudioRouter([])
    with pytest.raises(ValueError):
        await router.subscribe("wohnzimmer", "does-not-exist", lambda chunk: None)
