from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .models import SourceConfig

_LOG = logging.getLogger(__name__)

# A sink receives raw, interleaved PCM bytes matching the source's configured
# sample_rate/channels/bit_depth. What happens with those bytes (e.g. handing
# them to an aiosendspin PushStream) is entirely up to whoever subscribes.
FrameSink = Callable[[bytes], Awaitable[None]]

_CHUNK_MS = 20  # 20 ms chunks: small enough for low latency, large enough
# to not busy-loop on every single sample.


def _bytes_per_ms(source: SourceConfig) -> int:
    bytes_per_sample = max(1, source.bit_depth // 8)
    return (source.sample_rate * source.channels * bytes_per_sample) // 1000


def _fifo_path(uri: str) -> str:
    """Extract the filesystem path from a `pipe://` URI.

    Snapserver-style query parameters (`?name=...&sampleformat=...&mode=create`)
    are intentionally ignored: sample format is already represented
    explicitly in our own YAML config, so the router doesn't need them.
    """
    if uri.startswith("pipe://"):
        uri = uri.removeprefix("pipe://")
    return uri.split("?", 1)[0]


@dataclass
class _SourceState:
    config: SourceConfig
    task: asyncio.Task[None] | None = None
    sinks: dict[str, FrameSink] = field(default_factory=dict)  # group_id -> sink
    available: bool = False


class AudioRouter:
    """Owns the configured PCM sources and fans raw audio out to whichever
    groups are currently subscribed to each one.

    This class deliberately knows nothing about Sendspin/aiosendspin. It only
    reads the Snapserver-style FIFOs and calls the sink callback registered
    for a group. That keeps the (fiddly, blocking) FIFO handling isolated
    from the (fast-moving, still-being-verified) aiosendspin integration,
    which lives in SendspinBackend.

    Multiple groups can subscribe to the *same* source at once, and a group
    can only ever be subscribed to one source at a time (subscribing again
    replaces the previous subscription) -- exactly the "Wohnzimmer & Bad play
    Mopidy while Schlafzimmer plays Spotify, all at once" case.
    """

    def __init__(self, sources: list[SourceConfig]) -> None:
        self._sources: dict[str, _SourceState] = {
            source.source_id: _SourceState(source) for source in sources
        }
        # Reverse index so `subscribe()` can cleanly move a group from one
        # source to another.
        self._group_source: dict[str, str] = {}

    def state(self) -> dict[str, dict]:
        return {
            source_id: {
                "id": s.config.source_id,
                "name": s.config.name,
                "uri": s.config.uri,
                "sample_rate": s.config.sample_rate,
                "channels": s.config.channels,
                "bit_depth": s.config.bit_depth,
                "available": s.available,
            }
            for source_id, s in self._sources.items()
        }

    async def start(self) -> None:
        _LOG.info("Configured %d audio source(s)", len(self._sources))

    async def stop(self) -> None:
        for state in self._sources.values():
            await self._stop_reader(state)
        self._group_source.clear()

    async def subscribe(self, group_id: str, source_id: str, sink: FrameSink) -> None:
        """Attach `group_id` to `source_id`, (re)starting the FIFO reader if
        needed and detaching the group from any source it was on before."""
        target = self._require(source_id)
        previous_source_id = self._group_source.get(group_id)
        if previous_source_id and previous_source_id != source_id:
            await self.unsubscribe(group_id, previous_source_id)

        target.sinks[group_id] = sink
        self._group_source[group_id] = source_id
        if target.task is None or target.task.done():
            target.task = asyncio.create_task(
                self._read_loop(target), name=f"pcm-reader-{source_id}"
            )

    async def unsubscribe(self, group_id: str, source_id: str | None = None) -> None:
        """Detach `group_id` from `source_id` (or from whatever it is
        currently on, if omitted)."""
        source_id = source_id or self._group_source.pop(group_id, None)
        if source_id is None:
            return
        self._group_source.pop(group_id, None)
        state = self._sources.get(source_id)
        if state is None:
            return
        if state.sinks.pop(group_id, None) is not None and not state.sinks:
            await self._stop_reader(state)

    async def _stop_reader(self, state: _SourceState) -> None:
        if state.task is not None:
            state.task.cancel()
            await asyncio.gather(state.task, return_exceptions=True)
            state.task = None
        state.available = False

    def _require(self, source_id: str) -> _SourceState:
        try:
            return self._sources[source_id]
        except KeyError as exc:
            raise ValueError(f"Unknown source: {source_id}") from exc

    async def _read_loop(self, state: _SourceState) -> None:
        """Continuously read raw PCM from the source's FIFO and fan it out.

        Opening a FIFO for reading blocks until a writer (Mopidy/Spotify's
        Snapserver-compatible output) opens it, and a read of length 0 means
        the writer closed -- at which point we just reopen and keep waiting,
        so restarting the upstream player doesn't require restarting us.
        """
        path = _fifo_path(state.config.uri)
        chunk_size = max(_bytes_per_ms(state.config) * _CHUNK_MS, 1)
        loop = asyncio.get_running_loop()
        try:
            while True:
                try:
                    fd = await loop.run_in_executor(
                        None, os.open, path, os.O_RDONLY | os.O_NONBLOCK
                    )
                    # Drop O_NONBLOCK again once opened: we *want* blocking
                    # reads inside the executor thread, we just don't want
                    # the open() call itself to block if there's currently no
                    # writer and the FIFO would otherwise hang forever.
                    os.set_blocking(fd, True)
                except FileNotFoundError:
                    _LOG.warning("PCM pipe not found, retrying in 2s: %s", path)
                    state.available = False
                    await asyncio.sleep(2)
                    continue
                except OSError:
                    _LOG.exception("Failed to open PCM pipe: %s", path)
                    state.available = False
                    await asyncio.sleep(2)
                    continue

                state.available = True
                _LOG.info("Opened PCM source '%s' (%s)", state.config.source_id, path)
                try:
                    while True:
                        chunk = await loop.run_in_executor(None, os.read, fd, chunk_size)
                        if not chunk:
                            _LOG.info("PCM source '%s' writer closed, reopening", state.config.source_id)
                            break
                        await self._deliver(state, chunk)
                finally:
                    os.close(fd)
                    state.available = False
        finally:
            state.available = False

    async def _deliver(self, state: _SourceState, chunk: bytes) -> None:
        for group_id, sink in list(state.sinks.items()):
            try:
                await sink(chunk)
            except Exception:
                _LOG.exception(
                    "Audio sink for group '%s' (source '%s') failed",
                    group_id,
                    state.config.source_id,
                )
