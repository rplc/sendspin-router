from __future__ import annotations

import asyncio
import logging
import os
import select
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .models import SourceConfig

_LOG = logging.getLogger(__name__)

# A sink receives raw, interleaved PCM bytes matching the source's configured
# sample_rate/channels/bit_depth. Chunks always contain whole frames.
FrameSink = Callable[[bytes], Awaitable[None]]

_CHUNK_MS = 20  # small enough for low latency, large enough to not busy-loop
_IDLE_TIMEOUT_S = 0.5  # no data for this long -> source is reported unavailable
_REOPEN_DELAY_S = 0.25  # back-off while no writer has the FIFO open
# Realtime pacing: never read more than this far ahead of the audio clock.
# Players that write faster than realtime (GStreamer filesink, decoders
# catching up after a network stall) are throttled by the full pipe, exactly
# as Snapserver did it.
_MAX_LEAD_S = 0.2
# If the writer falls further behind than this (paused, stalled stream), the
# pacing clock restarts instead of letting the backlog burst through.
_MAX_LAG_S = 0.5

# Audio activity detection. This intentionally uses a very cheap sampled peak
# detector rather than FFT/RMS over every sample. At 48 kHz stereo we inspect
# one frame out of every 20, so even three sources require only ~29k sample
# conversions per second.
_ACTIVITY_SAMPLE_EVERY_FRAMES = 20
_ACTIVITY_START_PEAK = 180   # ~-45 dBFS for signed 16-bit PCM
_ACTIVITY_STOP_PEAK = 58     # ~-55 dBFS
_ACTIVITY_START_S = 0.10
_ACTIVITY_STOP_S = 1.00


def _fifo_path(uri: str) -> str:
    """Extract the filesystem path from a ``pipe://`` URI.

    Snapserver-style query parameters (``?name=...&sampleformat=...``) are
    ignored: the sample format is configured explicitly in YAML.
    """
    return uri.removeprefix("pipe://").split("?", 1)[0]


def _read_with_timeout(fd: int, size: int, timeout: float) -> bytes | None:
    """Blocking read with an upper bound, run inside an executor thread.

    Returns ``None`` on timeout and ``b""`` when no writer has the FIFO open.
    The timeout guarantees the executor thread comes back regularly, so
    cancelling the reader (group unsubscribed, shutdown) never leaves a thread
    stuck in ``read()`` and never blocks interpreter shutdown.
    """
    readable, _, _ = select.select([fd], [], [], timeout)
    if not readable:
        return None
    try:
        return os.read(fd, size)
    except BlockingIOError:
        return None


class _Pacer:
    """Keeps reads of one source at (at most) realtime speed."""

    def __init__(self, bytes_per_second: int) -> None:
        self._rate = max(1, bytes_per_second)
        self._start: float | None = None
        self._bytes = 0

    def reset(self) -> None:
        self._start = None

    def delay_after(self, size: int, now: float) -> float:
        """Account for ``size`` bytes just read; return seconds to wait before
        the next read."""
        if self._start is None:
            self._start, self._bytes = now, 0
        self._bytes += size
        ahead = self._start + self._bytes / self._rate - now
        if ahead < -_MAX_LAG_S:
            self._start, self._bytes = now, 0  # writer was slow: drop the credit
            return 0.0
        return max(0.0, ahead - _MAX_LEAD_S)


@dataclass
class _SourceState:
    config: SourceConfig
    task: asyncio.Task[None] | None = None
    sinks: dict[str, FrameSink] = field(default_factory=dict)  # group_id -> sink
    available: bool = False
    playing: bool = False
    active_since: float | None = None
    silent_since: float | None = None


class AudioRouter:
    """Reads the configured PCM FIFOs and fans raw audio out to subscribed groups.

    Knows nothing about Sendspin. Several groups can subscribe to the same
    source; a group is subscribed to at most one source at a time.

    Every configured FIFO is read continuously from ``start()`` on, whether a
    group listens or not; audio without subscribers is discarded. Reads are
    paced to realtime, so a writer can never run ahead of playback. Upstream
    players block as soon as nobody drains their FIFO (opening it for writing
    waits for a reader, writing waits once the 64 KB pipe buffer is full), so
    e.g. Mopidy would stop playing while no group uses it. Snapserver used to
    drain all FIFOs the same way.

    ``available`` means "PCM data is currently flowing", i.e. the upstream
    player is actually playing.
    """

    def __init__(self, sources: list[SourceConfig]) -> None:
        self._sources: dict[str, _SourceState] = {
            source.source_id: _SourceState(source) for source in sources
        }
        self._group_source: dict[str, str] = {}
        self._executor: ThreadPoolExecutor | None = None

    def playing_state(self) -> dict[str, bool]:
        """Return current signal activity for each configured PCM source."""
        return {source_id: state.playing for source_id, state in self._sources.items()}

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
                "playing": s.playing,
            }
            for source_id, s in self._sources.items()
        }

    async def start(self) -> None:
        """Start draining every configured FIFO."""
        for state in self._sources.values():
            self._ensure_reader(state)
        _LOG.info("Reading %d PCM source(s)", len(self._sources))

    async def stop(self) -> None:
        for state in self._sources.values():
            state.sinks.clear()
            if state.task is not None:
                state.task.cancel()
        await asyncio.gather(
            *(s.task for s in self._sources.values() if s.task is not None),
            return_exceptions=True,
        )
        for state in self._sources.values():
            state.task = None
            state.available = False
            state.playing = False
            state.active_since = None
            state.silent_since = None
        self._group_source.clear()
        if self._executor is not None:
            # Reader threads return within _IDLE_TIMEOUT_S; don't wait for them.
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

    async def subscribe(self, group_id: str, source_id: str, sink: FrameSink) -> None:
        """Attach ``group_id`` to ``source_id``, moving it off any previous source."""
        target = self._require(source_id)
        previous = self._group_source.get(group_id)
        if previous is not None and previous != source_id:
            await self.unsubscribe(group_id)

        target.sinks[group_id] = sink
        self._group_source[group_id] = source_id
        self._ensure_reader(target)

    async def unsubscribe(self, group_id: str) -> None:
        """Detach ``group_id`` from its source. The FIFO keeps being drained."""
        source_id = self._group_source.pop(group_id, None)
        state = self._sources.get(source_id) if source_id else None
        if state is not None:
            state.sinks.pop(group_id, None)

    def _ensure_reader(self, state: _SourceState) -> None:
        if state.task is None or state.task.done():
            state.task = asyncio.create_task(
                self._read_loop(state), name=f"pcm-reader-{state.config.source_id}"
            )

    def _thread_pool(self) -> ThreadPoolExecutor:
        # One thread per source, separate from the loop's default executor,
        # so permanently running readers never starve other blocking work.
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=max(1, len(self._sources)), thread_name_prefix="pcm-reader"
            )
        return self._executor

    def _require(self, source_id: str) -> _SourceState:
        try:
            return self._sources[source_id]
        except KeyError as exc:
            raise ValueError(f"Unknown source: {source_id}") from exc

    async def _read_loop(self, state: _SourceState) -> None:
        """Continuously read PCM from the source's FIFO and fan it out.

        The FIFO is opened non-blocking, so opening never waits for a writer.
        While no writer is attached, reads report EOF; we then back off briefly
        and reopen, so restarting the upstream player needs no router restart.
        """
        cfg = state.config
        path = _fifo_path(cfg.uri)
        frame = cfg.frame_size
        bytes_per_second = cfg.sample_rate * frame
        chunk_size = max(frame, bytes_per_second * _CHUNK_MS // 1000 // frame * frame)
        loop = asyncio.get_running_loop()
        missing_logged = False
        try:
            while True:
                try:
                    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
                except FileNotFoundError:
                    if not missing_logged:  # this loop runs forever; warn once
                        _LOG.warning("PCM pipe not found, waiting for it: %s", path)
                        missing_logged = True
                    await asyncio.sleep(2)
                    continue
                except OSError:
                    _LOG.exception("Failed to open PCM pipe: %s", path)
                    await asyncio.sleep(2)
                    continue
                if missing_logged:
                    _LOG.info("PCM pipe appeared: %s", path)
                    missing_logged = False

                remainder = b""
                received_any = False
                pacer = _Pacer(bytes_per_second)
                try:
                    while True:
                        chunk = await loop.run_in_executor(
                            self._thread_pool(), _read_with_timeout, fd, chunk_size,
                            _IDLE_TIMEOUT_S,
                        )
                        if chunk is None:  # no PCM arrived during the timeout
                            state.available = False
                            self._update_activity(state, 0, loop.time())
                            pacer.reset()
                            continue
                        if not chunk:  # no writer (anymore)
                            if received_any:
                                _LOG.info("PCM source '%s' writer closed", cfg.source_id)
                            break
                        if not received_any:
                            _LOG.info("PCM source '%s' receiving audio", cfg.source_id)
                            received_any = True
                        state.available = True
                        self._update_activity(state, self._peak_16bit(cfg, chunk), loop.time())

                        # Only hand whole frames downstream; a partial frame
                        # would shift every following sample.
                        data = remainder + chunk if remainder else chunk
                        usable = len(data) - len(data) % frame
                        remainder = data[usable:]
                        if usable:
                            await self._deliver(state, data[:usable])

                        delay = pacer.delay_after(len(chunk), loop.time())
                        if delay > 0:
                            await asyncio.sleep(delay)
                except OSError:
                    # The reader must survive anything: if it died, nobody
                    # would drain the FIFO and the upstream player would hang.
                    _LOG.exception("Reading PCM pipe failed, reopening: %s", path)
                    await asyncio.sleep(2)
                finally:
                    os.close(fd)
                    state.available = False
                    state.playing = False
                    state.active_since = None
                    state.silent_since = None
                await asyncio.sleep(_REOPEN_DELAY_S)
        finally:
            state.available = False
            state.playing = False
            state.active_since = None
            state.silent_since = None

    @staticmethod
    def _peak_16bit(cfg: SourceConfig, chunk: bytes) -> int:
        """Return a cheap sampled peak for signed little-endian 16-bit PCM.

        All current sources are 48 kHz / 16-bit / stereo. For other formats
        we conservatively report silence here; the FIFO still works normally.
        """
        if cfg.bit_depth != 16:
            return 0
        frame_size = cfg.frame_size
        if frame_size < 2:
            return 0
        step = frame_size * _ACTIVITY_SAMPLE_EVERY_FRAMES
        peak = 0
        for offset in range(0, len(chunk) - 1, step):
            for channel_offset in range(0, frame_size - 1, 2):
                value = int.from_bytes(chunk[offset + channel_offset:offset + channel_offset + 2],
                                       byteorder="little", signed=True)
                if abs(value) > peak:
                    peak = abs(value)
        return peak

    @staticmethod
    def _update_activity(state: _SourceState, peak: int, now: float) -> None:
        """Update playing state with hysteresis so silence/noise is stable."""
        if peak >= _ACTIVITY_START_PEAK:
            state.silent_since = None
            if not state.playing:
                if state.active_since is None:
                    state.active_since = now
                elif now - state.active_since >= _ACTIVITY_START_S:
                    state.playing = True
        elif peak <= _ACTIVITY_STOP_PEAK:
            state.active_since = None
            if state.playing:
                if state.silent_since is None:
                    state.silent_since = now
                elif now - state.silent_since >= _ACTIVITY_STOP_S:
                    state.playing = False
            else:
                state.silent_since = None
        else:
            # Between thresholds: retain the current state, but do not allow
            # a partial burst/noise sample to accumulate toward a transition.
            state.active_since = None
            state.silent_since = None

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
