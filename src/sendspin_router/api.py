from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .audio import AudioRouter
from .sendspin_backend import SendspinBackend


class GroupUpdate(BaseModel):
    members: list[str] | None = None
    volume: int | None = Field(default=None, ge=0, le=100)
    mute: bool | None = None


class SourceSelection(BaseModel):
    source: str | None
    groups: list[str] = Field(default_factory=list)


def create_app(backend: SendspinBackend, audio: AudioRouter) -> FastAPI:
    app = FastAPI(title="Sendspin Router", version="0.3.0")

    @app.get("/api/v1/status")
    async def status():
        return {**backend.status(), "router": audio.state()}

    @app.get("/api/v1/clients")
    async def clients():
        await backend.refresh_clients()
        return backend.list_clients()

    @app.get("/api/v1/groups")
    async def groups():
        return backend.list_groups()

    @app.put("/api/v1/groups/{group_id}")
    async def update_group(group_id: str, update: GroupUpdate):
        try:
            if update.members is not None:
                await backend.set_group_members(group_id, update.members)
            if update.volume is not None:
                await backend.set_group_volume(group_id, update.volume)
            if update.mute is not None:
                await backend.set_group_mute(group_id, update.mute)
            return next(g for g in backend.list_groups() if g["group_id"] == group_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.post("/api/v1/groups/{group_id}/volume")
    async def volume(group_id: str, update: GroupUpdate):
        if update.volume is None:
            raise HTTPException(400, "volume is required")
        try:
            await backend.set_group_volume(group_id, update.volume)
            return next(g for g in backend.list_groups() if g["group_id"] == group_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.post("/api/v1/groups/{group_id}/mute")
    async def mute(group_id: str, update: GroupUpdate):
        if update.mute is None:
            raise HTTPException(400, "mute is required")
        try:
            await backend.set_group_mute(group_id, update.mute)
            return next(g for g in backend.list_groups() if g["group_id"] == group_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/api/v1/router/sources")
    async def sources():
        return audio.state()

    @app.post("/api/v1/router/source")
    async def source(selection: SourceSelection):
        try:
            return await audio.select(selection.source, selection.groups)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/api/v1/router/stop")
    async def stop():
        return await audio.stop()

    return app
