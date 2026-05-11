"""
Cloud sync endpoints for SyncAura — Phase 2-4 (favorites, playlists,
playlist songs, listen events).

API shape (all routes are token-gated via Depends(get_current_user)):

    GET  /api/v1/sync/favorites?since=<unix_ms>
    POST /api/v1/sync/favorites              body: { items: [...] }
    GET  /api/v1/sync/playlists?since=<unix_ms>
    POST /api/v1/sync/playlists              body: { items: [...] }
    GET  /api/v1/sync/playlist_songs?since=<unix_ms>
    POST /api/v1/sync/playlist_songs         body: { items: [...] }
    GET  /api/v1/sync/listen_events?since=<unix_ms>&limit=500
    POST /api/v1/sync/listen_events          body: { items: [...] }

Conventions:
  * `since` is unix milliseconds. Server returns rows with
    `updated_at > since` for live-updateable tables, or
    `received_at > since` for the immutable listen_events log.
  * GETs include tombstones (rows with deleted_at != NULL) so clients
    can prune locally.
  * POST upserts stamp `updated_at = now()`. Tombstone is requested by
    setting `deleted: true` in the item payload.
  * Last-write-wins on `updated_at`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from auth import AuthedUser, get_current_user
from db import get_session
import models


router = APIRouter(prefix="/sync", tags=["sync"])


# ── helpers ──────────────────────────────────────────────────────────────


def _ms(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _from_ms(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _server_time_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ── shared schemas ───────────────────────────────────────────────────────


class FavoriteItem(BaseModel):
    videoId: str
    title: Optional[str] = None
    uploader: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    favoritedAt: int  # unix ms
    deleted: bool = False
    # Server-stamped on response; client ignores on push.
    updatedAt: Optional[int] = None
    deletedAt: Optional[int] = None


class FavoritesPushRequest(BaseModel):
    items: List[FavoriteItem]


class FavoritesResponse(BaseModel):
    items: List[FavoriteItem]
    serverTime: int


class PlaylistItem(BaseModel):
    syncId: str
    name: str
    createdAt: int
    autoBackupEnabled: bool = False
    deleted: bool = False
    updatedAt: Optional[int] = None
    deletedAt: Optional[int] = None


class PlaylistsPushRequest(BaseModel):
    items: List[PlaylistItem]


class PlaylistsResponse(BaseModel):
    items: List[PlaylistItem]
    serverTime: int


class PlaylistSongItem(BaseModel):
    playlistSyncId: str
    videoId: str
    title: Optional[str] = None
    uploader: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    position: int
    addedAt: int
    deleted: bool = False
    updatedAt: Optional[int] = None
    deletedAt: Optional[int] = None


class PlaylistSongsPushRequest(BaseModel):
    items: List[PlaylistSongItem]


class PlaylistSongsResponse(BaseModel):
    items: List[PlaylistSongItem]
    serverTime: int


class ListenEventItem(BaseModel):
    videoId: str
    title: Optional[str] = None
    uploader: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    playedAt: int
    durationListened: Optional[int] = None
    completionPct: Optional[int] = None
    source: Optional[str] = None
    # Server-only fields
    id: Optional[int] = None
    receivedAt: Optional[int] = None


class ListenEventsPushRequest(BaseModel):
    items: List[ListenEventItem]


class ListenEventsResponse(BaseModel):
    items: List[ListenEventItem]
    serverTime: int


# ── downloads (offline-download bookkeeping) ─────────────────────────────


class DownloadItem(BaseModel):
    videoId: str
    title: Optional[str] = None
    uploader: Optional[str] = None
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    fileSize: Optional[int] = None
    isAutoDownloaded: bool = False
    downloadedAt: int = 0
    deleted: bool = False
    # Server-stamped
    updatedAt: Optional[int] = None
    deletedAt: Optional[int] = None


class DownloadsPushRequest(BaseModel):
    items: List[DownloadItem]


class DownloadsResponse(BaseModel):
    items: List[DownloadItem]
    serverTime: int


# ── settings (flat key→value app-settings blob) ──────────────────────────


class SettingsResponse(BaseModel):
    # The whole blob as the client stored it; always carries an embedded
    # "_updated_at" (unix ms) used for last-write-wins. `null` when the
    # user has never pushed settings.
    settings: Optional[dict] = None
    serverTime: int


class SettingsPushRequest(BaseModel):
    settings: dict


# Cap on how many rows a single push can contain — defensive against a
# misbehaving client trying to ship its entire local DB at once.
MAX_BATCH = 500


def _check_batch_size(n: int) -> None:
    if n > MAX_BATCH:
        raise HTTPException(
            status_code=413,
            detail=f"Batch too large ({n} > {MAX_BATCH}); split into smaller chunks.",
        )


# ── /sync/favorites ──────────────────────────────────────────────────────


@router.get("/favorites", response_model=FavoritesResponse)
async def get_favorites(
    since: int = Query(0, ge=0, description="Unix ms; rows with updated_at > since"),
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    since_dt = _from_ms(since) or datetime.fromtimestamp(0, tz=timezone.utc)
    rows = (
        await session.execute(
            select(models.UserFavorite)
            .where(models.UserFavorite.user_id == user["uid"])
            .where(models.UserFavorite.updated_at > since_dt)
            .order_by(models.UserFavorite.updated_at.asc())
        )
    ).scalars().all()

    return FavoritesResponse(
        items=[
            FavoriteItem(
                videoId=r.video_id,
                title=r.title,
                uploader=r.uploader,
                duration=r.duration,
                thumbnail=r.thumbnail,
                favoritedAt=_ms(r.favorited_at) or 0,
                deleted=r.deleted_at is not None,
                updatedAt=_ms(r.updated_at),
                deletedAt=_ms(r.deleted_at),
            )
            for r in rows
        ],
        serverTime=_server_time_ms(),
    )


@router.post("/favorites", response_model=FavoritesResponse)
async def push_favorites(
    body: FavoritesPushRequest,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    _check_batch_size(len(body.items))
    now = datetime.now(timezone.utc)
    out: list[FavoriteItem] = []

    for item in body.items:
        values = {
            "user_id": user["uid"],
            "video_id": item.videoId,
            "title": item.title,
            "uploader": item.uploader,
            "duration": item.duration,
            "thumbnail": item.thumbnail,
            "favorited_at": _from_ms(item.favoritedAt) or now,
            "updated_at": now,
            "deleted_at": now if item.deleted else None,
        }
        stmt = pg_insert(models.UserFavorite).values(**values).on_conflict_do_update(
            index_elements=["user_id", "video_id"],
            set_={
                "title": values["title"],
                "uploader": values["uploader"],
                "duration": values["duration"],
                "thumbnail": values["thumbnail"],
                "favorited_at": values["favorited_at"],
                "updated_at": now,
                "deleted_at": values["deleted_at"],
            },
        )
        await session.execute(stmt)
        out.append(
            FavoriteItem(
                videoId=item.videoId,
                title=item.title,
                uploader=item.uploader,
                duration=item.duration,
                thumbnail=item.thumbnail,
                favoritedAt=item.favoritedAt,
                deleted=item.deleted,
                updatedAt=_ms(now),
                deletedAt=_ms(now) if item.deleted else None,
            )
        )

    await session.commit()
    return FavoritesResponse(items=out, serverTime=_server_time_ms())


# ── /sync/playlists ──────────────────────────────────────────────────────


@router.get("/playlists", response_model=PlaylistsResponse)
async def get_playlists(
    since: int = Query(0, ge=0),
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    since_dt = _from_ms(since) or datetime.fromtimestamp(0, tz=timezone.utc)
    rows = (
        await session.execute(
            select(models.UserPlaylist)
            .where(models.UserPlaylist.user_id == user["uid"])
            .where(models.UserPlaylist.updated_at > since_dt)
            .order_by(models.UserPlaylist.updated_at.asc())
        )
    ).scalars().all()

    return PlaylistsResponse(
        items=[
            PlaylistItem(
                syncId=r.sync_id,
                name=r.name,
                createdAt=_ms(r.created_at) or 0,
                autoBackupEnabled=r.auto_backup_enabled,
                deleted=r.deleted_at is not None,
                updatedAt=_ms(r.updated_at),
                deletedAt=_ms(r.deleted_at),
            )
            for r in rows
        ],
        serverTime=_server_time_ms(),
    )


@router.post("/playlists", response_model=PlaylistsResponse)
async def push_playlists(
    body: PlaylistsPushRequest,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    _check_batch_size(len(body.items))
    now = datetime.now(timezone.utc)
    out: list[PlaylistItem] = []

    for item in body.items:
        values = {
            "user_id": user["uid"],
            "sync_id": item.syncId,
            "name": item.name,
            "created_at": _from_ms(item.createdAt) or now,
            "auto_backup_enabled": item.autoBackupEnabled,
            "updated_at": now,
            "deleted_at": now if item.deleted else None,
        }
        stmt = pg_insert(models.UserPlaylist).values(**values).on_conflict_do_update(
            index_elements=["user_id", "sync_id"],
            set_={
                "name": values["name"],
                "created_at": values["created_at"],
                "auto_backup_enabled": values["auto_backup_enabled"],
                "updated_at": now,
                "deleted_at": values["deleted_at"],
            },
        )
        await session.execute(stmt)
        out.append(
            PlaylistItem(
                syncId=item.syncId,
                name=item.name,
                createdAt=item.createdAt,
                autoBackupEnabled=item.autoBackupEnabled,
                deleted=item.deleted,
                updatedAt=_ms(now),
                deletedAt=_ms(now) if item.deleted else None,
            )
        )

    await session.commit()
    return PlaylistsResponse(items=out, serverTime=_server_time_ms())


# ── /sync/playlist_songs ─────────────────────────────────────────────────


@router.get("/playlist_songs", response_model=PlaylistSongsResponse)
async def get_playlist_songs(
    since: int = Query(0, ge=0),
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    since_dt = _from_ms(since) or datetime.fromtimestamp(0, tz=timezone.utc)
    rows = (
        await session.execute(
            select(models.UserPlaylistSong)
            .where(models.UserPlaylistSong.user_id == user["uid"])
            .where(models.UserPlaylistSong.updated_at > since_dt)
            .order_by(models.UserPlaylistSong.updated_at.asc())
        )
    ).scalars().all()

    return PlaylistSongsResponse(
        items=[
            PlaylistSongItem(
                playlistSyncId=r.playlist_sync_id,
                videoId=r.video_id,
                title=r.title,
                uploader=r.uploader,
                duration=r.duration,
                thumbnail=r.thumbnail,
                position=r.position,
                addedAt=_ms(r.added_at) or 0,
                deleted=r.deleted_at is not None,
                updatedAt=_ms(r.updated_at),
                deletedAt=_ms(r.deleted_at),
            )
            for r in rows
        ],
        serverTime=_server_time_ms(),
    )


@router.post("/playlist_songs", response_model=PlaylistSongsResponse)
async def push_playlist_songs(
    body: PlaylistSongsPushRequest,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    _check_batch_size(len(body.items))
    now = datetime.now(timezone.utc)
    out: list[PlaylistSongItem] = []

    for item in body.items:
        values = {
            "user_id": user["uid"],
            "playlist_sync_id": item.playlistSyncId,
            "video_id": item.videoId,
            "title": item.title,
            "uploader": item.uploader,
            "duration": item.duration,
            "thumbnail": item.thumbnail,
            "position": item.position,
            "added_at": _from_ms(item.addedAt) or now,
            "updated_at": now,
            "deleted_at": now if item.deleted else None,
        }
        stmt = pg_insert(models.UserPlaylistSong).values(**values).on_conflict_do_update(
            index_elements=["user_id", "playlist_sync_id", "video_id"],
            set_={
                "title": values["title"],
                "uploader": values["uploader"],
                "duration": values["duration"],
                "thumbnail": values["thumbnail"],
                "position": values["position"],
                "added_at": values["added_at"],
                "updated_at": now,
                "deleted_at": values["deleted_at"],
            },
        )
        await session.execute(stmt)
        out.append(
            PlaylistSongItem(
                playlistSyncId=item.playlistSyncId,
                videoId=item.videoId,
                title=item.title,
                uploader=item.uploader,
                duration=item.duration,
                thumbnail=item.thumbnail,
                position=item.position,
                addedAt=item.addedAt,
                deleted=item.deleted,
                updatedAt=_ms(now),
                deletedAt=_ms(now) if item.deleted else None,
            )
        )

    await session.commit()
    return PlaylistSongsResponse(items=out, serverTime=_server_time_ms())


# ── /sync/listen_events ──────────────────────────────────────────────────


@router.get("/listen_events", response_model=ListenEventsResponse)
async def get_listen_events(
    since: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=2000),
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    since_dt = _from_ms(since) or datetime.fromtimestamp(0, tz=timezone.utc)
    rows = (
        await session.execute(
            select(models.UserListenEvent)
            .where(models.UserListenEvent.user_id == user["uid"])
            .where(models.UserListenEvent.received_at > since_dt)
            .order_by(models.UserListenEvent.received_at.asc())
            .limit(limit)
        )
    ).scalars().all()

    return ListenEventsResponse(
        items=[
            ListenEventItem(
                id=r.id,
                videoId=r.video_id,
                title=r.title,
                uploader=r.uploader,
                duration=r.duration,
                thumbnail=r.thumbnail,
                playedAt=_ms(r.played_at) or 0,
                durationListened=r.duration_listened,
                completionPct=r.completion_pct,
                source=r.source,
                receivedAt=_ms(r.received_at),
            )
            for r in rows
        ],
        serverTime=_server_time_ms(),
    )


@router.post("/listen_events", response_model=ListenEventsResponse)
async def push_listen_events(
    body: ListenEventsPushRequest,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    _check_batch_size(len(body.items))
    now = datetime.now(timezone.utc)
    out: list[ListenEventItem] = []

    for item in body.items:
        row = models.UserListenEvent(
            user_id=user["uid"],
            video_id=item.videoId,
            title=item.title,
            uploader=item.uploader,
            duration=item.duration,
            thumbnail=item.thumbnail,
            played_at=_from_ms(item.playedAt) or now,
            duration_listened=item.durationListened,
            completion_pct=item.completionPct,
            source=item.source,
            received_at=now,
        )
        session.add(row)
        # Defer ID assignment until commit; we'll backfill in `out`
        # below using flush() so the autoincrement value is realized.

    # One flush gives us autoincrement IDs without committing yet — lets
    # us return the server-assigned IDs in the same response.
    await session.flush()
    # Re-pull the just-inserted rows in arrival order.
    just_added = (
        await session.execute(
            select(models.UserListenEvent)
            .where(models.UserListenEvent.user_id == user["uid"])
            .where(models.UserListenEvent.received_at == now)
            .order_by(models.UserListenEvent.id.asc())
        )
    ).scalars().all()
    for r in just_added:
        out.append(
            ListenEventItem(
                id=r.id,
                videoId=r.video_id,
                title=r.title,
                uploader=r.uploader,
                duration=r.duration,
                thumbnail=r.thumbnail,
                playedAt=_ms(r.played_at) or 0,
                durationListened=r.duration_listened,
                completionPct=r.completion_pct,
                source=r.source,
                receivedAt=_ms(r.received_at),
            )
        )
    await session.commit()
    return ListenEventsResponse(items=out, serverTime=_server_time_ms())


# ── /sync/downloads ──────────────────────────────────────────────────────


@router.get("/downloads", response_model=DownloadsResponse)
async def get_downloads(
    since: int = Query(0, ge=0, description="Unix ms; rows with updated_at > since"),
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    since_dt = _from_ms(since) or datetime.fromtimestamp(0, tz=timezone.utc)
    rows = (
        await session.execute(
            select(models.UserDownload)
            .where(models.UserDownload.user_id == user["uid"])
            .where(models.UserDownload.updated_at > since_dt)
            .order_by(models.UserDownload.updated_at.asc())
        )
    ).scalars().all()
    return DownloadsResponse(
        items=[
            DownloadItem(
                videoId=r.video_id,
                title=r.title,
                uploader=r.uploader,
                duration=r.duration,
                thumbnail=r.thumbnail,
                fileSize=r.file_size,
                isAutoDownloaded=r.is_auto_downloaded,
                downloadedAt=_ms(r.downloaded_at) or 0,
                deleted=r.deleted_at is not None,
                updatedAt=_ms(r.updated_at),
                deletedAt=_ms(r.deleted_at),
            )
            for r in rows
        ],
        serverTime=_server_time_ms(),
    )


@router.post("/downloads", response_model=DownloadsResponse)
async def push_downloads(
    body: DownloadsPushRequest,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    _check_batch_size(len(body.items))
    now = datetime.now(timezone.utc)
    out: list[DownloadItem] = []
    for item in body.items:
        # _from_ms(0) is a *truthy* 1970 datetime, so a plain `or now`
        # wouldn't catch an omitted timestamp — guard on the raw int.
        downloaded_dt = _from_ms(item.downloadedAt) if item.downloadedAt and item.downloadedAt > 0 else now
        values = {
            "user_id": user["uid"],
            "video_id": item.videoId,
            "title": item.title,
            "uploader": item.uploader,
            "duration": item.duration,
            "thumbnail": item.thumbnail,
            "file_size": item.fileSize,
            "is_auto_downloaded": item.isAutoDownloaded,
            "downloaded_at": downloaded_dt,
            "updated_at": now,
            "deleted_at": now if item.deleted else None,
        }
        stmt = pg_insert(models.UserDownload).values(**values).on_conflict_do_update(
            index_elements=["user_id", "video_id"],
            set_={
                "title": values["title"],
                "uploader": values["uploader"],
                "duration": values["duration"],
                "thumbnail": values["thumbnail"],
                "file_size": values["file_size"],
                "is_auto_downloaded": values["is_auto_downloaded"],
                "downloaded_at": values["downloaded_at"],
                "updated_at": now,
                "deleted_at": values["deleted_at"],
            },
        )
        await session.execute(stmt)
        out.append(
            DownloadItem(
                videoId=item.videoId,
                title=item.title,
                uploader=item.uploader,
                duration=item.duration,
                thumbnail=item.thumbnail,
                fileSize=item.fileSize,
                isAutoDownloaded=item.isAutoDownloaded,
                downloadedAt=item.downloadedAt,
                deleted=item.deleted,
                updatedAt=_ms(now),
                deletedAt=_ms(now) if item.deleted else None,
            )
        )
    await session.commit()
    return DownloadsResponse(items=out, serverTime=_server_time_ms())


# ── /sync/settings ───────────────────────────────────────────────────────


@router.get("/settings", response_model=SettingsResponse)
async def get_settings(
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    row = (
        await session.execute(
            select(models.User).where(models.User.id == user["uid"])
        )
    ).scalar_one_or_none()
    return SettingsResponse(
        settings=(row.settings_json if row else None),
        serverTime=_server_time_ms(),
    )


@router.post("/settings", response_model=SettingsResponse)
async def push_settings(
    body: SettingsPushRequest,
    user: AuthedUser = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    incoming = dict(body.settings or {})
    # Last-write-wins on the embedded "_updated_at" (unix ms). A push
    # whose timestamp isn't newer than what we have is ignored — protects
    # against an out-of-order write from a device with a slow clock
    # clobbering a fresher one. If the client forgot to stamp the blob,
    # treat it as "now" (and write the stamp back) so the push isn't
    # silently dropped against a previously-stamped copy.
    incoming_ts = 0
    try:
        incoming_ts = int(incoming.get("_updated_at", 0))
    except (TypeError, ValueError):
        incoming_ts = 0
    if incoming_ts <= 0:
        incoming_ts = _server_time_ms()
        incoming["_updated_at"] = incoming_ts

    row = (
        await session.execute(
            select(models.User).where(models.User.id == user["uid"])
        )
    ).scalar_one_or_none()
    if row is None:
        # /auth/sync should have created the user already; if not, just
        # report back what was sent without persisting (the client retries).
        return SettingsResponse(settings=incoming, serverTime=_server_time_ms())

    existing = row.settings_json or {}
    existing_ts = 0
    try:
        existing_ts = int(existing.get("_updated_at", 0))
    except (TypeError, ValueError):
        existing_ts = 0

    if incoming_ts >= existing_ts:
        row.settings_json = incoming
        await session.commit()
        return SettingsResponse(settings=incoming, serverTime=_server_time_ms())
    else:
        # Stored copy is newer — keep it, hand it back so the client can
        # reconcile.
        return SettingsResponse(settings=existing, serverTime=_server_time_ms())
