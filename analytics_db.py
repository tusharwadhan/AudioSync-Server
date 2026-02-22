import asyncio
import time
import json
import os
import aiosqlite


class AnalyticsDB:
    def __init__(self, db_path: str = "analytics.db"):
        self.db_path = db_path
        self._event_buffer: list[dict] = []
        self._api_buffer: list[dict] = []
        self._flush_lock = asyncio.Lock()
        self._db: aiosqlite.Connection | None = None
        self._flush_task = None

    async def init(self):
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._create_tables()
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def close(self):
        if self._flush_task:
            self._flush_task.cancel()
        await self._flush()
        if self._db:
            await self._db.close()

    async def _create_tables(self):
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                client_id TEXT,
                client_name TEXT,
                video_id TEXT,
                title TEXT,
                query TEXT,
                room_code TEXT,
                detail TEXT,
                timestamp REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
            CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_events_client ON events(client_id);
            CREATE INDEX IF NOT EXISTS idx_events_video ON events(video_id);
            CREATE INDEX IF NOT EXISTS idx_events_room ON events(room_code);

            CREATE TABLE IF NOT EXISTS api_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER,
                response_time_ms REAL,
                client_ip TEXT,
                timestamp REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_api_timestamp ON api_requests(timestamp);
            CREATE INDEX IF NOT EXISTS idx_api_path ON api_requests(path);

            CREATE TABLE IF NOT EXISTS room_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_code TEXT NOT NULL,
                host_name TEXT,
                created_at REAL NOT NULL,
                destroyed_at REAL NOT NULL,
                peak_members INTEGER DEFAULT 1,
                songs_played INTEGER DEFAULT 0,
                had_password INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_room_history_created ON room_history(created_at);

            CREATE TABLE IF NOT EXISTS dashboard_sessions (
                token TEXT PRIMARY KEY,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL
            );
        """)
        await self._db.commit()

    # ---- Write methods (buffered) ----

    def log_event(self, event_type: str, client_id: str = None, client_name: str = None,
                  video_id: str = None, title: str = None, query: str = None,
                  room_code: str = None, detail: str = None):
        self._event_buffer.append({
            "event_type": event_type,
            "client_id": client_id[:8] if client_id else None,
            "client_name": client_name,
            "video_id": video_id,
            "title": title,
            "query": query,
            "room_code": room_code,
            "detail": detail,
            "timestamp": time.time(),
        })

    def log_api_request(self, method: str, path: str, status_code: int,
                        response_time_ms: float, client_ip: str):
        self._api_buffer.append({
            "method": method,
            "path": path,
            "status_code": status_code,
            "response_time_ms": response_time_ms,
            "client_ip": client_ip,
            "timestamp": time.time(),
        })

    async def log_room_destroyed(self, room_code: str, host_name: str,
                                  created_at: float, peak_members: int,
                                  songs_played: int, had_password: bool):
        if not self._db:
            return
        await self._db.execute(
            "INSERT INTO room_history (room_code, host_name, created_at, destroyed_at, "
            "peak_members, songs_played, had_password) VALUES (?,?,?,?,?,?,?)",
            (room_code, host_name, created_at, time.time(), peak_members, songs_played,
             1 if had_password else 0)
        )
        await self._db.commit()

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(2)
            try:
                await self._flush()
            except Exception as e:
                print(f"[Analytics] Flush error: {e}")

    async def _flush(self):
        async with self._flush_lock:
            events = self._event_buffer[:]
            api_reqs = self._api_buffer[:]
            self._event_buffer.clear()
            self._api_buffer.clear()

        if not events and not api_reqs:
            return
        if not self._db:
            return

        if events:
            await self._db.executemany(
                "INSERT INTO events (event_type, client_id, client_name, video_id, "
                "title, query, room_code, detail, timestamp) VALUES (?,?,?,?,?,?,?,?,?)",
                [(e["event_type"], e["client_id"], e["client_name"], e["video_id"],
                  e["title"], e["query"], e["room_code"], e["detail"], e["timestamp"])
                 for e in events]
            )
        if api_reqs:
            await self._db.executemany(
                "INSERT INTO api_requests (method, path, status_code, response_time_ms, "
                "client_ip, timestamp) VALUES (?,?,?,?,?,?)",
                [(r["method"], r["path"], r["status_code"], r["response_time_ms"],
                  r["client_ip"], r["timestamp"]) for r in api_reqs]
            )
        await self._db.commit()

    # ---- Session management ----

    async def create_session(self, token: str, duration: int = 86400):
        now = time.time()
        await self._db.execute(
            "INSERT OR REPLACE INTO dashboard_sessions (token, created_at, expires_at) VALUES (?,?,?)",
            (token, now, now + duration)
        )
        await self._db.commit()

    async def validate_session(self, token: str) -> bool:
        if not token:
            return False
        cursor = await self._db.execute(
            "SELECT expires_at FROM dashboard_sessions WHERE token = ?", (token,)
        )
        row = await cursor.fetchone()
        if not row:
            return False
        if time.time() > row["expires_at"]:
            await self._db.execute("DELETE FROM dashboard_sessions WHERE token = ?", (token,))
            await self._db.commit()
            return False
        return True

    async def delete_session(self, token: str):
        await self._db.execute("DELETE FROM dashboard_sessions WHERE token = ?", (token,))
        await self._db.commit()

    # ---- Read methods for dashboard ----

    async def get_event_counts(self, hours: int = 24) -> dict:
        cutoff = time.time() - hours * 3600
        counts = {}
        for etype in ["song_play", "search", "audio_extract", "room_create", "room_join",
                       "ws_connect", "lyrics_fetch", "browse"]:
            cursor = await self._db.execute(
                "SELECT COUNT(*) as cnt FROM events WHERE event_type = ? AND timestamp > ?",
                (etype, cutoff)
            )
            row = await cursor.fetchone()
            counts[etype] = row["cnt"] if row else 0

        # Unique users
        cursor = await self._db.execute(
            "SELECT COUNT(DISTINCT client_id) as cnt FROM events WHERE timestamp > ? AND client_id IS NOT NULL",
            (cutoff,)
        )
        row = await cursor.fetchone()
        counts["unique_users"] = row["cnt"] if row else 0

        return counts

    async def get_top_songs(self, days: int = 7, limit: int = 10) -> list[dict]:
        cutoff = time.time() - days * 86400
        cursor = await self._db.execute(
            "SELECT video_id, title, COUNT(*) as play_count FROM events "
            "WHERE event_type = 'song_play' AND timestamp > ? AND video_id IS NOT NULL "
            "GROUP BY video_id ORDER BY play_count DESC LIMIT ?",
            (cutoff, limit)
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def get_top_searches(self, days: int = 7, limit: int = 10) -> list[dict]:
        cutoff = time.time() - days * 86400
        cursor = await self._db.execute(
            "SELECT query, COUNT(*) as search_count FROM events "
            "WHERE event_type = 'search' AND timestamp > ? AND query IS NOT NULL "
            "GROUP BY LOWER(query) ORDER BY search_count DESC LIMIT ?",
            (cutoff, limit)
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def get_play_timeline(self, hours: int = 24) -> list[dict]:
        cutoff = time.time() - hours * 3600
        cursor = await self._db.execute(
            "SELECT CAST((timestamp - ?) / 3600 AS INTEGER) as hour_offset, "
            "COUNT(*) as count FROM events "
            "WHERE event_type = 'song_play' AND timestamp > ? "
            "GROUP BY hour_offset ORDER BY hour_offset",
            (cutoff, cutoff)
        )
        rows = [dict(row) for row in await cursor.fetchall()]
        # Fill gaps
        result = []
        hour_map = {r["hour_offset"]: r["count"] for r in rows}
        for h in range(hours):
            result.append({"hour_offset": h, "count": hour_map.get(h, 0)})
        return result

    async def get_recent_plays(self, limit: int = 50) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT video_id, title, client_id, client_name, timestamp FROM events "
            "WHERE event_type = 'song_play' ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def get_user_activity(self, days: int = 7) -> list[dict]:
        cutoff = time.time() - days * 86400
        cursor = await self._db.execute(
            "SELECT client_id, client_name, COUNT(*) as event_count, "
            "MAX(timestamp) as last_active, "
            "SUM(CASE WHEN event_type = 'song_play' THEN 1 ELSE 0 END) as plays, "
            "SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) as searches "
            "FROM events WHERE timestamp > ? AND client_id IS NOT NULL "
            "GROUP BY client_id ORDER BY event_count DESC",
            (cutoff,)
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def get_user_timeline(self, hours: int = 24) -> list[dict]:
        cutoff = time.time() - hours * 3600
        cursor = await self._db.execute(
            "SELECT CAST((timestamp - ?) / 3600 AS INTEGER) as hour_offset, "
            "COUNT(DISTINCT client_id) as unique_users FROM events "
            "WHERE timestamp > ? AND client_id IS NOT NULL "
            "GROUP BY hour_offset ORDER BY hour_offset",
            (cutoff, cutoff)
        )
        rows = [dict(row) for row in await cursor.fetchall()]
        hour_map = {r["hour_offset"]: r["unique_users"] for r in rows}
        return [{"hour_offset": h, "unique_users": hour_map.get(h, 0)} for h in range(hours)]

    async def get_user_log(self, client_id: str, limit: int = 100) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT event_type, video_id, title, query, room_code, detail, timestamp "
            "FROM events WHERE client_id = ? ORDER BY timestamp DESC LIMIT ?",
            (client_id, limit)
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def get_room_stats(self, days: int = 7) -> dict:
        cutoff = time.time() - days * 86400
        cursor = await self._db.execute(
            "SELECT COUNT(*) as total_rooms, "
            "AVG(destroyed_at - created_at) as avg_duration, "
            "MAX(peak_members) as max_peak_members, "
            "AVG(peak_members) as avg_peak_members, "
            "SUM(songs_played) as total_songs_played "
            "FROM room_history WHERE created_at > ?",
            (cutoff,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else {}

    async def get_room_history_list(self, limit: int = 50) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT room_code, host_name, created_at, destroyed_at, peak_members, "
            "songs_played, had_password FROM room_history "
            "ORDER BY destroyed_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def get_api_performance(self, hours: int = 24) -> dict:
        cutoff = time.time() - hours * 3600
        # Overall stats
        cursor = await self._db.execute(
            "SELECT COUNT(*) as total_requests, "
            "AVG(response_time_ms) as avg_ms, "
            "MAX(response_time_ms) as max_ms "
            "FROM api_requests WHERE timestamp > ?",
            (cutoff,)
        )
        overall = dict(await cursor.fetchone())

        # Per-endpoint stats
        cursor = await self._db.execute(
            "SELECT path, COUNT(*) as count, AVG(response_time_ms) as avg_ms "
            "FROM api_requests WHERE timestamp > ? "
            "GROUP BY path ORDER BY count DESC LIMIT 20",
            (cutoff,)
        )
        endpoints = [dict(row) for row in await cursor.fetchall()]

        return {"overall": overall, "endpoints": endpoints}

    async def get_request_timeline(self, hours: int = 24) -> list[dict]:
        cutoff = time.time() - hours * 3600
        cursor = await self._db.execute(
            "SELECT CAST((timestamp - ?) / 3600 AS INTEGER) as hour_offset, "
            "COUNT(*) as count, AVG(response_time_ms) as avg_ms FROM api_requests "
            "WHERE timestamp > ? GROUP BY hour_offset ORDER BY hour_offset",
            (cutoff, cutoff)
        )
        rows = [dict(row) for row in await cursor.fetchall()]
        hour_map = {r["hour_offset"]: r for r in rows}
        return [{"hour_offset": h, "count": hour_map.get(h, {}).get("count", 0),
                 "avg_ms": hour_map.get(h, {}).get("avg_ms", 0)} for h in range(hours)]

    async def get_error_rates(self, hours: int = 24) -> list[dict]:
        cutoff = time.time() - hours * 3600
        cursor = await self._db.execute(
            "SELECT path, "
            "SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as errors, "
            "COUNT(*) as total "
            "FROM api_requests WHERE timestamp > ? "
            "GROUP BY path HAVING errors > 0 ORDER BY errors DESC LIMIT 20",
            (cutoff,)
        )
        return [dict(row) for row in await cursor.fetchall()]

    async def cleanup_old_data(self, event_max_days: int = 30, api_max_days: int = 7):
        event_cutoff = time.time() - event_max_days * 86400
        api_cutoff = time.time() - api_max_days * 86400
        await self._db.execute("DELETE FROM events WHERE timestamp < ?", (event_cutoff,))
        await self._db.execute("DELETE FROM api_requests WHERE timestamp < ?", (api_cutoff,))
        await self._db.execute("DELETE FROM room_history WHERE destroyed_at < ?", (event_cutoff,))
        await self._db.execute("DELETE FROM dashboard_sessions WHERE expires_at < ?", (time.time(),))
        await self._db.commit()
