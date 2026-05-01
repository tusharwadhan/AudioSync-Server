"""
No-op stub for the previous SQLite-backed analytics module.

The full implementation was removed when consolidating to Postgres for the
new sync/auth data layer. Call sites in main.py (analytics.log_event(...),
analytics.log_api_request(...), etc.) remain in place but become silent
no-ops, so we don't have to touch every reference.

To restore real analytics later: replace this file with the original
implementation, or hook these methods up to the new Postgres tables.
"""


class AnalyticsDB:
    def __init__(self, db_path: str = ""):
        # path arg kept for compatibility with the old constructor signature
        self.db_path = db_path

    # ── Async methods ──

    async def init(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def cleanup_old_data(self) -> None:
        return None

    async def log_room_destroyed(self, *args, **kwargs) -> None:
        return None

    # ── Sync methods ──

    def log_api_request(self, *args, **kwargs) -> None:
        return None

    def log_event(self, *args, **kwargs) -> None:
        return None
