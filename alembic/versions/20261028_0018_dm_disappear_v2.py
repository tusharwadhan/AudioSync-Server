"""dm_thread_state: columns for the disappearing-messages v2 rules

All columns are nullable with no default and no backfill, so this is a
metadata-only change in Postgres and leaves every existing row untouched.
Nothing reads them yet - the ORM model deliberately does NOT map them in
this release (start.sh runs alembic non-fatally, so code must not depend on
a column until `alembic current` confirms this revision applied).

  retention_since_id  first message id the current disappearing mode covers
  last_seen_id_a/b    per-side "seen up to" marker for chat event rows
  last_activity_*     tombstone for the chat-list "Opened/Received/Sent"
                      preview once the underlying rows are deleted

Revision ID: 0018_dm_disappear_v2
Revises: 0017_fcm_token_appversion
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0018_dm_disappear_v2"
down_revision: Union[str, None] = "0017_fcm_token_appversion"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_COLUMNS = (
    ("retention_since_id", "BIGINT"),
    ("last_seen_id_a", "BIGINT"),
    ("last_seen_id_b", "BIGINT"),
    ("last_activity_at", "TIMESTAMP WITH TIME ZONE"),
    ("last_activity_from", "VARCHAR(128)"),
    ("last_activity_kind", "VARCHAR(16)"),
)


def upgrade() -> None:
    # IF NOT EXISTS keeps a re-run (or a partially-applied earlier attempt)
    # harmless.
    for name, ddl in _COLUMNS:
        op.execute(f"ALTER TABLE dm_thread_state ADD COLUMN IF NOT EXISTS {name} {ddl}")


def downgrade() -> None:
    for name, _ in reversed(_COLUMNS):
        op.execute(f"ALTER TABLE dm_thread_state DROP COLUMN IF EXISTS {name}")
