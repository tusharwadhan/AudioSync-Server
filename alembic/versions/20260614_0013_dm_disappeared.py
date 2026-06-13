"""Per-viewer disappearing messages (Snapchat-style)

Revision ID: 0013_dm_disappeared
Revises: 0012_dm_photos
Create Date: 2026-06-14

Replaces the old GLOBAL "delete on leave" behaviour (which deleted a
disappear-thread's read messages for BOTH users the moment either one
left — so the peer lost messages mid-view) with a PER-VIEWER hide.

  * dm_disappeared (user_uid, message_id) — "user_uid has dismissed
    message_id from their own view." When a user leaves a
    disappear-mode chat after reading, every read message in that
    thread (both directions) is recorded here for THAT user only.
    The snapshot filters these out of the leaving user's history; the
    peer is untouched until they themselves leave + reopen.

A message is hard-deleted only once BOTH participants have it in
dm_disappeared (handled in handle_dm_clear_on_leave + the periodic
prune), so storage stays bounded. FK CASCADE on message_id cleans up
these rows when the underlying message is GC'd.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0013_dm_disappeared"
down_revision: Union[str, None] = "0012_dm_photos"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "dm_disappeared",
        sa.Column("user_uid", sa.String(length=128), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("user_uid", "message_id"),
        sa.ForeignKeyConstraint(
            ["user_uid"], ["users.id"],
            name="fk_dm_disappeared_user", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["message_id"], ["dm_messages.id"],
            name="fk_dm_disappeared_message", ondelete="CASCADE",
        ),
    )
    # Lookup by message_id for the both-sides-disappeared GC pass.
    op.create_index(
        "ix_dm_disappeared_message", "dm_disappeared", ["message_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_dm_disappeared_message", table_name="dm_disappeared")
    op.drop_table("dm_disappeared")
