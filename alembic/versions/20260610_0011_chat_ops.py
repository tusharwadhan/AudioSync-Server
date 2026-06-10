"""DM chat-ops: per-user mute + per-thread clear (R2)

Revision ID: 0011_chat_ops
Revises: 0010_user_status
Create Date: 2026-06-10

Two new junction tables backing the R2 three-dot menu actions:

  * muted_peers (user_uid, peer_uid, muted_at)
      Each row is a one-way "user_uid has muted peer_uid for DM
      notifications." `_push_dm_fcm` skips the push when the
      recipient has muted the sender. In-app delivery + unread
      counts are unaffected — mute only suppresses the
      notification path. Snapshot returns the caller's muted list
      under the key `muted_peers`.

  * chat_clears (user_uid, peer_uid, cleared_before_ts)
      Per-thread cutoff timestamp (epoch ms). The snapshot's three
      DM arrays (history_dms, unread_dms, sent_unread_dms) drop
      messages with sent_at <= cleared_before_ts for the requesting
      user only — the peer's view is unaffected (WhatsApp-style
      one-sided clear).

Both tables: composite PK on (user_uid, peer_uid). Indexes on
user_uid lookup (covered by the composite PK already, but explicit
indexes help in case the access pattern shifts).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0011_chat_ops"
down_revision: Union[str, None] = "0010_user_status"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Both tables FK back to `users` with ON DELETE CASCADE so a
    # deleted account doesn't leave orphan mute/clear rows that
    # would inflate every future snapshot (audit finding #8).
    op.create_table(
        "muted_peers",
        sa.Column("user_uid", sa.String(length=128), nullable=False),
        sa.Column("peer_uid", sa.String(length=128), nullable=False),
        sa.Column(
            "muted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("user_uid", "peer_uid"),
        sa.ForeignKeyConstraint(
            ["user_uid"], ["users.id"],
            name="fk_muted_peers_user", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["peer_uid"], ["users.id"],
            name="fk_muted_peers_peer", ondelete="CASCADE",
        ),
    )
    op.create_table(
        "chat_clears",
        sa.Column("user_uid", sa.String(length=128), nullable=False),
        sa.Column("peer_uid", sa.String(length=128), nullable=False),
        sa.Column("cleared_before_ts", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("user_uid", "peer_uid"),
        sa.ForeignKeyConstraint(
            ["user_uid"], ["users.id"],
            name="fk_chat_clears_user", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["peer_uid"], ["users.id"],
            name="fk_chat_clears_peer", ondelete="CASCADE",
        ),
    )


def downgrade() -> None:
    op.drop_table("chat_clears")
    op.drop_table("muted_peers")
