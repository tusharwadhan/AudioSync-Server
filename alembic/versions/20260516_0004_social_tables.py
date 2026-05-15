"""social v1: lounge + DMs + friendships

Revision ID: 0004_social_tables
Revises: 0003_downloads_and_settings
Create Date: 2026-05-16

Adds the four tables that back the v1 social features (one global
lounge, global online presence, opt-in DMs with a stranger-gate):

  * friendships — created the moment either side accepts a pending
    DM thread. (uid_a, uid_b) is always stored with uid_a < uid_b so
    we never get duplicate rows for the same pair.

  * dm_thread_state — per-pair gate state. Lets the server know
    whether a fresh dm_send should land as a 'pending' request (gate
    still up) or a normal message (gate crossed via friendship).

  * dm_messages — actual DM payloads. Kept on the server until
    read_at is set AND a grace window has passed (~1h), so concurrent
    sessions still see them, then the periodic prune task removes
    them. The "wipe on app restart" UX is enforced client-side; the
    server simply doesn't return read+aged messages on snapshot.

  * lounge_messages — the global lounge. Rolling last-200 retention
    enforced by a periodic prune task (or trigger).

No presence table — connected users are tracked in an in-memory map
on the FastAPI process. On server restart everyone is offline until
they reconnect.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0004_social_tables"
down_revision: Union[str, None] = "0003_downloads_and_settings"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── friendships ───────────────────────────────────────────────────────
    op.create_table(
        "friendships",
        sa.Column("uid_a", sa.String(length=128), nullable=False),
        sa.Column("uid_b", sa.String(length=128), nullable=False),
        sa.Column(
            "formed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("uid_a", "uid_b"),
        sa.ForeignKeyConstraint(["uid_a"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["uid_b"], ["users.id"], ondelete="CASCADE"),
        sa.CheckConstraint("uid_a < uid_b", name="ck_friendships_uid_order"),
    )
    op.create_index("ix_friendships_uid_a", "friendships", ["uid_a"])
    op.create_index("ix_friendships_uid_b", "friendships", ["uid_b"])

    # ── dm_thread_state ───────────────────────────────────────────────────
    op.create_table(
        "dm_thread_state",
        sa.Column("uid_a", sa.String(length=128), nullable=False),
        sa.Column("uid_b", sa.String(length=128), nullable=False),
        # 'pending_from_a' | 'pending_from_b' | 'accepted'
        # | 'declined_by_a' | 'declined_by_b'
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("uid_a", "uid_b"),
        sa.ForeignKeyConstraint(["uid_a"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["uid_b"], ["users.id"], ondelete="CASCADE"),
        sa.CheckConstraint("uid_a < uid_b", name="ck_dm_thread_uid_order"),
    )

    # ── dm_messages ───────────────────────────────────────────────────────
    op.create_table(
        "dm_messages",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("from_uid", sa.String(length=128), nullable=False),
        sa.Column("to_uid", sa.String(length=128), nullable=False),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("np_video_id", sa.String(length=32), nullable=True),
        sa.Column("np_title", sa.String(length=512), nullable=True),
        sa.Column("np_artist", sa.String(length=255), nullable=True),
        sa.Column("np_thumbnail", sa.String(length=1024), nullable=True),
        sa.Column(
            "sent_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["from_uid"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["to_uid"], ["users.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "text IS NOT NULL OR np_video_id IS NOT NULL",
            name="ck_dm_messages_has_content",
        ),
    )
    # Partial indexes on unread rows — these are the hot paths for the
    # snapshot endpoint and the WS reconnect catch-up.
    op.create_index(
        "ix_dm_messages_to_unread",
        "dm_messages",
        ["to_uid", "sent_at"],
        postgresql_where=sa.text("read_at IS NULL"),
    )
    op.create_index(
        "ix_dm_messages_from_unread",
        "dm_messages",
        ["from_uid", "sent_at"],
        postgresql_where=sa.text("read_at IS NULL"),
    )

    # ── lounge_messages ───────────────────────────────────────────────────
    op.create_table(
        "lounge_messages",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("from_uid", sa.String(length=128), nullable=False),
        # Snapshot of the sender's identity at send-time so old messages
        # keep showing the right name/avatar even if the user later
        # renames or signs out of Google.
        sa.Column("from_name", sa.String(length=255), nullable=False),
        sa.Column("from_avatar_url", sa.String(length=1024), nullable=True),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("np_video_id", sa.String(length=32), nullable=True),
        sa.Column("np_title", sa.String(length=512), nullable=True),
        sa.Column("np_artist", sa.String(length=255), nullable=True),
        sa.Column("np_thumbnail", sa.String(length=1024), nullable=True),
        sa.Column(
            "sent_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["from_uid"], ["users.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "text IS NOT NULL OR np_video_id IS NOT NULL",
            name="ck_lounge_messages_has_content",
        ),
    )
    # Newest-first pull for the snapshot's "last 200 messages".
    op.create_index(
        "ix_lounge_messages_sent_at_desc",
        "lounge_messages",
        [sa.text("sent_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_lounge_messages_sent_at_desc", table_name="lounge_messages")
    op.drop_table("lounge_messages")

    op.drop_index("ix_dm_messages_from_unread", table_name="dm_messages")
    op.drop_index("ix_dm_messages_to_unread", table_name="dm_messages")
    op.drop_table("dm_messages")

    op.drop_table("dm_thread_state")

    op.drop_index("ix_friendships_uid_b", table_name="friendships")
    op.drop_index("ix_friendships_uid_a", table_name="friendships")
    op.drop_table("friendships")
