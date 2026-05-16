"""dm chat features: reactions / reply / edit / delete / share_moment

Revision ID: 0005_dm_chat_features
Revises: 0004_social_tables
Create Date: 2026-05-17

Adds the columns DMs need to reach feature parity with room chat:

  * reactions JSONB — `{emoji: [uid, uid, ...]}` matching the room
    chat in-memory shape.
  * reply_to_message_id BIGINT — references another row in this
    same table; intentionally NOT a foreign key because the
    replied-to message may have been pruned by the 1h post-read
    cleanup job and we still want the reply text to render with a
    "(message no longer available)" placeholder client-side.
  * edited_at TIMESTAMPTZ — set when the sender edits their own
    text; client shows an "edited" indicator.
  * deleted BOOLEAN — tombstone so a deleted message still
    renders ("Message deleted" placeholder) for users who already
    have it on screen. The actual prune still happens via the
    read-grace window.
  * share_moment JSONB — payload for the special "share-moment"
    card type (now-playing snapshot, lyric highlight, etc.).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0005_dm_chat_features"
down_revision: Union[str, None] = "0004_social_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "dm_messages",
        sa.Column(
            "reactions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "dm_messages",
        sa.Column("reply_to_message_id", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "dm_messages",
        sa.Column("edited_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "dm_messages",
        sa.Column(
            "deleted",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "dm_messages",
        sa.Column(
            "share_moment",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )

    # Relax the has-content CHECK so a share_moment-only DM is valid.
    op.drop_constraint("ck_dm_messages_has_content", "dm_messages", type_="check")
    op.create_check_constraint(
        "ck_dm_messages_has_content",
        "dm_messages",
        "text IS NOT NULL OR np_video_id IS NOT NULL OR share_moment IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_constraint("ck_dm_messages_has_content", "dm_messages", type_="check")
    op.create_check_constraint(
        "ck_dm_messages_has_content",
        "dm_messages",
        "text IS NOT NULL OR np_video_id IS NOT NULL",
    )
    op.drop_column("dm_messages", "share_moment")
    op.drop_column("dm_messages", "deleted")
    op.drop_column("dm_messages", "edited_at")
    op.drop_column("dm_messages", "reply_to_message_id")
    op.drop_column("dm_messages", "reactions")
