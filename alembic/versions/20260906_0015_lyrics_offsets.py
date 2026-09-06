"""lyrics timing offsets

Revision ID: 0015_lyrics_offsets
Revises: 0014_playback_error_logs
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0015_lyrics_offsets"
down_revision: Union[str, None] = "0014_playback_error_logs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "lyrics_offsets",
        sa.Column("video_id", sa.String(length=32), nullable=False),
        sa.Column("lyrics_hash", sa.String(length=64), nullable=False),
        sa.Column("uid", sa.String(length=128), nullable=False),
        sa.Column("offset_ms", sa.Integer(), nullable=False,
                  server_default=sa.text("0")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("video_id", "lyrics_hash", "uid"),
    )
    # The read path always asks "every submission for this song+lyrics",
    # never "everything by this user", so the index follows that shape.
    op.create_index(
        "ix_lyrics_offsets_lookup", "lyrics_offsets",
        ["video_id", "lyrics_hash"],
    )


def downgrade() -> None:
    op.drop_index("ix_lyrics_offsets_lookup", table_name="lyrics_offsets")
    op.drop_table("lyrics_offsets")
