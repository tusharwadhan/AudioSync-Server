"""moment_shares: short-id pointers to a song window, for out-of-app links

Revision ID: 0019_moment_shares
Revises: 0018_dm_disappear_v2
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0019_moment_shares"
down_revision: Union[str, None] = "0018_dm_disappear_v2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "moment_shares",
        sa.Column("id", sa.String(16), primary_key=True),
        sa.Column(
            "user_id", sa.String(128),
            sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_moment_shares_user_created", "moment_shares", ["user_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_moment_shares_user_created", table_name="moment_shares")
    op.drop_table("moment_shares")
