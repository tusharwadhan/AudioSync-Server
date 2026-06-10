"""User status text (R2)

Revision ID: 0010_user_status
Revises: 0009_user_avatar
Create Date: 2026-06-10

Adds a short user-set status string ("Studying", "Listening to X",
etc.) that renders next to the display name in PeerProfileSheet,
friends list, and online users list.

  * users.status_text VARCHAR(100) — nullable; empty string in
    PATCH body clears it (server converts to NULL). 100-char cap
    enforced by the PATCH handler.

Broadcasts go out as `user_status_changed` WS events to friends
only (same audience as `user_avatar_changed` — privacy-correct).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0010_user_status"
down_revision: Union[str, None] = "0009_user_avatar"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("status_text", sa.String(length=100), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "status_text")
