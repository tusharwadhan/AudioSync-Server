"""DM retention mode (keep vs disappear) + message event type

Revision ID: 0008_dm_retention_mode
Revises: 0007_user_fcm_token
Create Date: 2026-05-22

Adds a per-conversation retention setting plus a way to mark system
notices in the message stream.

  * dm_thread_state.retention_mode VARCHAR — 'keep' (default) keeps the
    conversation history; 'disappear' deletes messages once the other
    party has viewed them and left the chat (Snapchat-style). Either
    user can switch it; last-write-wins.
  * dm_messages.event_type VARCHAR (nullable) — null for normal content;
    'retention_keep' / 'retention_disappear' for the inline "X switched
    the conversation to …" notice. Event rows are never auto-deleted by
    the disappear sweep (they're tiny metadata).

Default 'keep' is a deliberate reversal of the previous behaviour (read
DMs were pruned ~1h after read regardless). History now persists unless
a user opts the thread into disappearing mode.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0008_dm_retention_mode"
down_revision: Union[str, None] = "0007_user_fcm_token"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "dm_thread_state",
        sa.Column(
            "retention_mode",
            sa.String(length=16),
            nullable=False,
            server_default="keep",
        ),
    )
    op.add_column(
        "dm_messages",
        sa.Column("event_type", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dm_messages", "event_type")
    op.drop_column("dm_thread_state", "retention_mode")
