"""lounge chat features: reactions + reply

Revision ID: 0006_lounge_chat_features
Revises: 0005_dm_chat_features
Create Date: 2026-05-18

Brings the global lounge halfway to DM chat parity:

  * lounge_messages.reactions JSONB — same shape as the DM/room
    chat ({emoji: [uid, ...]})
  * lounge_messages.reply_to_message_id BIGINT — soft reference
    (no FK; the replied-to row may have been pruned by the rolling
    last-200 retention, and the client renders a placeholder in
    that case)

Deliberately NOT adding edit / delete to lounge: deletes in a
public chat surface raise moderation questions that aren't in
scope for v1, and edits are confusing without delete (the only
way to take something back becomes "post a correction").

Typing indicators are skipped on purpose too — N-people-typing
in a global chat is just noise.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0006_lounge_chat_features"
down_revision: Union[str, None] = "0005_dm_chat_features"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "lounge_messages",
        sa.Column(
            "reactions",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "lounge_messages",
        sa.Column("reply_to_message_id", sa.BigInteger(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("lounge_messages", "reply_to_message_id")
    op.drop_column("lounge_messages", "reactions")
