"""persist user FCM token

Revision ID: 0007_user_fcm_token
Revises: 0006_lounge_chat_features
Create Date: 2026-05-22

Adds users.fcm_token (+ updated_at) so DM push notifications survive a
server restart/redeploy.

Previously the recipient's device token lived only in social.py's
in-memory `_fcm_tokens_by_uid` dict, which is rebuilt only when a client
sends `social_fcm_register` (on social_subscribe). Every deploy wiped the
process and therefore the map, so an OFFLINE user could not be pushed a
DM until they reopened the app and reconnected — i.e. "all users stop
receiving FCM after the server updates". Persisting the token lets
`_push_dm_fcm` fall back to the DB on a cold cache.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0007_user_fcm_token"
down_revision: Union[str, None] = "0006_lounge_chat_features"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("fcm_token", sa.String(length=512), nullable=True))
    op.add_column(
        "users",
        sa.Column("fcm_token_updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "fcm_token_updated_at")
    op.drop_column("users", "fcm_token")
