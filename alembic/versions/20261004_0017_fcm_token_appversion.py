"""user_fcm_tokens.app_version_code — gates custom-render morning pushes

Revision ID: 0017_fcm_token_appversion
Revises: 0016_morning_push
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0017_fcm_token_appversion"
down_revision: Union[str, None] = "0016_morning_push"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "user_fcm_tokens",
        sa.Column("app_version_code", sa.Integer(), nullable=False,
                  server_default=sa.text("0")),
    )


def downgrade() -> None:
    op.drop_column("user_fcm_tokens", "app_version_code")
