"""User avatar — custom-photo flag + photo_updated_at version stamp

Revision ID: 0009_user_avatar
Revises: 0008_dm_retention_mode
Create Date: 2026-06-09

Adds two columns to `users` so we can support user-set profile photos
without /auth/sync clobbering them on every cold start, and so clients
can detect avatar changes through Coil's URL-keyed memory cache + our
own DmAvatarCache disk cache (both of which serve stale bitmaps when
the URL string doesn't change — Firebase Storage doesn't rotate
download-URL tokens on blob overwrite).

  * users.custom_photo BOOLEAN — true if the user has uploaded their
    own avatar via PATCH /api/v1/users/me. /auth/sync's photo_url
    write is now gated on `not custom_photo` so Google sign-in cannot
    revert a custom avatar.
  * users.photo_updated_at BIGINT — epoch milliseconds of the last
    avatar change. Server appends `?v={this}` to every emitted
    photo_url so Coil + DmAvatarCache treat URL changes as cache
    misses and refetch.

Defaults: custom_photo=false (preserves current /auth/sync behaviour
for existing rows); photo_updated_at=NULL (rendered as `?v=0` until
the user uploads — Coil still keys correctly because the buster
string is consistent).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0009_user_avatar"
down_revision: Union[str, None] = "0008_dm_retention_mode"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column(
            "custom_photo",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "users",
        sa.Column("photo_updated_at", sa.BigInteger(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "photo_updated_at")
    op.drop_column("users", "custom_photo")
