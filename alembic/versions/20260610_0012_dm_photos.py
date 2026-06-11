"""DM one-time photos (R4)

Revision ID: 0012_dm_photos
Revises: 0011_chat_ops
Create Date: 2026-06-10

Adds two columns to `dm_messages` for one-time photo support
(WhatsApp-style "view once").

  * dm_messages.photo_url TEXT NULL — Cloudinary secure_url of the
    photo blob. NULLed when the recipient opens the message so the
    snapshot stops emitting the URL after the photo is destroyed.

  * dm_messages.view_once_status TEXT NULL — enum 'sent' | 'opened'
    (NULL for non-photo messages). The CHECK constraint enforces
    the enum; a second CHECK enforces the cross-column invariant
    that `photo_url IS NOT NULL` iff `view_once_status = 'sent'`.

Why a tombstone instead of deleting the row: the receiver's
"Photo no longer available" placeholder + the sender's "Opened"
badge both need the row to persist, just without the asset URL.

Audit fix #3: CHECK constraints prevent enum typos and the
URL-NULL-on-open invariant from silently breaking.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0012_dm_photos"
down_revision: Union[str, None] = "0011_chat_ops"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "dm_messages",
        sa.Column("photo_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "dm_messages",
        sa.Column("view_once_status", sa.String(length=16), nullable=True),
    )
    # Persist client_nonce so the server-side `dm_view_once_opened`
    # broadcast can carry it back to the sender — without
    # persistence, a recipient who opens the photo BEFORE the
    # sender's dm_send echo has landed (slow sender network +
    # fast recipient open) gets a broadcast with only `id`, which
    # the sender's optimistic-message ID (negative until echo
    # replaces) can't match. The optimistic-match pattern needs
    # nonce as a fallback. Audit fix #1.
    op.add_column(
        "dm_messages",
        sa.Column("client_nonce", sa.String(length=64), nullable=True),
    )
    # Relax the existing `has_content` CHECK so a view-once photo
    # (which has neither text nor np nor share_moment) is a valid
    # message kind. After this change the four-way disjunction
    # allows: text OR now-playing OR share-moment OR view-once-photo.
    # An "opened" tombstone where status='opened' and
    # photo_url=NULL won't satisfy this check unless ANOTHER content
    # field is set; but we ONLY ever NULL photo_url AT THE SAME
    # MOMENT the row's view_once_status flips to 'opened', and the
    # row was originally inserted with photo_url=NOT NULL — so the
    # invariant ck_dm_messages_view_once_url_invariant guarantees
    # we never insert/update into an invalid state.
    #
    # Wait — that means after open, the has_content CHECK fails:
    # text=NULL, np_video_id=NULL, share_moment=NULL,
    # photo_url=NULL. So we need to also OR view_once_status here.
    op.drop_constraint("ck_dm_messages_has_content", "dm_messages")
    op.create_check_constraint(
        "ck_dm_messages_has_content",
        "dm_messages",
        "text IS NOT NULL OR np_video_id IS NOT NULL "
        "OR share_moment IS NOT NULL OR photo_url IS NOT NULL "
        "OR view_once_status IS NOT NULL",
    )
    # Enum guard — typos in the handler can't corrupt the column.
    op.create_check_constraint(
        "ck_dm_messages_view_once_status",
        "dm_messages",
        "view_once_status IS NULL OR view_once_status IN ('sent', 'opened')",
    )
    # Cross-column invariant — photo_url is present iff status='sent'.
    # When status flips to 'opened', the handler NULLs photo_url
    # atomically; this constraint ensures the two never diverge.
    op.create_check_constraint(
        "ck_dm_messages_view_once_url_invariant",
        "dm_messages",
        "(view_once_status = 'sent') = (photo_url IS NOT NULL)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_dm_messages_view_once_url_invariant", "dm_messages")
    op.drop_constraint("ck_dm_messages_view_once_status", "dm_messages")
    op.drop_constraint("ck_dm_messages_has_content", "dm_messages")
    op.create_check_constraint(
        "ck_dm_messages_has_content",
        "dm_messages",
        "text IS NOT NULL OR np_video_id IS NOT NULL "
        "OR share_moment IS NOT NULL",
    )
    op.drop_column("dm_messages", "client_nonce")
    op.drop_column("dm_messages", "view_once_status")
    op.drop_column("dm_messages", "photo_url")
