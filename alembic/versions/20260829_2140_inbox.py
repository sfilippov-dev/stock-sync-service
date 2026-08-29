"""таблица inbox для отсева повторной доставки из брокера

Revision ID: 7c1e94aa30d2
Revises: 436cd32e442b
Create Date: 2026-08-29 21:40:11.104882
"""
from alembic import op
import sqlalchemy as sa


revision = '7c1e94aa30d2'
down_revision = '436cd32e442b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'inbox',
        sa.Column('message_id', sa.String(length=128), nullable=False),
        sa.Column('target', sa.String(length=64), nullable=False),
        sa.Column('payload', sa.Text(), nullable=False),
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('message_id'),
    )
    op.create_index('ix_inbox_received_at', 'inbox', ['received_at'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_inbox_received_at', table_name='inbox')
    op.drop_table('inbox')
