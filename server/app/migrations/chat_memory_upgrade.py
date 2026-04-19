"""
One-time migration: add new columns to conversations and messages tables
for the chat memory system upgrade.

Run: python -m app.migrations.chat_memory_upgrade
"""
import asyncio
from sqlalchemy import text
from app.core.database import engine


async def migrate():
    async with engine.begin() as conn:
        # ── conversations table ──
        await conn.execute(text("""
            ALTER TABLE conversations
            ADD COLUMN IF NOT EXISTS title_generated BOOLEAN NOT NULL DEFAULT false,
            ADD COLUMN IF NOT EXISTS summary TEXT,
            ADD COLUMN IF NOT EXISTS token_count INTEGER NOT NULL DEFAULT 0
        """))

        # ── messages table ──
        await conn.execute(text("""
            ALTER TABLE messages
            ADD COLUMN IF NOT EXISTS token_count INTEGER NOT NULL DEFAULT 0
        """))

        # Drop old position column and index
        await conn.execute(text("DROP INDEX IF EXISTS ix_messages_conv_position"))
        await conn.execute(text("ALTER TABLE messages DROP COLUMN IF EXISTS position"))

        # Create new index on (conversation_id, created_at)
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_messages_conv_created
            ON messages (conversation_id, created_at)
        """))

    print("✓ Chat memory migration complete.")


if __name__ == "__main__":
    asyncio.run(migrate())
