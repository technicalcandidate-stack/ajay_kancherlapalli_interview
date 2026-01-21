"""
Migration script to add vector search capabilities.
Run this once to:
1. Install pgvector extension
2. Add embedding column to events
3. Ensure raw_content is stored
4. Generate embeddings for existing events
"""
import asyncio
import os
from typing import List, Optional
import asyncpg

from embedding import (
    get_embeddings_batch, 
    prepare_event_for_embedding,
    EMBEDDING_DIMENSION
)


async def migrate(db_url: str):
    """Run the full migration."""
    print("Connecting to database...")
    conn = await asyncpg.connect(db_url)
    
    try:
        # Step 1: Install pgvector extension
        print("\n1. Installing pgvector extension...")
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            print("   ✓ pgvector extension installed")
        except Exception as e:
            print(f"   ✗ Error installing pgvector: {e}")
            print("   You may need to install pgvector first:")
            print("   brew install pgvector")
            print("   Then in psql: CREATE EXTENSION vector;")
            return
        
        # Step 2: Add columns if they don't exist
        print("\n2. Adding columns to events table...")
        
        # Check if raw_content exists
        has_raw = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'events' AND column_name = 'raw_content'
            )
        """)
        if not has_raw:
            await conn.execute("ALTER TABLE events ADD COLUMN raw_content TEXT")
            print("   ✓ Added raw_content column")
        else:
            print("   ✓ raw_content column already exists")
        
        # Check if embedding column exists
        has_embedding = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'events' AND column_name = 'content_embedding'
            )
        """)
        if not has_embedding:
            await conn.execute(f"""
                ALTER TABLE events 
                ADD COLUMN content_embedding vector({EMBEDDING_DIMENSION})
            """)
            print(f"   ✓ Added content_embedding column (dimension: {EMBEDDING_DIMENSION})")
        else:
            print("   ✓ content_embedding column already exists")
        
        # Step 3: Create index for fast similarity search
        print("\n3. Creating vector index...")
        try:
            # Check if index exists
            has_idx = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE indexname = 'events_embedding_idx'
                )
            """)
            if not has_idx:
                # Use ivfflat for larger datasets, or hnsw for smaller
                event_count = await conn.fetchval("SELECT COUNT(*) FROM events")
                
                if event_count > 1000:
                    # ivfflat is better for larger datasets
                    lists = min(event_count // 10, 100)
                    await conn.execute(f"""
                        CREATE INDEX events_embedding_idx ON events 
                        USING ivfflat (content_embedding vector_cosine_ops)
                        WITH (lists = {lists})
                    """)
                    print(f"   ✓ Created ivfflat index with {lists} lists")
                else:
                    # hnsw is better for smaller datasets and exact results
                    await conn.execute("""
                        CREATE INDEX events_embedding_idx ON events 
                        USING hnsw (content_embedding vector_cosine_ops)
                    """)
                    print("   ✓ Created hnsw index")
            else:
                print("   ✓ Vector index already exists")
        except Exception as e:
            print(f"   Warning: Could not create index: {e}")
            print("   You can create it manually later for better performance")
        
        # Step 4: Check raw_content population
        print("\n4. Checking raw_content population...")
        null_count = await conn.fetchval("""
            SELECT COUNT(*) FROM events WHERE raw_content IS NULL
        """)
        total_count = await conn.fetchval("SELECT COUNT(*) FROM events")
        
        if null_count > 0:
            print(f"   ⚠ {null_count}/{total_count} events have NULL raw_content")
            print("   You may need to backfill from your JSONL files")
            print("   See backfill_raw_content() function")
        else:
            print(f"   ✓ All {total_count} events have raw_content")
        
        # Step 5: Generate embeddings for events that don't have them
        print("\n5. Generating embeddings for events...")
        await generate_embeddings(conn)
        
        print("\n✓ Migration complete!")
        
    finally:
        await conn.close()


async def generate_embeddings(conn: asyncpg.Connection, batch_size: int = 50):
    """Generate embeddings for events that don't have them."""
    
    # Get events needing embeddings
    events = await conn.fetch("""
        SELECT id, event_type, summary, raw_content
        FROM events
        WHERE content_embedding IS NULL
          AND (raw_content IS NOT NULL OR summary IS NOT NULL)
        ORDER BY id
    """)
    
    if not events:
        print("   ✓ All events already have embeddings")
        return
    
    print(f"   Generating embeddings for {len(events)} events...")
    
    # Process in batches
    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        
        # Prepare texts for embedding
        texts = []
        for e in batch:
            text = e["raw_content"] or e["summary"] or ""
            if not text.strip():
                texts.append("No content available")
            else:
                # Truncate very long texts (embedding models have limits)
                texts.append(text[:8000])  # ~2000 tokens
        
        # Generate embeddings
        try:
            embeddings = await get_embeddings_batch(texts)
            
            # Update database
            for j, (event, embedding) in enumerate(zip(batch, embeddings)):
                embedding_str = f"[{','.join(map(str, embedding))}]"
                await conn.execute("""
                    UPDATE events 
                    SET content_embedding = $1::vector
                    WHERE id = $2
                """, embedding_str, event["id"])
            
            print(f"   Processed {min(i + batch_size, len(events))}/{len(events)} events")
            
        except Exception as e:
            print(f"   Error processing batch starting at {i}: {e}")
            continue
    
    # Verify
    remaining = await conn.fetchval("""
        SELECT COUNT(*) FROM events WHERE content_embedding IS NULL
    """)
    print(f"   ✓ Done. {remaining} events still without embeddings")


async def backfill_raw_content(conn: asyncpg.Connection, jsonl_path: str):
    """
    Backfill raw_content from original JSONL files.
    Call this if your events are missing raw_content.
    """
    import json
    
    print(f"Backfilling raw_content from {jsonl_path}...")
    
    # Build lookup of existing events by key fields
    events = await conn.fetch("""
        SELECT id, event_type, event_date, account_id
        FROM events WHERE raw_content IS NULL
    """)
    
    if not events:
        print("No events need backfilling")
        return
    
    updated = 0
    with open(jsonl_path, "r") as f:
        for line in f:
            record = json.loads(line)
            
            # Extract raw content based on type
            event_type = record.get("type", "").lower()
            raw_content = None
            
            if event_type == "email":
                body = record.get("body", "")
                subject = record.get("subject", "")
                raw_content = f"Subject: {subject}\n\n{body}"
            elif event_type == "phone_call":
                raw_content = record.get("transcript", "") or record.get("summary", "")
            elif event_type == "sms":
                raw_content = record.get("content", "") or record.get("message", "")
            
            if raw_content:
                # Try to match to existing event
                # This is approximate - you may need to adjust matching logic
                result = await conn.execute("""
                    UPDATE events 
                    SET raw_content = $1
                    WHERE raw_content IS NULL
                      AND event_type = $2
                      AND id IN (SELECT id FROM events LIMIT 1)
                """, raw_content, event_type)
                
                if result != "UPDATE 0":
                    updated += 1
    
    print(f"Updated {updated} events with raw_content")


# CLI
if __name__ == "__main__":
    import sys
    
    db_url = os.getenv("DATABASE_URL", "postgresql://localhost/memory_system")
    
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        if len(sys.argv) < 3:
            print("Usage: python migrate_to_vector.py backfill <jsonl_path>")
            sys.exit(1)
        
        async def run_backfill():
            conn = await asyncpg.connect(db_url)
            await backfill_raw_content(conn, sys.argv[2])
            await conn.close()
        
        asyncio.run(run_backfill())
    else:
        asyncio.run(migrate(db_url))