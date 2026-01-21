"""
Generate embeddings for all events that don't have them yet.
Run: python generate_embeddings.py
"""
import os
import asyncio
import httpx
import asyncpg

VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/memory_system")
BATCH_SIZE = 20  # Smaller batches


async def get_embeddings_batch(texts: list[str], max_retries: int = 5) -> list[list[float]]:
    """Get embeddings from Voyage AI with retry logic."""
    async with httpx.AsyncClient() as client:
        for attempt in range(max_retries):
            try:
                response = await client.post(
                    "https://api.voyageai.com/v1/embeddings",
                    headers={"Authorization": f"Bearer {VOYAGE_API_KEY}"},
                    json={
                        "model": "voyage-3",
                        "input": texts,
                        "input_type": "document"
                    },
                    timeout=60.0
                )
                response.raise_for_status()
                data = response.json()["data"]
                sorted_data = sorted(data, key=lambda x: x["index"])
                return [d["embedding"] for d in sorted_data]
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait_time = 2 ** attempt * 5  # 5, 10, 20, 40, 80 seconds
                    print(f"    Rate limited, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    raise
        raise Exception("Max retries exceeded")


async def main():
    if not VOYAGE_API_KEY:
        print("ERROR: Set VOYAGE_API_KEY environment variable")
        print("  export VOYAGE_API_KEY='your-key-here'")
        return
    
    print("Connecting to database...")
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Get events without embeddings
    events = await conn.fetch("""
        SELECT id, raw_content 
        FROM events 
        WHERE content_embedding IS NULL 
          AND raw_content IS NOT NULL
        ORDER BY id
    """)
    
    print(f"Found {len(events)} events needing embeddings")
    
    if not events:
        print("All events already have embeddings!")
        await conn.close()
        return
    
    # Process in batches
    for i in range(0, len(events), BATCH_SIZE):
        batch = events[i:i + BATCH_SIZE]
        
        # Prepare texts (truncate to ~8000 chars for API limits)
        texts = [e["raw_content"][:8000] for e in batch]
        
        try:
            print(f"Processing batch {i//BATCH_SIZE + 1}/{(len(events)-1)//BATCH_SIZE + 1}...")
            embeddings = await get_embeddings_batch(texts)
            
            # Update database
            for event, embedding in zip(batch, embeddings):
                embedding_str = f"[{','.join(map(str, embedding))}]"
                await conn.execute("""
                    UPDATE events 
                    SET content_embedding = $1::vector
                    WHERE id = $2
                """, embedding_str, event["id"])
            
            print(f"  ✓ Updated {len(batch)} events")
            
            # Longer delay to avoid rate limits
            await asyncio.sleep(3)
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            continue
    
    # Verify
    remaining = await conn.fetchval("""
        SELECT COUNT(*) FROM events WHERE content_embedding IS NULL
    """)
    total = await conn.fetchval("SELECT COUNT(*) FROM events")
    
    print(f"\nDone! {total - remaining}/{total} events have embeddings")
    
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())