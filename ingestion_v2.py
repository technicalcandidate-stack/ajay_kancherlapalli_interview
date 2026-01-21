"""
Updated ingestion pipeline that:
1. Stores full raw content (not just summaries)
2. Generates embeddings for semantic search
3. Still extracts structured data via LLM
"""
import asyncio
import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import asyncpg
from anthropic import Anthropic

from embedding import get_embedding, prepare_event_for_embedding
from resolver import AccountResolver  # Your existing resolver


class EventIngester:
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
        self.resolver = AccountResolver(db_pool)
        self.client = Anthropic()
        
        # Batch embedding settings
        self.embedding_batch_size = 50
        self.pending_embeddings: List[Dict] = []
    
    async def ingest_jsonl(self, filepath: str, skip_llm: bool = False):
        """
        Ingest events from a JSONL file.
        
        Args:
            filepath: Path to JSONL file
            skip_llm: If True, skip LLM extraction (faster, but no summaries)
        """
        print(f"Ingesting from {filepath}...")
        
        events_processed = 0
        events_skipped = 0
        
        with open(filepath, "r") as f:
            for line_num, line in enumerate(f, 1):
                try:
                    record = json.loads(line.strip())
                    
                    # Process the event
                    result = await self.process_event(record, skip_llm=skip_llm)
                    
                    if result:
                        events_processed += 1
                    else:
                        events_skipped += 1
                    
                    # Progress indicator
                    if line_num % 100 == 0:
                        print(f"  Processed {line_num} lines...")
                    
                except json.JSONDecodeError as e:
                    print(f"  Line {line_num}: Invalid JSON - {e}")
                except Exception as e:
                    print(f"  Line {line_num}: Error - {e}")
        
        # Flush any pending embeddings
        await self._flush_embeddings()
        
        print(f"\nComplete: {events_processed} ingested, {events_skipped} skipped")
    
    async def process_event(
        self, 
        record: Dict[str, Any], 
        skip_llm: bool = False
    ) -> Optional[int]:
        """Process a single event record and insert into database."""
        
        event_type = record.get("type", "").lower()
        
        # Extract raw content based on event type
        raw_content = self._extract_raw_content(record, event_type)
        
        # Resolve account
        account_id = await self._resolve_account(record)
        if not account_id:
            return None  # Could not resolve to an account
        
        # Extract structured data via LLM (optional)
        if skip_llm:
            extracted = {
                "summary": raw_content[:200] if raw_content else None,
                "sentiment_score": None,
                "action_items": [],
                "entities": {}
            }
        else:
            extracted = await self._extract_with_llm(record, event_type, raw_content)
        
        # Parse event date
        event_date = self._parse_date(record)
        
        # Insert event
        async with self.pool.acquire() as conn:
            event_id = await conn.fetchval("""
                INSERT INTO events (
                    account_id, event_type, event_date, raw_content,
                    summary, sentiment_score, action_items, 
                    direction, participants, source_data
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING id
            """,
                account_id,
                event_type,
                event_date,
                raw_content,
                extracted.get("summary"),
                extracted.get("sentiment_score"),
                json.dumps(extracted.get("action_items", [])),
                record.get("direction"),
                record.get("participants", []),
                json.dumps(record)  # Store original for reference
            )
        
        # Queue embedding generation
        self.pending_embeddings.append({
            "event_id": event_id,
            "content": raw_content or extracted.get("summary", "")
        })
        
        # Batch process embeddings
        if len(self.pending_embeddings) >= self.embedding_batch_size:
            await self._flush_embeddings()
        
        return event_id
    
    def _extract_raw_content(self, record: Dict, event_type: str) -> Optional[str]:
        """Extract the full raw content from a record."""
        
        if event_type == "email":
            subject = record.get("subject", "")
            body = record.get("body", "")
            # Include headers if available
            from_addr = record.get("from", "")
            to_addr = record.get("to", "")
            
            parts = []
            if from_addr:
                parts.append(f"From: {from_addr}")
            if to_addr:
                parts.append(f"To: {to_addr}")
            if subject:
                parts.append(f"Subject: {subject}")
            if parts:
                parts.append("")  # Blank line before body
            parts.append(body)
            
            return "\n".join(parts)
        
        elif event_type == "phone_call":
            # Prefer transcript over summary
            transcript = record.get("transcript")
            if transcript:
                return f"Phone Call Transcript:\n{transcript}"
            
            summary = record.get("summary", "")
            return f"Phone Call Summary:\n{summary}" if summary else None
        
        elif event_type == "sms":
            content = record.get("content") or record.get("message", "")
            return f"SMS: {content}" if content else None
        
        else:
            # Generic fallback
            return record.get("content") or record.get("body") or record.get("text")
    
    async def _resolve_account(self, record: Dict) -> Optional[int]:
        """Resolve the account for this event."""
        
        # Try email domain first
        email = record.get("from") or record.get("to") or record.get("email")
        if email and "@" in email:
            domain = email.split("@")[1].lower()
            account_id = await self.resolver.resolve_by_domain(domain)
            if account_id:
                return account_id
        
        # Try phone number
        phone = record.get("phone") or record.get("from_phone") or record.get("to_phone")
        if phone:
            account_id = await self.resolver.resolve_by_phone(phone)
            if account_id:
                return account_id
        
        # Try company name
        company = record.get("company") or record.get("company_name")
        if company:
            account_id = await self.resolver.resolve_by_name(company)
            if account_id:
                return account_id
        
        return None
    
    async def _extract_with_llm(
        self, 
        record: Dict, 
        event_type: str, 
        raw_content: str
    ) -> Dict:
        """Extract structured information using Claude."""
        
        prompt = f"""Analyze this {event_type} communication and extract:

1. A one-sentence summary
2. Sentiment score (-1 to 1, where -1 is very negative, 0 is neutral, 1 is very positive)
3. Any action items or follow-ups needed
4. Key entities mentioned (people, companies, policies, amounts)

Communication:
{raw_content}

Respond in JSON format:
{{
    "summary": "...",
    "sentiment_score": 0.0,
    "action_items": ["..."],
    "entities": {{
        "people": [],
        "amounts": [],
        "policies": [],
        "dates": []
    }}
}}"""
        
        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse JSON from response
            text = response.content[0].text
            # Handle markdown code blocks
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]
            
            return json.loads(text.strip())
            
        except Exception as e:
            print(f"LLM extraction error: {e}")
            return {
                "summary": raw_content[:200] if raw_content else None,
                "sentiment_score": None,
                "action_items": [],
                "entities": {}
            }
    
    def _parse_date(self, record: Dict) -> datetime:
        """Parse event date from various formats."""
        date_str = (
            record.get("date") or 
            record.get("timestamp") or 
            record.get("sent_at") or
            record.get("created_at")
        )
        
        if not date_str:
            return datetime.now()
        
        # Try common formats
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%m/%d/%Y %H:%M:%S"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str[:19], fmt)
            except ValueError:
                continue
        
        return datetime.now()
    
    async def _flush_embeddings(self):
        """Generate and store embeddings for pending events."""
        if not self.pending_embeddings:
            return
        
        from embedding import get_embeddings_batch
        
        # Prepare texts
        texts = [e["content"][:8000] for e in self.pending_embeddings]  # Truncate
        
        try:
            embeddings = await get_embeddings_batch(texts)
            
            async with self.pool.acquire() as conn:
                for item, embedding in zip(self.pending_embeddings, embeddings):
                    embedding_str = f"[{','.join(map(str, embedding))}]"
                    await conn.execute("""
                        UPDATE events 
                        SET content_embedding = $1::vector
                        WHERE id = $2
                    """, embedding_str, item["event_id"])
            
            print(f"  Generated {len(embeddings)} embeddings")
            
        except Exception as e:
            print(f"  Embedding error: {e}")
        
        self.pending_embeddings = []


# CLI
async def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python ingestion_v2.py <jsonl_file> [--skip-llm]")
        sys.exit(1)
    
    filepath = sys.argv[1]
    skip_llm = "--skip-llm" in sys.argv
    
    pool = await asyncpg.create_pool(
        os.getenv("DATABASE_URL", "postgresql://localhost/memory_system")
    )
    
    ingester = EventIngester(pool)
    await ingester.ingest_jsonl(filepath, skip_llm=skip_llm)
    
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())