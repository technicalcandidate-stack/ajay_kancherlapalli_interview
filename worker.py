"""
worker.py - Background worker for deferred processing
Handles: pending extractions, summary generation, embedding updates
"""

import json
import asyncio
from datetime import datetime, timedelta
import asyncpg
import anthropic

from models import MemoryDatabase, ExtractionResult
from extractor import ClaudeExtractor

DATABASE_URL = "postgresql://user:pass@localhost:5432/memory_system"

# ============================================================================
# EXTRACTION WORKER
# ============================================================================

class ExtractionWorker:
    """Processes events with pending extractions."""
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.db = MemoryDatabase(pool)
        self.extractor = ClaudeExtractor()
    
    async def process_pending(self, batch_size: int = 10) -> int:
        """Process pending extractions, returns count processed."""
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT e.id, e.account_id, e.channel, e.raw_content, e.subject,
                       a.name as account_name, a.industry, a.stage
                FROM events e
                JOIN accounts a ON a.id = e.account_id
                WHERE e.extraction_status = 'pending'
                  AND e.raw_content IS NOT NULL
                  AND LENGTH(e.raw_content) > 10
                ORDER BY e.occurred_at DESC
                LIMIT $1
            """, batch_size)
            
            if not rows:
                return 0
            
            processed = 0
            for row in rows:
                try:
                    await self._extract_event(conn, dict(row))
                    processed += 1
                except Exception as e:
                    print(f"  ❌ Event {row['id']}: {e}")
                    await conn.execute("""
                        UPDATE events SET extraction_status = 'failed' WHERE id = $1
                    """, row['id'])
            
            return processed
    
    async def _extract_event(self, conn, event: dict):
        """Extract a single event."""
        
        context = {
            'name': event.get('account_name'),
            'industry': event.get('industry'),
            'stage': event.get('stage'),
        }
        
        # Build fake email/call dict for extractor
        comm_data = {
            'source_body': event['raw_content'],
            'subject': event.get('subject'),
        }
        
        if event['channel'] == 'email':
            extraction = await self.extractor.extract_from_email(comm_data, context)
        elif event['channel'] == 'call':
            comm_data['source_text'] = event['raw_content']
            extraction = await self.extractor.extract_from_call(comm_data, context)
        else:
            # SMS - minimal extraction
            extraction = ExtractionResult(summary=event['raw_content'][:100])
        
        # Update event
        await self.db.update_event_extraction(event['id'], extraction)
        
        # Update account if significant
        if extraction.stage_signal or extraction.significance == 'critical':
            await self.db.update_account_from_extraction(
                event['account_id'], event['id'], extraction
            )
        
        print(f"  ✓ Event {event['id']}: {extraction.summary[:50]}...")

# ============================================================================
# SUMMARY WORKER
# ============================================================================

class SummaryWorker:
    """Generates/refreshes account summaries."""
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.db = MemoryDatabase(pool)
        self.extractor = ClaudeExtractor()
    
    async def update_stale_summaries(self, max_age_hours: int = 24, batch_size: int = 5) -> int:
        """Update summaries older than max_age_hours."""
        
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, name, industry, stage, coverage_types,
                       best_quote_premium, best_quote_carrier, pending_from_customer
                FROM accounts
                WHERE (summary_generated_at IS NULL OR summary_generated_at < $1)
                  AND stage NOT IN ('closed_won', 'closed_lost')
                ORDER BY last_activity_at DESC NULLS LAST
                LIMIT $2
            """, cutoff, batch_size)
            
            for row in rows:
                await self._update_summary(conn, dict(row))
            
            return len(rows)
    
    async def _update_summary(self, conn, account: dict):
        """Generate new summary for account."""
        
        # Get recent events
        events = await self.db.get_account_events(account['id'], limit=15)
        
        summary = await self.extractor.generate_account_summary(account, events)
        
        await conn.execute("""
            UPDATE accounts SET
                executive_summary = $2,
                summary_generated_at = NOW()
            WHERE id = $1
        """, account['id'], summary)
        
        print(f"  ✓ Summary for {account['name']}: {summary[:60]}...")

# ============================================================================
# PENDING ITEMS RESOLVER
# ============================================================================

class PendingItemsResolver:
    """Detects when pending items have been resolved."""
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
    
    async def check_resolutions(self) -> int:
        """Check if any pending items have been resolved by recent events."""
        
        resolved_count = 0
        
        async with self.pool.acquire() as conn:
            # Get pending items
            pending = await conn.fetch("""
                SELECT pi.id, pi.account_id, pi.normalized_key, pi.description
                FROM pending_items pi
                WHERE pi.status = 'pending'
            """)
            
            for item in pending:
                # Look for events that might resolve this
                # e.g., "loss runs" requested, later email says "received loss runs"
                resolved_event = await conn.fetchrow("""
                    SELECT id, occurred_at, summary
                    FROM events
                    WHERE account_id = $1
                      AND occurred_at > (SELECT requested_at FROM pending_items WHERE id = $2)
                      AND (
                          summary ILIKE '%received%' || $3 || '%'
                          OR summary ILIKE '%uploaded%' || $3 || '%'
                          OR raw_content ILIKE '%attached%' || $3 || '%'
                      )
                    ORDER BY occurred_at ASC
                    LIMIT 1
                """, item['account_id'], item['id'], item['normalized_key'] or '')
                
                if resolved_event:
                    await conn.execute("""
                        UPDATE pending_items SET
                            status = 'received',
                            resolved_event_id = $2,
                            resolved_at = $3
                        WHERE id = $1
                    """, item['id'], resolved_event['id'], resolved_event['occurred_at'])
                    
                    resolved_count += 1
                    print(f"  ✓ Resolved: {item['description'][:40]}...")
            
            # Update account pending arrays
            await conn.execute("""
                UPDATE accounts a SET
                    pending_from_customer = (
                        SELECT COALESCE(array_agg(description), '{}')
                        FROM pending_items
                        WHERE account_id = a.id 
                          AND status = 'pending'
                          AND responsible_party = 'customer'
                    )
            """)
        
        return resolved_count

# ============================================================================
# MAIN WORKER LOOP
# ============================================================================

async def run_worker():
    """Main background worker loop."""
    
    pool = await asyncpg.create_pool(DATABASE_URL)
    
    extraction_worker = ExtractionWorker(pool)
    summary_worker = SummaryWorker(pool)
    pending_resolver = PendingItemsResolver(pool)
    
    print("="*60)
    print("Background Worker Started")
    print("="*60)
    
    iteration = 0
    
    while True:
        try:
            iteration += 1
            print(f"\n[Iteration {iteration}] {datetime.utcnow().strftime('%H:%M:%S')}")
            
            # Process pending extractions (every iteration)
            extracted = await extraction_worker.process_pending(batch_size=10)
            if extracted:
                print(f"  Extracted {extracted} events")
            
            # Check pending items resolution (every iteration)
            resolved = await pending_resolver.check_resolutions()
            if resolved:
                print(f"  Resolved {resolved} pending items")
            
            # Update summaries (every 5 iterations)
            if iteration % 5 == 0:
                updated = await summary_worker.update_stale_summaries(max_age_hours=24, batch_size=5)
                if updated:
                    print(f"  Updated {updated} summaries")
            
            # Sleep
            await asyncio.sleep(10)
            
        except KeyboardInterrupt:
            print("\nShutting down...")
            break
        except Exception as e:
            print(f"  ❌ Error: {e}")
            await asyncio.sleep(30)
    
    await pool.close()

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    asyncio.run(run_worker())