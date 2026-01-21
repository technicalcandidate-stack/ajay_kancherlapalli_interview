"""
ingestion.py - Main ingestion pipeline for JSONL data
"""

import json
import asyncio
from datetime import datetime
from typing import Optional
import asyncpg

from models import MemoryDatabase, ExtractionResult
from extractor import ClaudeExtractor, QuoteExtractor

# ============================================================================
# CONFIGURATION
# ============================================================================

DATABASE_URL = "postgresql://localhost/memory_system"

# ============================================================================
# INGESTION PIPELINE
# ============================================================================

class IngestionPipeline:
    """Processes JSONL account data into the memory system."""
    
    def __init__(self, pool: asyncpg.Pool, extract: bool = True):
        self.db = MemoryDatabase(pool)
        self.extractor = ClaudeExtractor() if extract else None
        self.extract = extract
    
    async def process_account(self, data: dict) -> int:
        """Process a single account record from JSONL."""
        
        # 1. Upsert account
        account_id = await self.db.upsert_account(data)
        account_name = data.get('account_name', 'Unknown')
        print(f"  📁 Account: {account_name} (ID: {account_id})")
        
        # Get account context for extraction
        account_context = await self.db.get_account(account_id) or {}
        
        # 2. Process emails
        for email in data.get('emails', []):
            await self._process_email(account_id, email, account_context)
        
        # 3. Process phone calls
        for call in data.get('phone_calls', []):
            await self._process_call(account_id, call, account_context)
        
        # 4. Process SMS (phone_messages)
        for sms in data.get('phone_messages', []):
            await self._process_sms(account_id, sms)
        
        return account_id
    
    async def _process_email(self, account_id: int, email: dict, context: dict):
        """Process a single email."""
        
        # Determine direction and type
        from_addr = email.get('from', {}).get('address', '')
        direction = email.get('direction', 'unknown')
        if direction == 'unknown':
            direction = 'outbound' if 'harperinsure' in from_addr.lower() else 'inbound'
        
        stage_tags = email.get('stage_tags', [])
        event_type = f'email_{direction}'
        if 'QUOTE' in stage_tags and direction == 'inbound':
            event_type = 'quote_received'
        
        # Build event data
        event_data = {
            'external_id': email.get('id'),
            'event_type': event_type,
            'channel': 'email',
            'direction': direction,
            'occurred_at': self._parse_datetime(email.get('activity_start_time')),
            'subject': email.get('subject'),
            'raw_content': email.get('source_body') or email.get('activity_content', ''),
            'from_address': self._parse_email_addr(from_addr),
            'from_name': email.get('from', {}).get('name', ''),
            'to_addresses': [t.get('address', '') for t in email.get('to', [])],
            'stage_tags': stage_tags,
            'source_metadata': email.get('email_metadata', {}),
        }
        
        # Insert event
        event_id = await self.db.insert_event(account_id, event_data)
        if not event_id:
            print(f"    ⏭️  Skipped duplicate email: {email.get('subject', '')[:40]}")
            return
        
        print(f"    📧 Email: {email.get('subject', '')[:50]}...")
        
        # Check for pre-parsed quote in metadata
        quote_data = QuoteExtractor.extract_from_email_metadata(email)
        if quote_data and quote_data.get('premium'):
            await self.db.insert_quote(account_id, event_id, quote_data)
            await self.db.update_account_quote_stats(
                account_id, 
                quote_data['premium'],
                quote_data['carrier_name']
            )
            print(f"       💰 Quote: ${quote_data['premium']} from {quote_data['carrier_name']}")
        
        # LLM extraction (if enabled)
        if self.extract and self.extractor:
            extraction = await self.extractor.extract_from_email(email, context)
            await self.db.update_event_extraction(event_id, extraction)
            await self.db.update_account_from_extraction(account_id, event_id, extraction)
            
            # If quote detected by LLM but not in metadata
            if not quote_data and extraction.amounts.get('premium'):
                quote_data = {
                    'carrier_name': extraction.entities.get('companies', ['Unknown'])[0] if extraction.entities.get('companies') else 'Unknown',
                    'premium': extraction.amounts['premium'],
                    'coverage_types': extraction.entities.get('coverage_types', []),
                }
                await self.db.insert_quote(account_id, event_id, quote_data)
                await self.db.update_account_quote_stats(
                    account_id,
                    quote_data['premium'],
                    quote_data['carrier_name']
                )
    
    async def _process_call(self, account_id: int, call: dict, context: dict):
        """Process a phone call."""
        
        direction = call.get('direction', 'unknown')
        
        event_data = {
            'external_id': call.get('id'),
            'event_type': f'call_{direction}',
            'channel': 'call',
            'direction': direction,
            'occurred_at': self._parse_datetime(call.get('created_at') or call.get('completed_at')),
            'subject': call.get('activity_summary', ''),
            'raw_content': call.get('source_text', ''),
            'duration_seconds': call.get('duration_seconds'),
            'source_metadata': {
                'phone_provider': call.get('phone_provider'),
                'call_source': call.get('call_source'),
            },
        }
        
        event_id = await self.db.insert_event(account_id, event_data)
        if not event_id:
            return
        
        duration = call.get('duration_seconds', 0)
        print(f"    📞 Call: {duration}s {direction}")
        
        # LLM extraction
        if self.extract and self.extractor and call.get('source_text'):
            extraction = await self.extractor.extract_from_call(call, context)
            await self.db.update_event_extraction(event_id, extraction)
            await self.db.update_account_from_extraction(account_id, event_id, extraction)
    
    async def _process_sms(self, account_id: int, sms: dict):
        """Process an SMS message."""
        
        direction = sms.get('direction', 'unknown')
        
        event_data = {
            'external_id': sms.get('id'),
            'event_type': f'sms_{direction}',
            'channel': 'sms',
            'direction': direction,
            'occurred_at': self._parse_datetime(sms.get('created_at')),
            'raw_content': sms.get('source_text', ''),
        }
        
        event_id = await self.db.insert_event(account_id, event_data)
        if event_id:
            content = sms.get('source_text', '')[:40]
            print(f"    💬 SMS: {content}...")
    
    def _parse_datetime(self, dt_str: str) -> datetime:
        if not dt_str:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return datetime.utcnow()
    
    def _parse_email_addr(self, raw: str) -> str:
        """Extract just the email from 'Name <email>' format."""
        if '<' in raw and '>' in raw:
            start = raw.index('<') + 1
            end = raw.index('>')
            return raw[start:end]
        return raw

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def ingest_jsonl(filepath: str, extract: bool = True):
    """Ingest a JSONL file into the memory system."""
    
    pool = await asyncpg.create_pool(DATABASE_URL)
    pipeline = IngestionPipeline(pool, extract=extract)
    
    print(f"{'='*60}")
    print(f"Ingesting: {filepath}")
    print(f"LLM Extraction: {'enabled' if extract else 'disabled'}")
    print(f"{'='*60}")
    
    processed = 0
    errors = 0
    
    with open(filepath, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            
            try:
                data = json.loads(line)
                await pipeline.process_account(data)
                processed += 1
            except json.JSONDecodeError as e:
                print(f"  ❌ Line {line_num}: Invalid JSON - {e}")
                errors += 1
            except Exception as e:
                print(f"  ❌ Line {line_num}: {e}")
                errors += 1
            
            if processed % 10 == 0:
                print(f"  ... processed {processed} accounts")
    
    print(f"{'='*60}")
    print(f"Complete! Processed: {processed}, Errors: {errors}")
    
    await pool.close()

# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import sys
    
    filepath = sys.argv[1] if len(sys.argv) > 1 else "accounts.jsonl"
    extract = "--no-extract" not in sys.argv
    
    asyncio.run(ingest_jsonl(filepath, extract=extract))