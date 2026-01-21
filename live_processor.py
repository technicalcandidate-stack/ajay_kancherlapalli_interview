"""
live_processor.py - Process live incoming emails/calls/sms
This handles the account resolution problem for new communications.
"""

import asyncio
from datetime import datetime
from typing import Optional
import asyncpg

from models import MemoryDatabase
from resolver import AccountResolver
from extractor import ClaudeExtractor, QuoteExtractor

DATABASE_URL = "postgresql://user:pass@localhost:5432/memory_system"

# ============================================================================
# LIVE MESSAGE PROCESSOR
# ============================================================================

class LiveMessageProcessor:
    """
    Processes live incoming messages (not from JSONL).
    Handles account resolution since we don't know which account it belongs to.
    """
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.db = MemoryDatabase(pool)
        self.resolver = AccountResolver(pool)
        self.extractor = ClaudeExtractor()
    
    async def process_incoming_email(self, email: dict) -> dict:
        """
        Process a new incoming email.
        
        Args:
            email: Raw email dict with from, to, subject, body, etc.
            
        Returns:
            Result dict with account_id, event_id, resolution info
        """
        
        print(f"📨 Processing email: {email.get('subject', 'No subject')[:50]}")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 1: Resolve to an account
        # ══════════════════════════════════════════════════════════════════
        resolution = await self.resolver.resolve_from_email(email)
        
        if not resolution.account_id:
            print(f"   ⚠️  Could not resolve account")
            print(f"   From: {email.get('from', {}).get('address')}")
            return {
                'success': False,
                'error': 'Could not resolve account',
                'resolution': resolution.__dict__
            }
        
        print(f"   ✓ Resolved to account {resolution.account_id}")
        print(f"     Method: {resolution.method}")
        print(f"     Confidence: {resolution.confidence:.0%}")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 2: Get account context for extraction
        # ══════════════════════════════════════════════════════════════════
        account = await self.db.get_account(resolution.account_id)
        account_context = account or {}
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 3: Determine email properties
        # ══════════════════════════════════════════════════════════════════
        from_addr = self._parse_email_address(email.get('from', {}).get('address', ''))
        direction = 'outbound' if 'harperinsure' in from_addr.lower() else 'inbound'
        
        event_type = f'email_{direction}'
        stage_tags = email.get('stage_tags', [])
        if 'QUOTE' in stage_tags and direction == 'inbound':
            event_type = 'quote_received'
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 4: Insert event
        # ══════════════════════════════════════════════════════════════════
        event_data = {
            'external_id': email.get('id'),
            'event_type': event_type,
            'channel': 'email',
            'direction': direction,
            'occurred_at': self._parse_datetime(email.get('timestamp') or email.get('date')),
            'subject': email.get('subject'),
            'raw_content': email.get('body') or email.get('source_body', ''),
            'from_address': from_addr,
            'from_name': email.get('from', {}).get('name', ''),
            'to_addresses': [self._parse_email_address(t.get('address', '')) for t in email.get('to', [])],
            'stage_tags': stage_tags,
        }
        
        event_id = await self.db.insert_event(resolution.account_id, event_data)
        
        if not event_id:
            return {
                'success': False,
                'error': 'Duplicate email',
                'account_id': resolution.account_id,
            }
        
        print(f"   ✓ Created event {event_id}")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 5: Check for pre-parsed quote
        # ══════════════════════════════════════════════════════════════════
        quote_data = QuoteExtractor.extract_from_email_metadata(email)
        if quote_data and quote_data.get('premium'):
            await self.db.insert_quote(resolution.account_id, event_id, quote_data)
            await self.db.update_account_quote_stats(
                resolution.account_id,
                quote_data['premium'],
                quote_data['carrier_name']
            )
            print(f"   💰 Quote detected: ${quote_data['premium']}")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 6: LLM Extraction
        # ══════════════════════════════════════════════════════════════════
        extraction = await self.extractor.extract_from_email(email, account_context)
        await self.db.update_event_extraction(event_id, extraction)
        await self.db.update_account_from_extraction(resolution.account_id, event_id, extraction)
        
        print(f"   ✓ Extracted: {extraction.summary[:60]}...")
        
        return {
            'success': True,
            'account_id': resolution.account_id,
            'account_name': account_context.get('name'),
            'event_id': event_id,
            'resolution': resolution.__dict__,
            'extraction_summary': extraction.summary,
        }
    
    async def process_incoming_call(self, call: dict) -> dict:
        """Process a new incoming phone call."""
        
        print(f"📞 Processing call: {call.get('duration_seconds', 0)}s")
        
        # Resolve account
        resolution = await self.resolver.resolve_from_call(call)
        
        if not resolution.account_id:
            return {
                'success': False,
                'error': 'Could not resolve account',
            }
        
        print(f"   ✓ Resolved to account {resolution.account_id}")
        
        account = await self.db.get_account(resolution.account_id)
        
        # Insert event
        direction = call.get('direction', 'unknown')
        event_data = {
            'external_id': call.get('id'),
            'event_type': f'call_{direction}',
            'channel': 'call',
            'direction': direction,
            'occurred_at': self._parse_datetime(call.get('timestamp')),
            'raw_content': call.get('transcript', ''),
            'duration_seconds': call.get('duration_seconds'),
        }
        
        event_id = await self.db.insert_event(resolution.account_id, event_data)
        
        if not event_id:
            return {'success': False, 'error': 'Duplicate call'}
        
        # Extract if has transcript
        if call.get('transcript'):
            extraction = await self.extractor.extract_from_call(call, account or {})
            await self.db.update_event_extraction(event_id, extraction)
            await self.db.update_account_from_extraction(resolution.account_id, event_id, extraction)
        
        return {
            'success': True,
            'account_id': resolution.account_id,
            'event_id': event_id,
        }
    
    async def process_incoming_sms(self, sms: dict) -> dict:
        """Process a new incoming SMS."""
        
        print(f"💬 Processing SMS: {sms.get('content', '')[:30]}...")
        
        resolution = await self.resolver.resolve_from_sms(sms)
        
        if not resolution.account_id:
            return {'success': False, 'error': 'Could not resolve account'}
        
        event_data = {
            'external_id': sms.get('id'),
            'event_type': f"sms_{sms.get('direction', 'unknown')}",
            'channel': 'sms',
            'direction': sms.get('direction'),
            'occurred_at': self._parse_datetime(sms.get('timestamp')),
            'raw_content': sms.get('content', ''),
        }
        
        event_id = await self.db.insert_event(resolution.account_id, event_data)
        
        return {
            'success': event_id is not None,
            'account_id': resolution.account_id,
            'event_id': event_id,
        }
    
    def _parse_email_address(self, raw: str) -> str:
        if not raw:
            return ''
        if '<' in raw and '>' in raw:
            return raw[raw.index('<')+1:raw.index('>')]
        return raw
    
    def _parse_datetime(self, dt) -> datetime:
        if not dt:
            return datetime.utcnow()
        if isinstance(dt, datetime):
            return dt
        try:
            return datetime.fromisoformat(str(dt).replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return datetime.utcnow()

# ============================================================================
# WEBHOOK HANDLER EXAMPLE (FastAPI)
# ============================================================================

"""
from fastapi import FastAPI, Request
app = FastAPI()

processor = None

@app.on_event("startup")
async def startup():
    global processor
    pool = await asyncpg.create_pool(DATABASE_URL)
    processor = LiveMessageProcessor(pool)

@app.post("/webhook/email")
async def handle_email(request: Request):
    email = await request.json()
    result = await processor.process_incoming_email(email)
    return result

@app.post("/webhook/call")
async def handle_call(request: Request):
    call = await request.json()
    result = await processor.process_incoming_call(call)
    return result

@app.post("/webhook/sms")
async def handle_sms(request: Request):
    sms = await request.json()
    result = await processor.process_incoming_sms(sms)
    return result
"""

# ============================================================================
# TEST CLI
# ============================================================================

async def test_live_processing():
    """Test with sample data."""
    
    pool = await asyncpg.create_pool(DATABASE_URL)
    processor = LiveMessageProcessor(pool)
    
    # Simulate incoming email
    test_email = {
        'from': {'address': 'maya.alvarez@sunnydayschildcare.com', 'name': 'Maya Alvarez'},
        'to': [{'address': 'jordan.kim@harperinsure.com'}],
        'subject': 'Re: Insurance Quote Follow-up',
        'body': 'Hi Jordan, I uploaded the background check documents. When can we expect the final quote?',
        'timestamp': datetime.utcnow().isoformat(),
    }
    
    print("="*60)
    print("Testing Live Email Processing")
    print("="*60)
    
    result = await processor.process_incoming_email(test_email)
    print(f"\nResult: {result}")
    
    await pool.close()

if __name__ == "__main__":
    asyncio.run(test_live_processing())