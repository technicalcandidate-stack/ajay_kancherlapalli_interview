"""
resolver.py - Account resolution from emails, calls, SMS
"""

import re
from typing import Optional
import asyncpg
from models import ResolutionResult

class AccountResolver:
    """Resolves incoming communications to accounts."""
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
    
    async def resolve_from_email(self, email: dict) -> ResolutionResult:
        """Resolve account from an email."""
        
        # Extract email addresses
        from_addr = self._parse_email_address(email.get('from', {}).get('address', ''))
        to_addrs = [
            self._parse_email_address(t.get('address', ''))
            for t in email.get('to', [])
        ]
        
        # Get customer emails (not Harper internal)
        customer_emails = [
            addr for addr in [from_addr] + to_addrs
            if addr and 'harperinsure' not in addr.lower()
        ]
        
        subject = email.get('subject', '')
        body = email.get('source_body', '') or email.get('activity_content', '')
        
        # ══════════════════════════════════════════════════════════════════
        # STRATEGY 1: Email Domain Lookup
        # ══════════════════════════════════════════════════════════════════
        for addr in customer_emails:
            domain = self._extract_domain(addr)
            if domain:
                result = await self._lookup_by_domain(domain)
                if result:
                    return ResolutionResult(
                        account_id=result,
                        confidence=0.95,
                        method='email_domain',
                        matched_on=domain
                    )
        
        # ══════════════════════════════════════════════════════════════════
        # STRATEGY 2: Exact Email Match
        # ══════════════════════════════════════════════════════════════════
        for addr in customer_emails:
            result = await self._lookup_by_exact_email(addr)
            if result:
                return ResolutionResult(
                    account_id=result,
                    confidence=0.95,
                    method='exact_email',
                    matched_on=addr
                )
        
        # ══════════════════════════════════════════════════════════════════
        # STRATEGY 3: Phone Number in Body/Signature
        # ══════════════════════════════════════════════════════════════════
        phones = self._extract_phone_numbers(body)
        for phone in phones:
            result = await self._lookup_by_phone(phone)
            if result:
                return ResolutionResult(
                    account_id=result,
                    confidence=0.85,
                    method='phone_number',
                    matched_on=phone
                )
        
        # ══════════════════════════════════════════════════════════════════
        # STRATEGY 4: Company Name in Subject
        # ══════════════════════════════════════════════════════════════════
        result = await self._search_by_name_in_text(subject)
        if result:
            return ResolutionResult(
                account_id=result['id'],
                confidence=result['confidence'],
                method='name_in_subject',
                matched_on=result['matched_name']
            )
        
        # ══════════════════════════════════════════════════════════════════
        # STRATEGY 5: Company Name in Body
        # ══════════════════════════════════════════════════════════════════
        result = await self._search_by_name_in_text(body[:1000])
        if result:
            return ResolutionResult(
                account_id=result['id'],
                confidence=result['confidence'] * 0.8,
                method='name_in_body',
                matched_on=result['matched_name']
            )
        
        # ══════════════════════════════════════════════════════════════════
        # STRATEGY 6: Thread Matching (Re: / Fwd:)
        # ══════════════════════════════════════════════════════════════════
        if subject.lower().startswith(('re:', 'fwd:', 'fw:')):
            clean_subject = re.sub(r'^(re:|fwd?:)\s*', '', subject, flags=re.I)
            result = await self._find_by_prior_subject(clean_subject)
            if result:
                return ResolutionResult(
                    account_id=result,
                    confidence=0.80,
                    method='thread_match',
                    matched_on=clean_subject
                )
        
        # No match
        return ResolutionResult(
            account_id=None,
            confidence=0.0,
            method='no_match',
            matched_on=''
        )
    
    async def resolve_from_call(self, call: dict) -> ResolutionResult:
        """Resolve account from a phone call."""
        
        # Try phone number first
        for field in ['from_number', 'to_number', 'phone']:
            phone = call.get(field, '')
            if phone:
                normalized = re.sub(r'[^\d]', '', phone)
                result = await self._lookup_by_phone(normalized)
                if result:
                    return ResolutionResult(
                        account_id=result,
                        confidence=0.90,
                        method='phone_number',
                        matched_on=phone
                    )
        
        # Try transcript for company name
        transcript = call.get('source_text', '') or call.get('transcript', '')
        if transcript:
            result = await self._search_by_name_in_text(transcript[:500])
            if result:
                return ResolutionResult(
                    account_id=result['id'],
                    confidence=result['confidence'] * 0.7,
                    method='name_in_transcript',
                    matched_on=result['matched_name']
                )
        
        return ResolutionResult(None, 0.0, 'no_match', '')
    
    async def resolve_from_sms(self, sms: dict) -> ResolutionResult:
        """Resolve account from SMS."""
        
        # Phone number is primary signal for SMS
        for field in ['from_number', 'to_number', 'phone']:
            phone = sms.get(field, '')
            if phone:
                normalized = re.sub(r'[^\d]', '', phone)
                result = await self._lookup_by_phone(normalized)
                if result:
                    return ResolutionResult(
                        account_id=result,
                        confidence=0.90,
                        method='phone_number',
                        matched_on=phone
                    )
        
        return ResolutionResult(None, 0.0, 'no_match', '')
    
    async def resolve_from_query(self, query: str, hints: list[str]) -> list[int]:
        """Resolve accounts from a natural language query."""
        
        account_ids = set()
        
        async with self.pool.acquire() as conn:
            for hint in hints:
                # Try exact ID
                if hint.isdigit():
                    exists = await conn.fetchval(
                        "SELECT id FROM accounts WHERE external_id = $1", hint
                    )
                    if exists:
                        account_ids.add(exists)
                        continue
                
                # Try exact name
                row = await conn.fetchrow(
                    "SELECT id FROM accounts WHERE LOWER(name) = LOWER($1)", hint
                )
                if row:
                    account_ids.add(row['id'])
                    continue
                
                # Try email
                if '@' in hint:
                    domain = hint.split('@')[1].lower()
                    row = await conn.fetchrow("""
                        SELECT account_id FROM account_email_domains WHERE domain = $1
                    """, domain)
                    if row:
                        account_ids.add(row['account_id'])
                        continue
                
                # Try fuzzy name match
                rows = await conn.fetch("""
                    SELECT id, similarity(name, $1) as sim
                    FROM accounts WHERE name % $1
                    ORDER BY sim DESC LIMIT 3
                """, hint)
                if rows and rows[0]['sim'] > 0.3:
                    account_ids.add(rows[0]['id'])
                    continue
                
                # Try aliases
                row = await conn.fetchrow("""
                    SELECT account_id FROM account_aliases
                    WHERE alias_value % $1
                    ORDER BY similarity(alias_value, $1) DESC
                    LIMIT 1
                """, hint)
                if row:
                    account_ids.add(row['account_id'])
        
        return list(account_ids)
    
    # ════════════════════════════════════════════════════════════════════════
    # HELPER METHODS
    # ════════════════════════════════════════════════════════════════════════
    
    def _parse_email_address(self, raw: str) -> Optional[str]:
        """Extract email from 'Name <email@domain.com>' format."""
        if not raw:
            return None
        match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', raw)
        return match.group(0).lower() if match else None
    
    def _extract_domain(self, email: str) -> Optional[str]:
        """Extract domain from email."""
        if not email or '@' not in email:
            return None
        return email.split('@')[1].lower()
    
    def _extract_phone_numbers(self, text: str) -> list[str]:
        """Extract phone numbers from text."""
        patterns = [
            r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',
            r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',
            r'\+1\s*\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',
        ]
        phones = []
        for pattern in patterns:
            phones.extend(re.findall(pattern, text))
        return [re.sub(r'[^\d]', '', p) for p in phones]
    
    async def _lookup_by_domain(self, domain: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT account_id FROM account_email_domains WHERE domain = $1",
                domain.lower()
            )
            return row['account_id'] if row else None
    
    async def _lookup_by_exact_email(self, email: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM accounts WHERE LOWER(primary_email) = $1",
                email.lower()
            )
            return row['id'] if row else None
    
    async def _lookup_by_phone(self, phone: str) -> Optional[int]:
        normalized = re.sub(r'[^\d]', '', phone)
        if len(normalized) == 11 and normalized.startswith('1'):
            normalized = normalized[1:]
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT account_id FROM account_phones WHERE normalized_phone = $1",
                normalized
            )
            return row['account_id'] if row else None
    
    async def _search_by_name_in_text(self, text: str) -> Optional[dict]:
        if not text:
            return None
        
        async with self.pool.acquire() as conn:
            # Exact substring match
            rows = await conn.fetch("""
                SELECT id, name, 0.90 as confidence
                FROM accounts
                WHERE $1 ILIKE '%' || name || '%'
                LIMIT 5
            """, text)
            
            if rows:
                best = max(rows, key=lambda r: r['confidence'])
                return {'id': best['id'], 'matched_name': best['name'], 'confidence': best['confidence']}
            
            # Fuzzy alias match
            rows = await conn.fetch("""
                SELECT a.id, a.name, similarity(aa.alias_value, $1) as sim
                FROM accounts a
                JOIN account_aliases aa ON aa.account_id = a.id
                WHERE aa.alias_value % $1
                ORDER BY sim DESC LIMIT 1
            """, text[:100])
            
            if rows and rows[0]['sim'] > 0.3:
                return {'id': rows[0]['id'], 'matched_name': rows[0]['name'], 'confidence': float(rows[0]['sim'])}
            
            return None
    
    async def _find_by_prior_subject(self, subject: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT account_id FROM events
                WHERE channel = 'email' AND subject ILIKE '%' || $1 || '%'
                ORDER BY occurred_at DESC LIMIT 1
            """, subject[:100])
            return row['account_id'] if row else None