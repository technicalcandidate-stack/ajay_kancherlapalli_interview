"""
models.py - Data models and database operations
"""

import hashlib
import re
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any
import asyncpg

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ExtractionResult:
    summary: str = ""
    stage_signal: Optional[str] = None
    action_items: list[str] = field(default_factory=list)
    deadlines: list[dict] = field(default_factory=list)
    entities: dict = field(default_factory=dict)
    amounts: dict = field(default_factory=dict)
    sentiment: str = "neutral"
    significance: str = "routine"

@dataclass
class ResolutionResult:
    account_id: Optional[int]
    confidence: float
    method: str
    matched_on: str

# ============================================================================
# DATA CLEANING HELPERS
# ============================================================================

def clean_decimal(value: Any) -> Optional[float]:
    """Clean messy decimal values like 'about $1.0M', '$0.00', '≈31.3k'."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        return None
    
    # Convert to string and clean
    s = str(value).strip()
    if not s:
        return None
    
    # Remove currency symbols, commas, spaces
    s = re.sub(r'[$,\s]', '', s)
    
    # Remove words like "about", "around", "approximately"
    s = re.sub(r'^(about|around|approximately|≈|~)', '', s, flags=re.I)
    
    # Handle 'k' for thousands, 'M' for millions
    multiplier = 1
    if s.lower().endswith('k'):
        multiplier = 1000
        s = s[:-1]
    elif s.lower().endswith('m'):
        multiplier = 1000000
        s = s[:-1]
    
    try:
        return float(s) * multiplier
    except ValueError:
        return None

def clean_integer(value: Any) -> Optional[int]:
    """Clean messy integer values like 'Since 2025 (licensed)'."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, dict):
        return None
    
    # Try to extract a number
    s = str(value)
    match = re.search(r'\d+', s)
    if match:
        try:
            return int(match.group())
        except ValueError:
            return None
    return None

def clean_state(value: Any) -> Optional[str]:
    """Clean state field - must be 2 chars or None."""
    if value is None:
        return None
    if isinstance(value, dict):
        return None
    
    s = str(value).strip().upper()
    
    # If it's already 2 chars, use it
    if len(s) == 2:
        return s
    
    # Try to extract state abbreviation from longer strings
    # Common patterns: "California", "CA", "Texas, TX"
    state_abbrevs = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC'
    }
    
    # Check if full state name
    if s in state_abbrevs:
        return state_abbrevs[s]
    
    # Try to find 2-letter code in string
    match = re.search(r'\b([A-Z]{2})\b', s)
    if match and match.group(1) in state_abbrevs.values():
        return match.group(1)
    
    return None

def clean_string(value: Any) -> Optional[str]:
    """Clean string value, converting dicts to JSON strings."""
    if value is None:
        return None
    if isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)

# ============================================================================
# DATABASE CLASS
# ============================================================================

class MemoryDatabase:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
    
    # ════════════════════════════════════════════════════════════════════════
    # ACCOUNT OPERATIONS
    # ════════════════════════════════════════════════════════════════════════
    
    async def upsert_account(self, data: dict) -> int:
        """Create or update account from JSONL data."""
        structured = data.get('structured_data', {})
        address = structured.get('address', {})
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO accounts (
                    external_id, name, description, website,
                    primary_email, primary_phone,
                    industry, sub_industry, naics_code, sic_code,
                    address_city, address_state, address_postal, timezone,
                    annual_revenue_usd, annual_payroll_usd,
                    employee_count_ft, employee_count_pt, years_in_business,
                    coverage_types, source_stage, tivly_entry_date_time,
                    stage
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23
                )
                ON CONFLICT (external_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    primary_email = COALESCE(EXCLUDED.primary_email, accounts.primary_email),
                    updated_at = NOW()
                RETURNING id
            """,
                str(data.get('account_id')),
                data.get('account_name') or structured.get('company_name'),
                clean_string(structured.get('description')),
                clean_string(structured.get('website')),
                clean_string(structured.get('primary_email')),
                clean_string(structured.get('primary_phone')),
                clean_string(structured.get('industry')),
                clean_string(structured.get('sub_industry')),
                clean_string(structured.get('naics_code')),
                clean_string(structured.get('sic_code')),
                clean_string(address.get('city')),
                clean_state(address.get('state')),  # Fixed: now handles long state names
                clean_string(address.get('postal_code')),
                clean_string(structured.get('timezone')),
                clean_decimal(structured.get('annual_revenue_usd')),  # Fixed: handles "about $1M"
                clean_decimal(structured.get('annual_payroll_usd')),  # Fixed: handles "$0.00"
                clean_integer(structured.get('full_time_employees')),
                clean_integer(structured.get('part_time_employees')),
                clean_integer(structured.get('years_in_business')),  # Fixed: handles "Since 2025"
                structured.get('insurance_types', []),
                clean_string(structured.get('general_stage')),
                self._parse_datetime(structured.get('tivly_entry_date_time')),
                self._map_stage(structured.get('general_stage', 'lead'))
            )
            
            account_id = row['id']
            
            # Populate resolution tables
            await self._populate_resolution_tables(conn, account_id, data)
            
            return account_id
    
    async def _populate_resolution_tables(self, conn, account_id: int, data: dict):
        """Populate lookup tables for entity resolution."""
        structured = data.get('structured_data', {})
        
        # Email domain
        email = structured.get('primary_email')
        if email and isinstance(email, str) and '@' in email:
            domain = email.split('@')[1].lower()
            await conn.execute("""
                INSERT INTO account_email_domains (domain, account_id)
                VALUES ($1, $2) ON CONFLICT DO NOTHING
            """, domain, account_id)
        
        # Website domain
        website = structured.get('website')
        if website and isinstance(website, str):
            domain = website.lower().replace('www.', '').replace('http://', '').replace('https://', '').split('/')[0]
            if domain:
                await conn.execute("""
                    INSERT INTO account_email_domains (domain, account_id, confidence)
                    VALUES ($1, $2, 0.8) ON CONFLICT DO NOTHING
                """, domain, account_id)
        
        # Phone
        phone = structured.get('primary_phone')
        if phone and isinstance(phone, str):
            normalized = re.sub(r'[^\d]', '', phone)
            if normalized and len(normalized) >= 7:
                await conn.execute("""
                    INSERT INTO account_phones (normalized_phone, account_id)
                    VALUES ($1, $2) ON CONFLICT DO NOTHING
                """, normalized, account_id)
        
        # Name aliases
        name = data.get('account_name') or structured.get('company_name')
        if name and isinstance(name, str):
            await conn.execute("""
                INSERT INTO account_aliases (account_id, alias_type, alias_value)
                VALUES ($1, 'name', $2) ON CONFLICT DO NOTHING
            """, account_id, name)
            
            # Abbreviated form
            words = name.split()
            if len(words) > 1:
                abbrev = ' '.join(words[:2])
                await conn.execute("""
                    INSERT INTO account_aliases (account_id, alias_type, alias_value, confidence)
                    VALUES ($1, 'abbreviation', $2, 0.7) ON CONFLICT DO NOTHING
                """, account_id, abbrev)
    
    def _map_stage(self, source_stage: str) -> str:
        """Map source stages to normalized stages.
        
        Pipeline stages: intake → application → submission → quote
        """
        if not source_stage:
            return 'intake'
        if isinstance(source_stage, dict):
            return 'intake'
        
        stage_lower = str(source_stage).lower().strip()
        
        # INTAKE: Initial contact, gathering info
        if any(term in stage_lower for term in [
            'lead', 'intake', 'new', 'prospect', 'information_gathering',
            'referral'
        ]):
            return 'intake'
        
        # APPLICATION: App received, collecting documents
        if any(term in stage_lower for term in [
            'application', 'documentation', 'document'
        ]):
            return 'application'
        
        # SUBMISSION: Sent to carriers/underwriters
        if any(term in stage_lower for term in [
            'submission', 'submitted', 'underwriting', 'pending'
        ]):
            return 'submission'
        
        # QUOTE: Quote received, reviewing, binding
        if any(term in stage_lower for term in [
            'quote', 'indication', 'binding', 'bound', 'closing', 
            'policy', 'quoting'
        ]):
            return 'quote'
        
        # Default to intake if unknown
        return 'intake'
    
    def _parse_datetime(self, dt_str: Any) -> Optional[datetime]:
        """Parse datetime string."""
        if not dt_str:
            return None
        if isinstance(dt_str, datetime):
            return dt_str
        if isinstance(dt_str, dict):
            return None
        try:
            return datetime.fromisoformat(str(dt_str).replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return None
    
    # ════════════════════════════════════════════════════════════════════════
    # EVENT OPERATIONS
    # ════════════════════════════════════════════════════════════════════════
    
    async def insert_event(self, account_id: int, event_data: dict) -> Optional[int]:
        """Insert an event, returns event_id or None if duplicate."""
        content = event_data.get('raw_content', '')
        if not isinstance(content, str):
            content = json.dumps(content) if content else ''
        
        content_hash = hashlib.sha256(content.encode()).hexdigest() if content else None
        
        async with self.pool.acquire() as conn:
            # Check duplicate
            if content_hash:
                existing = await conn.fetchval("""
                    SELECT id FROM events 
                    WHERE account_id = $1 AND content_hash = $2
                """, account_id, content_hash)
                if existing:
                    return None
            
            # Clean to_addresses
            to_addrs = event_data.get('to_addresses', [])
            if not isinstance(to_addrs, list):
                to_addrs = [str(to_addrs)] if to_addrs else []
            to_addrs = [str(a) for a in to_addrs]
            
            # Clean stage_tags
            stage_tags = event_data.get('stage_tags', [])
            if not isinstance(stage_tags, list):
                stage_tags = [str(stage_tags)] if stage_tags else []
            
            row = await conn.fetchrow("""
                INSERT INTO events (
                    account_id, external_id, event_type, channel, direction,
                    occurred_at, subject, raw_content, content_hash,
                    from_address, from_name, to_addresses,
                    duration_seconds, stage_tags, source_metadata,
                    extraction_status
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                RETURNING id
            """,
                account_id,
                str(event_data.get('external_id', '')),
                str(event_data.get('event_type', 'unknown')),
                str(event_data.get('channel', 'unknown')),
                str(event_data.get('direction', 'unknown')) if event_data.get('direction') else None,
                event_data.get('occurred_at') or datetime.utcnow(),
                clean_string(event_data.get('subject')),
                content,
                content_hash,
                clean_string(event_data.get('from_address')),
                clean_string(event_data.get('from_name')),
                to_addrs,
                clean_integer(event_data.get('duration_seconds')),
                stage_tags,
                json.dumps(event_data.get('source_metadata', {})),
                'pending'
            )
            return row['id']
    
    async def update_event_extraction(self, event_id: int, extraction: ExtractionResult):
        """Update event with extraction results."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE events SET
                    summary = $2,
                    extracted_stage_signal = $3,
                    extracted_action_items = $4,
                    extracted_sentiment = $5,
                    event_significance = $6,
                    extraction_data = $7,
                    extraction_status = 'complete'
                WHERE id = $1
            """,
                event_id,
                extraction.summary[:1000] if extraction.summary else None,
                extraction.stage_signal,
                extraction.action_items or [],
                extraction.sentiment,
                extraction.significance,
                json.dumps({
                    'entities': extraction.entities,
                    'amounts': extraction.amounts,
                    'deadlines': extraction.deadlines
                })
            )
    
    # ════════════════════════════════════════════════════════════════════════
    # QUOTE OPERATIONS
    # ════════════════════════════════════════════════════════════════════════
    
    async def insert_quote(self, account_id: int, event_id: int, quote_data: dict) -> int:
        """Insert a quote record."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO quotes (
                    account_id, source_event_id,
                    carrier_name, carrier_state, carrier_reference,
                    coverage_types, premium, valid_until,
                    subjectivities, raw_quote_data
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                RETURNING id
            """,
                account_id,
                event_id,
                clean_string(quote_data.get('carrier_name')) or 'Unknown',
                clean_state(quote_data.get('carrier_state')),
                clean_string(quote_data.get('quote_number')),
                quote_data.get('coverage_types', []) if isinstance(quote_data.get('coverage_types'), list) else [],
                clean_decimal(quote_data.get('premium')),
                self._parse_datetime(quote_data.get('valid_until')),
                quote_data.get('subjectivities', []) if isinstance(quote_data.get('subjectivities'), list) else [],
                json.dumps(quote_data)
            )
            return row['id']
    
    # ════════════════════════════════════════════════════════════════════════
    # DERIVED STATE UPDATES
    # ════════════════════════════════════════════════════════════════════════
    
    async def update_account_from_extraction(self, account_id: int, event_id: int, 
                                              extraction: ExtractionResult):
        """Update account derived state based on extraction."""
        async with self.pool.acquire() as conn:
            updates = ["updated_at = NOW()"]
            params = [account_id]
            idx = 2
            
            # Stage change
            if extraction.stage_signal and isinstance(extraction.stage_signal, str):
                updates.append(f"stage = ${idx}")
                params.append(extraction.stage_signal)
                idx += 1
                updates.append("stage_changed_at = NOW()")
                updates.append(f"stage_change_event_id = ${idx}")
                params.append(event_id)
                idx += 1
            
            # Urgency
            if extraction.significance == 'critical' or extraction.sentiment == 'urgent':
                updates.append("is_urgent = TRUE")
                updates.append(f"urgency_reason = ${idx}")
                params.append(str(extraction.summary)[:200] if extraction.summary else 'Critical event')
                idx += 1
            
            # Sentiment
            if extraction.sentiment and isinstance(extraction.sentiment, str):
                updates.append(f"customer_sentiment = ${idx}")
                params.append(extraction.sentiment)
                idx += 1
            
            query = f"UPDATE accounts SET {', '.join(updates)} WHERE id = $1"
            await conn.execute(query, *params)
    
    async def update_account_quote_stats(self, account_id: int, premium: float, carrier: str):
        """Update account quote statistics."""
        if not premium:
            return
            
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE accounts SET
                    has_active_quote = TRUE,
                    quote_count = quote_count + 1,
                    best_quote_premium = CASE 
                        WHEN best_quote_premium IS NULL OR $2 < best_quote_premium 
                        THEN $2 ELSE best_quote_premium 
                    END,
                    best_quote_carrier = CASE 
                        WHEN best_quote_premium IS NULL OR $2 < best_quote_premium 
                        THEN $3 ELSE best_quote_carrier 
                    END,
                    updated_at = NOW()
                WHERE id = $1
            """, account_id, premium, carrier or 'Unknown')
    
    # ════════════════════════════════════════════════════════════════════════
    # QUERY OPERATIONS
    # ════════════════════════════════════════════════════════════════════════
    
    async def get_account(self, account_id: int) -> Optional[dict]:
        """Get account by ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM accounts WHERE id = $1", account_id)
            return dict(row) if row else None
    
    async def get_account_events(self, account_id: int, limit: int = 20) -> list[dict]:
        """Get recent events for an account."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM events 
                WHERE account_id = $1 
                ORDER BY occurred_at DESC 
                LIMIT $2
            """, account_id, limit)
            return [dict(r) for r in rows]
    
    async def get_account_quotes(self, account_id: int) -> list[dict]:
        """Get quotes for an account."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT q.*, e.summary as source_summary
                FROM quotes q
                LEFT JOIN events e ON e.id = q.source_event_id
                WHERE q.account_id = $1
                ORDER BY q.created_at DESC
            """, account_id)
            return [dict(r) for r in rows]
    
    async def get_pending_items(self, account_id: int) -> list[dict]:
        """Get pending items for an account."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM pending_items
                WHERE account_id = $1 AND status = 'pending'
                ORDER BY created_at DESC
            """, account_id)
            return [dict(r) for r in rows]