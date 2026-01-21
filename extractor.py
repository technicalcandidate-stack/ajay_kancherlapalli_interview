"""
extractor.py - LLM-based extraction using Claude
"""

import json
from typing import Optional
import anthropic
from models import ExtractionResult

CLAUDE_MODEL = "claude-sonnet-4-20250514"

class ClaudeExtractor:
    """Extracts structured information from communications using Claude."""
    
    def __init__(self):
        self.client = anthropic.Anthropic()
    
    async def extract_from_email(self, email: dict, account_context: dict) -> ExtractionResult:
        """Extract structured info from email."""
        
        from_addr = email.get('from', {}).get('address', 'Unknown')
        to_list = [t.get('address', '') for t in email.get('to', [])]
        subject = email.get('subject', 'No subject')
        body = email.get('source_body') or email.get('activity_content', '')
        
        prompt = f"""Analyze this insurance brokerage email and extract structured information.

ACCOUNT CONTEXT:
- Company: {account_context.get('name', 'Unknown')}
- Industry: {account_context.get('industry', 'Unknown')}
- Current Stage: {account_context.get('stage', 'Unknown')}

EMAIL:
From: {from_addr}
To: {to_list}
Subject: {subject}

Content:
{body[:3000]}

---

Extract as JSON:
{{
    "summary": "One sentence summary",
    "stage_signal": "null or one of: intake, application, submission, quote",
    "action_items": ["list of action items requested or implied"],
    "deadlines": [{{"date": "YYYY-MM-DD or null", "description": "what"}}],
    "entities": {{
        "people": ["names mentioned"],
        "companies": ["companies mentioned"],
        "coverage_types": ["GL", "WC", etc.],
        "documents": ["loss runs", "COI", etc.]
    }},
    "amounts": {{
        "premium": null or number,
        "limits": {{"per_occurrence": null, "aggregate": null}},
        "deductible": null
    }},
    "sentiment": "positive/neutral/negative/urgent",
    "significance": "routine/important/critical"
}}

STAGE DEFINITIONS:
- intake: Initial contact, lead qualification, gathering basic info
- application: Application received/in progress, collecting documents
- submission: Submitted to carriers, awaiting response
- quote: Quote received, negotiating, closing

Return ONLY valid JSON."""

        response = self.client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return self._parse_extraction(response.content[0].text)
    
    async def extract_from_call(self, call: dict, account_context: dict) -> ExtractionResult:
        """Extract structured info from phone call transcript."""
        
        transcript = call.get('source_text') or call.get('transcript', '')
        duration = call.get('duration_seconds', 0)
        direction = call.get('direction', 'unknown')
        
        prompt = f"""Analyze this insurance brokerage phone call transcript.

ACCOUNT CONTEXT:
- Company: {account_context.get('name', 'Unknown')}
- Industry: {account_context.get('industry', 'Unknown')}

CALL:
Direction: {direction}
Duration: {duration} seconds

Transcript:
{transcript[:3000]}

---

Extract as JSON (same schema):
{{
    "summary": "One sentence summary",
    "stage_signal": "null or one of: intake, application, submission, quote",
    "action_items": ["list"],
    "deadlines": [{{"date": null, "description": ""}}],
    "entities": {{"people": [], "companies": [], "coverage_types": [], "documents": []}},
    "amounts": {{"premium": null, "limits": {{}}, "deductible": null}},
    "sentiment": "positive/neutral/negative/urgent",
    "significance": "routine/important/critical"
}}

STAGE DEFINITIONS:
- intake: Initial contact, lead qualification, gathering basic info
- application: Application received/in progress, collecting documents
- submission: Submitted to carriers, awaiting response
- quote: Quote received, negotiating, closing

Return ONLY valid JSON."""

        response = self.client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return self._parse_extraction(response.content[0].text)
    
    async def generate_account_summary(self, account: dict, events: list[dict]) -> str:
        """Generate executive summary for account."""
        
        events_text = "\n".join([
            f"- {e.get('occurred_at', 'Unknown')}: {e.get('summary', 'No summary')}"
            for e in events[:15]
        ])
        
        prompt = f"""Write a 2-3 sentence executive summary for this insurance account.

ACCOUNT:
- Name: {account.get('name')}
- Industry: {account.get('industry')}
- Stage: {account.get('stage')}
- Coverage: {account.get('coverage_types')}
- Best Quote: ${account.get('best_quote_premium', 'None')} from {account.get('best_quote_carrier', 'N/A')}
- Pending from Customer: {account.get('pending_from_customer', [])}

RECENT ACTIVITY:
{events_text}

Focus on: current status, blockers, recommended next action."""

        response = self.client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text.strip()
    
    def _parse_extraction(self, text: str) -> ExtractionResult:
        """Parse Claude's JSON response into ExtractionResult."""
        try:
            # Clean up potential markdown
            text = text.strip()
            if text.startswith('```'):
                text = text.split('```')[1]
                if text.startswith('json'):
                    text = text[4:]
            
            data = json.loads(text)
            
            return ExtractionResult(
                summary=data.get('summary', ''),
                stage_signal=data.get('stage_signal') if data.get('stage_signal') != 'null' else None,
                action_items=data.get('action_items', []),
                deadlines=data.get('deadlines', []),
                entities=data.get('entities', {}),
                amounts=data.get('amounts', {}),
                sentiment=data.get('sentiment', 'neutral'),
                significance=data.get('significance', 'routine')
            )
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            return ExtractionResult(
                summary=f"Extraction failed: {str(e)}",
                stage_signal=None,
                action_items=[],
                deadlines=[],
                entities={},
                amounts={},
                sentiment='neutral',
                significance='routine'
            )


class QuoteExtractor:
    """Extracts quote data from email metadata or content."""
    
    @staticmethod
    def extract_from_email_metadata(email: dict) -> Optional[dict]:
        """Extract quote from pre-parsed email_metadata."""
        
        metadata = email.get('email_metadata', {})
        quote_details = metadata.get('quote_details')
        
        if not quote_details:
            return None
        
        carrier = quote_details.get('carrier', {})
        
        return {
            'carrier_name': carrier.get('name', 'Unknown'),
            'carrier_state': carrier.get('state'),
            'quote_number': quote_details.get('quote_number'),
            'premium': quote_details.get('premium_estimate'),
            'valid_until': quote_details.get('valid_until'),
            'coverage_types': quote_details.get('coverage_type', []),
            'subjectivities': quote_details.get('subjectivities', []),
        }
    
    @staticmethod
    def has_quote_signal(email: dict) -> bool:
        """Check if email likely contains a quote."""
        stage_tags = email.get('stage_tags', [])
        subject = email.get('subject', '').lower()
        
        return (
            'QUOTE' in stage_tags or
            'quote' in subject or
            'indication' in subject or
            'premium' in subject
        )