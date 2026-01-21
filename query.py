"""
query.py - Natural language query interface for the memory system
"""

import json
import asyncio
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
import asyncpg
import anthropic

from models import MemoryDatabase
from resolver import AccountResolver

DATABASE_URL = "postgresql://localhost:5432/memory_system"
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class QueryAnalysis:
    query_type: str  # single_account, multi_account, brokerage_level
    account_hints: list[str]
    time_range: Optional[tuple]
    info_needed: list[str]

@dataclass
class Answer:
    text: str
    cited_event_ids: list[int]
    cited_sources: list[dict]
    accounts_referenced: list[int]
    confidence: float

# ============================================================================
# QUERY ANALYZER
# ============================================================================

class QueryAnalyzer:
    """Analyzes natural language queries to determine type and extract hints."""
    
    def __init__(self):
        self.client = anthropic.Anthropic()
    
    async def analyze(self, query: str) -> QueryAnalysis:
        """Analyze a query to understand what's being asked."""
        
        prompt = f"""Analyze this query to an insurance brokerage memory system.

QUERY: "{query}"

Determine:
1. query_type: Is this about a "single_account", "multi_account", or "brokerage_level" (all accounts)?
2. account_hints: Extract any company names, person names, emails, or identifiers mentioned
3. time_range: If the query mentions a time period, extract start/end dates (YYYY-MM-DD format)
4. info_needed: What information is needed? Options: status, history, quotes, pending_items, timeline, contacts, summary

Return as JSON:
{{
    "query_type": "single_account",
    "account_hints": ["Sunny Days Childcare"],
    "time_range": null,
    "info_needed": ["status", "quotes"]
}}

Return ONLY valid JSON, no other text."""

        response = self.client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        try:
            # Clean potential markdown
            text = response.content[0].text.strip()
            if text.startswith('```'):
                text = text.split('```')[1]
                if text.startswith('json'):
                    text = text[4:]
            
            data = json.loads(text)
            return QueryAnalysis(
                query_type=data.get('query_type', 'single_account'),
                account_hints=data.get('account_hints', []),
                time_range=data.get('time_range'),
                info_needed=data.get('info_needed', ['status'])
            )
        except (json.JSONDecodeError, KeyError):
            # Fallback: extract hints manually
            return QueryAnalysis(
                query_type='single_account',
                account_hints=[],
                time_range=None,
                info_needed=['status']
            )

# ============================================================================
# CONTEXT RETRIEVER
# ============================================================================

class ContextRetriever:
    """Retrieves relevant context from the database for answering queries."""
    
    def __init__(self, pool: asyncpg.Pool):
        self.db = MemoryDatabase(pool)
        self.pool = pool
    
    async def retrieve(self, account_ids: list[int], analysis: QueryAnalysis) -> dict:
        """Retrieve context for answering the query."""
        
        context = {
            'account_states': [],
            'recent_events': [],
            'quotes': [],
            'pending_items': [],
        }
        
        async with self.pool.acquire() as conn:
            
            # ══════════════════════════════════════════════════════════════
            # BROKERAGE-LEVEL QUERIES (search across all accounts)
            # ══════════════════════════════════════════════════════════════
            if analysis.query_type == 'brokerage_level' or not account_ids:
                
                # Determine what we're searching for based on the query
                search_terms = self._extract_search_terms(analysis)
                
                # Search events by sentiment/content
                if 'pricing' in str(analysis.info_needed) or 'worried' in str(search_terms):
                    # Find accounts with negative sentiment or pricing concerns
                    rows = await conn.fetch("""
                        SELECT DISTINCT ON (a.id)
                            a.id, a.name, a.stage, a.industry, a.address_state,
                            a.coverage_types, a.best_quote_premium, a.best_quote_carrier,
                            a.customer_sentiment, a.pending_from_customer,
                            a.executive_summary,
                            EXTRACT(DAY FROM NOW() - COALESCE(a.stage_changed_at, a.created_at))::INTEGER as days_in_current_stage
                        FROM accounts a
                        JOIN events e ON e.account_id = a.id
                        WHERE 
                            a.customer_sentiment IN ('negative', 'frustrated', 'concerned')
                            OR e.extracted_sentiment IN ('negative', 'urgent')
                            OR e.summary ILIKE '%price%'
                            OR e.summary ILIKE '%cost%'
                            OR e.summary ILIKE '%expensive%'
                            OR e.summary ILIKE '%afford%'
                            OR e.summary ILIKE '%budget%'
                            OR e.summary ILIKE '%worried%'
                            OR e.summary ILIKE '%concern%'
                        ORDER BY a.id, e.occurred_at DESC
                        LIMIT 20
                    """)
                    context['account_states'] = [dict(r) for r in rows]
                    
                    # Get relevant events for these accounts
                    if context['account_states']:
                        acct_ids = [a['id'] for a in context['account_states']]
                        event_rows = await conn.fetch("""
                            SELECT 
                                id, account_id, event_type, channel, direction,
                                occurred_at, subject, summary,
                                extracted_sentiment, extraction_status
                            FROM events
                            WHERE account_id = ANY($1)
                              AND (
                                  extracted_sentiment IN ('negative', 'urgent')
                                  OR summary ILIKE '%price%'
                                  OR summary ILIKE '%cost%'
                                  OR summary ILIKE '%expensive%'
                                  OR summary ILIKE '%worried%'
                              )
                            ORDER BY occurred_at DESC
                            LIMIT 30
                        """, acct_ids)
                        context['recent_events'] = [dict(r) for r in event_rows]
                
                # Generic brokerage-level: get overview
                else:
                    # Get accounts with recent activity
                    rows = await conn.fetch("""
                        SELECT 
                            id, name, stage, industry, address_state,
                            coverage_types, best_quote_premium, best_quote_carrier,
                            customer_sentiment, pending_from_customer,
                            last_activity_at, executive_summary,
                            EXTRACT(DAY FROM NOW() - COALESCE(stage_changed_at, created_at))::INTEGER as days_in_current_stage
                        FROM accounts
                        ORDER BY last_activity_at DESC NULLS LAST
                        LIMIT 20
                    """)
                    context['account_states'] = [dict(r) for r in rows]
                
                return context
            
            # ══════════════════════════════════════════════════════════════
            # ACCOUNT-SPECIFIC QUERIES (existing logic)
            # ══════════════════════════════════════════════════════════════
            
            # Get account states
            for aid in account_ids:
                row = await conn.fetchrow("""
                    SELECT 
                        id, external_id, name, stage, stage_detail,
                        industry, sub_industry, address_state,
                        coverage_types, primary_email, primary_phone,
                        best_quote_premium, best_quote_carrier, quote_count,
                        has_active_quote, pending_from_customer, pending_from_carrier,
                        next_action, next_action_date,
                        is_urgent, urgency_reason, customer_sentiment,
                        last_activity_at, executive_summary,
                        created_at, stage_changed_at,
                        EXTRACT(DAY FROM NOW() - COALESCE(stage_changed_at, created_at))::INTEGER as days_in_current_stage
                    FROM accounts WHERE id = $1
                """, aid)
                if row:
                    context['account_states'].append(dict(row))
            
            # Get recent events
            for aid in account_ids:
                rows = await conn.fetch("""
                    SELECT 
                        id, account_id, event_type, channel, direction,
                        occurred_at, subject, summary, raw_content,
                        extracted_action_items, extracted_sentiment,
                        extraction_status
                    FROM events 
                    WHERE account_id = $1 
                    ORDER BY occurred_at DESC 
                    LIMIT 15
                """, aid)
                context['recent_events'].extend([dict(r) for r in rows])
            
            # Get quotes if needed
            if 'quotes' in analysis.info_needed or 'status' in analysis.info_needed:
                for aid in account_ids:
                    rows = await conn.fetch("""
                        SELECT 
                            q.id, q.account_id, q.carrier_name, q.carrier_state,
                            q.carrier_reference, q.coverage_types, q.premium,
                            q.valid_until, q.status, q.subjectivities,
                            q.source_event_id, q.created_at,
                            e.summary as source_event_summary
                        FROM quotes q
                        LEFT JOIN events e ON e.id = q.source_event_id
                        WHERE q.account_id = $1
                        ORDER BY q.created_at DESC
                    """, aid)
                    context['quotes'].extend([dict(r) for r in rows])
            
            # Get pending items if needed
            if 'pending_items' in analysis.info_needed or 'status' in analysis.info_needed:
                for aid in account_ids:
                    rows = await conn.fetch("""
                        SELECT 
                            id, account_id, item_type, description,
                            responsible_party, status, requested_at,
                            requested_event_id, resolved_at
                        FROM pending_items
                        WHERE account_id = $1
                        ORDER BY created_at DESC
                    """, aid)
                    context['pending_items'].extend([dict(r) for r in rows])
        
        return context
    
    def _extract_search_terms(self, analysis: QueryAnalysis) -> list[str]:
        """Extract search terms from the analysis."""
        terms = []
        terms.extend(analysis.account_hints)
        terms.extend(analysis.info_needed)
        return terms

# ============================================================================
# ANSWER GENERATOR
# ============================================================================

class AnswerGenerator:
    """Generates natural language answers with citations from context."""
    
    def __init__(self):
        self.client = anthropic.Anthropic()
    
    async def generate(self, query: str, context: dict) -> Answer:
        """Generate an answer to the query using the provided context."""
        
        context_text = self._format_context(context)
        
        prompt = f"""You are an AI assistant for an insurance brokerage. Answer the user's question using ONLY the provided context.

IMPORTANT RULES:
1. Only use information from the provided context
2. Cite specific events by their ID when making claims, e.g., [Event #123]
3. If the context doesn't contain enough information to answer, say so
4. Be concise but complete
5. Format your answer clearly

CONTEXT:
{context_text}

USER QUESTION: {query}

Provide your answer. After your answer, on a new line, list the event IDs you cited in this exact format:
CITED: [1, 2, 3]

If you didn't cite any events, write:
CITED: []"""

        response = self.client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        answer_text = response.content[0].text
        
        # Extract citations
        cited_ids = []
        if "CITED:" in answer_text:
            parts = answer_text.split("CITED:")
            answer_text = parts[0].strip()
            try:
                cited_str = parts[1].strip()
                cited_ids = json.loads(cited_str)
                if not isinstance(cited_ids, list):
                    cited_ids = []
            except (json.JSONDecodeError, IndexError):
                cited_ids = []
        
        # Build cited sources for display
        cited_sources = []
        for event in context.get('recent_events', []):
            if event.get('id') in cited_ids:
                cited_sources.append({
                    'event_id': event['id'],
                    'type': event.get('event_type'),
                    'date': str(event.get('occurred_at')),
                    'summary': event.get('summary') or event.get('subject') or 'No summary',
                })
        
        return Answer(
            text=answer_text,
            cited_event_ids=cited_ids,
            cited_sources=cited_sources,
            accounts_referenced=[a.get('id') for a in context.get('account_states', [])],
            confidence=0.85 if cited_ids else 0.5
        )
    
    def _format_context(self, context: dict) -> str:
        """Format context into a readable string for the LLM."""
        
        sections = []
        
        # Account states
        for acc in context.get('account_states', []):
            pending_customer = acc.get('pending_from_customer') or []
            pending_carrier = acc.get('pending_from_carrier') or []
            
            sections.append(f"""
════════════════════════════════════════════════════════════
ACCOUNT: {acc.get('name')} (ID: {acc.get('id')})
════════════════════════════════════════════════════════════
Stage: {acc.get('stage')} ({acc.get('stage_detail') or 'N/A'})
Days in Stage: {acc.get('days_in_current_stage', 'Unknown')}
Industry: {acc.get('industry', 'Unknown')}
Location: {acc.get('address_state', 'Unknown')}
Coverage Types: {acc.get('coverage_types', [])}

Contact: {acc.get('primary_email', 'N/A')} | {acc.get('primary_phone', 'N/A')}

Quote Status:
  Has Active Quote: {acc.get('has_active_quote', False)}
  Best Quote: ${acc.get('best_quote_premium') or 'None'} from {acc.get('best_quote_carrier') or 'N/A'}
  Total Quotes: {acc.get('quote_count', 0)}

Pending Items:
  From Customer: {pending_customer if pending_customer else 'None'}
  From Carrier: {pending_carrier if pending_carrier else 'None'}

Next Action: {acc.get('next_action') or 'None'} (Due: {acc.get('next_action_date') or 'N/A'})
Urgent: {acc.get('is_urgent', False)} - {acc.get('urgency_reason') or ''}
Customer Sentiment: {acc.get('customer_sentiment', 'Unknown')}
Last Activity: {acc.get('last_activity_at', 'Unknown')}

Summary: {acc.get('executive_summary') or 'No summary available'}
""")
        
        # Recent events
        if context.get('recent_events'):
            sections.append("\n════════════════════════════════════════════════════════════")
            sections.append("RECENT TIMELINE")
            sections.append("════════════════════════════════════════════════════════════")
            
            for evt in context['recent_events']:
                occurred = evt.get('occurred_at')
                if occurred:
                    occurred = occurred.strftime('%Y-%m-%d %H:%M') if hasattr(occurred, 'strftime') else str(occurred)[:16]
                
                sections.append(f"""
[Event #{evt.get('id')}] {occurred}
  Type: {evt.get('event_type')} ({evt.get('channel')}, {evt.get('direction')})
  Subject: {evt.get('subject') or 'N/A'}
  Summary: {evt.get('summary') or 'No summary yet'}
""")
        
        # Quotes
        if context.get('quotes'):
            sections.append("\n════════════════════════════════════════════════════════════")
            sections.append("QUOTES")
            sections.append("════════════════════════════════════════════════════════════")
            
            for q in context['quotes']:
                sections.append(f"""
Carrier: {q.get('carrier_name')}
  Premium: ${q.get('premium') or 'Unknown'}
  Status: {q.get('status')}
  Reference: {q.get('carrier_reference') or 'N/A'}
  Valid Until: {q.get('valid_until') or 'N/A'}
  Subjectivities: {q.get('subjectivities') or 'None'}
  Source: [Event #{q.get('source_event_id')}]
""")
        
        # Pending items
        if context.get('pending_items'):
            sections.append("\n════════════════════════════════════════════════════════════")
            sections.append("PENDING ITEMS")
            sections.append("════════════════════════════════════════════════════════════")
            
            for item in context['pending_items']:
                sections.append(f"""
- {item.get('description')}
  Type: {item.get('item_type')} | Responsible: {item.get('responsible_party')}
  Status: {item.get('status')} | Requested: {item.get('requested_at') or 'Unknown'}
""")
        
        return "\n".join(sections)

# ============================================================================
# MAIN QUERY INTERFACE
# ============================================================================

class MemoryQueryInterface:
    """Main interface for querying the memory system."""
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.analyzer = QueryAnalyzer()
        self.resolver = AccountResolver(pool)
        self.retriever = ContextRetriever(pool)
        self.generator = AnswerGenerator()
    
    async def query(self, question: str) -> Answer:
        """Answer a natural language question about accounts."""
        
        print(f"\n{'─'*60}")
        print(f"🔍 Analyzing query...")
        
        # 1. Analyze the query
        analysis = await self.analyzer.analyze(question)
        print(f"   Type: {analysis.query_type}")
        print(f"   Hints: {analysis.account_hints}")
        print(f"   Info needed: {analysis.info_needed}")
        
        # 2. Resolve accounts from hints
        account_ids = []
        if analysis.account_hints:
            account_ids = await self.resolver.resolve_from_query(question, analysis.account_hints)
            print(f"   Resolved account IDs: {account_ids}")
        
        if not account_ids and analysis.query_type == 'single_account':
            # Try to find account from the query itself
            account_ids = await self.resolver.resolve_from_query(question, [question])
            if account_ids:
                print(f"   Found via query text: {account_ids}")
        
        # 3. Retrieve context
        print(f"📚 Retrieving context...")
        context = await self.retriever.retrieve(account_ids, analysis)
        print(f"   Accounts: {len(context['account_states'])}")
        print(f"   Events: {len(context['recent_events'])}")
        print(f"   Quotes: {len(context['quotes'])}")
        
        # 4. Generate answer
        print(f"💭 Generating answer...")
        answer = await self.generator.generate(question, context)
        
        # 5. Log the query
        await self._log_query(question, analysis, account_ids, answer)
        
        return answer
    
    async def _log_query(self, question: str, analysis: QueryAnalysis,
                         account_ids: list[int], answer: Answer):
        """Log query for observability."""
        
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO query_log (
                        query_text, query_type, resolved_account_ids,
                        events_retrieved, response_text, sources_cited
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    question,
                    analysis.query_type,
                    account_ids,
                    answer.cited_event_ids,
                    answer.text[:5000],  # Truncate if too long
                    answer.cited_event_ids
                )
        except Exception as e:
            print(f"   ⚠️ Failed to log query: {e}")

# ============================================================================
# INTERACTIVE CLI
# ============================================================================

async def interactive():
    """Run an interactive query session."""
    
    print("="*60)
    print("  MEMORY SYSTEM QUERY INTERFACE")
    print("="*60)
    print("Connecting to database...")
    
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print(f"   Check DATABASE_URL: {DATABASE_URL}")
        return
    
    interface = MemoryQueryInterface(pool)
    
    print("✓ Connected!")
    print("\nAsk questions about your insurance accounts.")
    print("Type 'quit' or 'exit' to stop.\n")
    
    while True:
        try:
            question = input("📝 Your question: ").strip()
            
            if question.lower() in ('quit', 'exit', 'q'):
                break
            
            if not question:
                continue
            
            answer = await interface.query(question)
            
            print("\n" + "="*60)
            print("📋 ANSWER:")
            print("="*60)
            print(answer.text)
            
            if answer.cited_sources:
                print("\n📚 SOURCES CITED:")
                for src in answer.cited_sources:
                    summary = src.get('summary', 'No summary')
                    if len(summary) > 70:
                        summary = summary[:70] + "..."
                    print(f"   • [Event #{src['event_id']}] {src['date'][:10]}: {summary}")
            
            print(f"\n🎯 Confidence: {answer.confidence:.0%}")
            print(f"📊 Accounts: {answer.accounts_referenced}")
            
        except KeyboardInterrupt:
            print("\n")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    await pool.close()
    print("\n👋 Goodbye!")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    asyncio.run(interactive())