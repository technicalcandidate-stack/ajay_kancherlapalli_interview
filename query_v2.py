"""
Query interface using semantic search.
Retrieves relevant events with full raw content for LLM context.
"""
import asyncio
import os
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
import asyncpg
from anthropic import Anthropic

from search import SemanticSearch, format_results_for_llm, format_grouped_results_for_llm, SearchResult


class MemoryQueryEngine:
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
        self.search = SemanticSearch(db_pool)
        self.client = Anthropic()
    
    async def query(
        self,
        question: str,
        account_id: Optional[int] = None,
        account_name: Optional[str] = None,
        top_k: int = 15
    ) -> str:
        """
        Answer a question using semantic search over events.
        
        Automatically detects:
        1. If query mentions a specific account -> search that account only
        2. If query is an aggregate/list query -> query database directly
        3. Otherwise -> semantic search across all accounts
        """
        # Resolve account if name provided explicitly
        if account_name and not account_id:
            account_id = await self._resolve_account(account_name)
            if not account_id:
                return f"Could not find account matching '{account_name}'"
        
        # Auto-detect account from query if not provided
        if not account_id:
            detected = await self._detect_account_in_query(question)
            if detected:
                account_id, detected_name = detected
                print(f"  [Detected account: {detected_name} (id={account_id})]")
        
        # Check if this is an aggregate/list query
        if not account_id:
            aggregate_result = await self._handle_aggregate_query(question)
            if aggregate_result:
                print("  [Handled as aggregate query]")
                return aggregate_result
        
        if account_id:
            # Single account query
            results = await self.search.search_account_events(
                query=question,
                account_id=account_id,
                top_k=top_k
            )
            
            # Fallback: if no relevant events found, get the 10 most recent
            used_fallback = False
            if not results:
                print("  [No semantic matches - falling back to 10 most recent events]")
                results = await self._get_recent_events(account_id, limit=10)
                used_fallback = True
            
            if not results:
                return f"No communications found for this account."
            
            account_info = await self._get_account_info(account_id)
            context = format_results_for_llm(results)
            
            answer, confidence = await self._generate_answer(
                question=question,
                context=context,
                account_info=account_info,
                is_cross_account=False
            )
            
            # Append source metadata
            sources = self._format_sources(results)
            return f"{answer}\n\n---\n\n**Confidence: {confidence['level']}** ({confidence['score']:.0%})\n{confidence['explanation']}\n\n**Sources Referenced:**\n{sources}"
        else:
            # Cross-account (brokerage-wide) query
            grouped_results = await self.search.search_all_accounts(
                query=question,
                top_k_total=50,
                top_k_per_account=3,
                min_similarity=0.3
            )
            
            if not grouped_results:
                return "No relevant communications found across any accounts."
            
            context = format_grouped_results_for_llm(grouped_results)
            
            # Flatten for source listing
            all_results = []
            for results_list in grouped_results.values():
                all_results.extend(results_list)
            
            answer, confidence = await self._generate_answer(
                question=question,
                context=context,
                account_info=None,
                is_cross_account=True,
                num_accounts=len(grouped_results)
            )
            
            # Append source metadata
            sources = self._format_sources(all_results)
            return f"{answer}\n\n---\n\n**Confidence: {confidence['level']}** ({confidence['score']:.0%})\n{confidence['explanation']}\n\n**Sources Referenced:**\n{sources}"
    
    async def _handle_aggregate_query(self, question: str) -> Optional[str]:
        """
        Handle aggregate/list queries by querying database directly.
        Returns None if not an aggregate query.
        """
        q_lower = question.lower()
        
        # Detect aggregate query patterns
        aggregate_keywords = [
            "which accounts", "list accounts", "show accounts", "what accounts",
            "how many accounts", "accounts in", "accounts that", "accounts with",
            "all accounts", "every account", "breakdown", "summary", "overview",
            "by stage", "by industry", "by state", "pipeline",
            "across the board", "trending", "overall", "brokerage", "all clients",
            "entire book", "portfolio", "sentiment", "how are we doing", "performance"
        ]
        
        is_aggregate = any(p in q_lower for p in aggregate_keywords)
        if not is_aggregate:
            return None
        
        # Get all account data with event stats
        async with self.pool.acquire() as conn:
            accounts = await conn.fetch("""
                SELECT 
                    a.id,
                    a.name,
                    a.stage,
                    a.stage_detail,
                    a.industry,
                    a.address_state,
                    a.primary_email,
                    a.primary_phone,
                    a.annual_revenue_usd,
                    a.employee_count_ft,
                    a.years_in_business,
                    a.coverage_types,
                    COUNT(e.id) as total_events,
                    COUNT(CASE WHEN e.event_type LIKE '%email%' THEN 1 END) as email_count,
                    COUNT(CASE WHEN e.event_type LIKE '%call%' THEN 1 END) as call_count,
                    COUNT(CASE WHEN e.event_type LIKE '%sms%' THEN 1 END) as sms_count,
                    MAX(e.occurred_at) as last_event_at,
                    MIN(e.occurred_at) as first_event_at
                FROM accounts a
                LEFT JOIN events e ON a.id = e.account_id
                GROUP BY a.id
                ORDER BY a.stage, a.name
            """)
            
            # Get stage counts
            stage_counts = await conn.fetch("""
                SELECT stage, COUNT(*) as count
                FROM accounts
                GROUP BY stage
                ORDER BY count DESC
            """)
            
            # Get industry counts
            industry_counts = await conn.fetch("""
                SELECT industry, COUNT(*) as count
                FROM accounts
                WHERE industry IS NOT NULL
                GROUP BY industry
                ORDER BY count DESC
                LIMIT 10
            """)
            
            # Get sentiment breakdown from events
            sentiment_stats = await conn.fetch("""
                SELECT 
                    extracted_sentiment,
                    COUNT(*) as count
                FROM events
                WHERE extracted_sentiment IS NOT NULL
                GROUP BY extracted_sentiment
                ORDER BY count DESC
            """)
            
            # Get recent event trends (last 7 days vs prior 7 days)
            event_trends = await conn.fetch("""
                SELECT 
                    CASE 
                        WHEN occurred_at >= NOW() - INTERVAL '7 days' THEN 'last_7_days'
                        WHEN occurred_at >= NOW() - INTERVAL '14 days' THEN 'prior_7_days'
                        ELSE 'older'
                    END as period,
                    event_type,
                    COUNT(*) as count,
                    COUNT(CASE WHEN extracted_sentiment = 'positive' THEN 1 END) as positive,
                    COUNT(CASE WHEN extracted_sentiment = 'negative' THEN 1 END) as negative,
                    COUNT(CASE WHEN extracted_sentiment = 'neutral' THEN 1 END) as neutral
                FROM events
                WHERE occurred_at >= NOW() - INTERVAL '14 days'
                GROUP BY period, event_type
                ORDER BY period, event_type
            """)
        
        # Build context for LLM
        context = self._format_aggregate_context(accounts, stage_counts, industry_counts, sentiment_stats, event_trends)
        
        # Use LLM to answer the specific question
        system_prompt = """You are a helpful assistant for Harper Insurance brokerage.
You have access to account metadata and event statistics for the entire brokerage.

Answer the user's question based ONLY on the data provided.
Format your response clearly - use tables or lists where appropriate.
Be specific with numbers and names.
If asked for a list, provide the actual account names and relevant details."""

        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=3000,
            system=system_prompt,
            messages=[{
                "role": "user", 
                "content": f"""Here is the brokerage data:

{context}

Question: {question}

Provide a clear, specific answer based on the data above.

After your answer, on a new line, provide a confidence assessment in this exact format:
CONFIDENCE: [HIGH/MEDIUM/LOW] | [Your explanation of why you are or aren't confident the data answers the question]"""
            }]
        )
        
        answer, confidence = self._parse_confidence(response.content[0].text)
        
        return f"{answer}\n\n---\n\n**Confidence: {confidence['level']}** ({confidence['score']:.0%})\n{confidence['explanation']}"
    
    def _format_aggregate_context(self, accounts, stage_counts, industry_counts, sentiment_stats, event_trends) -> str:
        """Format account data into context for LLM."""
        lines = []
        
        # Summary stats
        lines.append("## BROKERAGE SUMMARY")
        lines.append(f"Total accounts: {len(accounts)}")
        lines.append("")
        
        # Stage breakdown
        lines.append("## ACCOUNTS BY STAGE")
        for s in stage_counts:
            lines.append(f"- {s['stage']}: {s['count']} accounts")
        lines.append("")
        
        # Industry breakdown
        lines.append("## TOP INDUSTRIES")
        for ind in industry_counts:
            lines.append(f"- {ind['industry']}: {ind['count']} accounts")
        lines.append("")
        
        # Sentiment breakdown
        lines.append("## SENTIMENT ACROSS ALL EVENTS")
        if sentiment_stats:
            for s in sentiment_stats:
                lines.append(f"- {s['extracted_sentiment'] or 'unknown'}: {s['count']} events")
        else:
            lines.append("- No sentiment data available")
        lines.append("")
        
        # Event trends
        lines.append("## EVENT TRENDS (Last 14 Days)")
        if event_trends:
            last_7 = [e for e in event_trends if e['period'] == 'last_7_days']
            prior_7 = [e for e in event_trends if e['period'] == 'prior_7_days']
            
            lines.append("Last 7 days:")
            for e in last_7:
                lines.append(f"  - {e['event_type']}: {e['count']} total (positive: {e['positive']}, negative: {e['negative']}, neutral: {e['neutral']})")
            
            lines.append("Prior 7 days:")
            for e in prior_7:
                lines.append(f"  - {e['event_type']}: {e['count']} total (positive: {e['positive']}, negative: {e['negative']}, neutral: {e['neutral']})")
        else:
            lines.append("- No recent event data")
        lines.append("")
        
        # Individual account details
        lines.append("## ALL ACCOUNTS")
        lines.append("")
        
        now = datetime.now()
        for acc in accounts:
            # Calculate days since last contact
            if acc['last_event_at']:
                last_event = acc['last_event_at']
                if last_event.tzinfo:
                    from datetime import timezone
                    now_tz = datetime.now(timezone.utc)
                    days_since = (now_tz - last_event).days
                else:
                    days_since = (now - last_event).days
            else:
                days_since = None
            
            lines.append(f"### {acc['name']} (ID: {acc['id']})")
            lines.append(f"Stage: {acc['stage']}" + (f" / {acc['stage_detail']}" if acc['stage_detail'] else ""))
            lines.append(f"Industry: {acc['industry'] or 'N/A'}")
            lines.append(f"State: {acc['address_state'] or 'N/A'}")
            lines.append(f"Revenue: ${acc['annual_revenue_usd']:,.0f}" if acc['annual_revenue_usd'] else "Revenue: N/A")
            lines.append(f"Employees: {acc['employee_count_ft'] or 'N/A'}")
            lines.append(f"Years in business: {acc['years_in_business'] or 'N/A'}")
            lines.append(f"Total events: {acc['total_events']} (emails: {acc['email_count']}, calls: {acc['call_count']}, sms: {acc['sms_count']})")
            lines.append(f"Last contact: {days_since} days ago" if days_since is not None else "Last contact: never")
            lines.append(f"Email: {acc['primary_email'] or 'N/A'}")
            lines.append("")
        
        return "\n".join(lines)
    
    async def _detect_account_in_query(self, query: str) -> Optional[tuple[int, str]]:
        """Detect if the query mentions a specific account name."""
        query_lower = query.lower()
        
        async with self.pool.acquire() as conn:
            # Get all account names
            accounts = await conn.fetch("SELECT id, name FROM accounts")
            
            best_match = None
            best_length = 0
            
            for acc in accounts:
                name_lower = acc["name"].lower()
                # Check if account name appears in query
                if name_lower in query_lower:
                    # Prefer longer matches (more specific)
                    if len(name_lower) > best_length:
                        best_match = (acc["id"], acc["name"])
                        best_length = len(name_lower)
                else:
                    # Try partial match (first two words of account name)
                    name_parts = name_lower.split()
                    if len(name_parts) >= 2:
                        partial = " ".join(name_parts[:2])
                        if partial in query_lower and len(partial) > best_length:
                            best_match = (acc["id"], acc["name"])
                            best_length = len(partial)
            
            return best_match
    
    def _format_sources(self, results) -> str:
        """Format source metadata for display after answer."""
        lines = []
        for i, r in enumerate(results, 1):
            date_str = r.occurred_at.strftime('%Y-%m-%d %H:%M')
            lines.append(f"[SOURCE {i}] Event #{r.event_id} | {r.account_name} | {r.event_type} | {date_str} | Direction: {r.direction or 'N/A'} | Similarity: {r.similarity:.2f}")
            if r.summary:
                lines.append(f"    Summary: {r.summary[:100]}{'...' if len(r.summary) > 100 else ''}")
        return "\n".join(lines)
    
    def _calculate_confidence(self, results, used_fallback: bool = False, is_aggregate: bool = False, data_completeness: float = None) -> dict:
        """
        Calculate confidence score based on:
        - Best similarity score (if even one source is highly relevant, confidence is high)
        - Whether fallback was used
        - Data completeness for aggregates
        """
        if is_aggregate:
            # For aggregate queries, base on data completeness
            score = data_completeness if data_completeness else 0.7
            if score >= 0.8:
                level = "High"
                explanation = "Data is comprehensive across accounts and event history."
            elif score >= 0.5:
                level = "Medium"
                explanation = "Some data gaps exist. Results based on available records."
            else:
                level = "Low"
                explanation = "Limited data available. Results may be incomplete."
            return {"score": score, "level": level, "explanation": explanation}
        
        if not results:
            return {"score": 0.0, "level": "None", "explanation": "No relevant data found."}
        
        if used_fallback:
            return {
                "score": 0.3,
                "level": "Low",
                "explanation": "No semantically relevant events found. Using most recent events as context - answer may not directly address your question."
            }
        
        # Confidence based on BEST match (not average)
        max_similarity = max(r.similarity for r in results)
        num_strong_matches = sum(1 for r in results if r.similarity >= 0.5)
        
        # Score is driven by the best match
        score = max_similarity
        
        # Determine level and explanation
        if max_similarity >= 0.6:
            level = "High"
            explanation = f"Found highly relevant source (best match: {max_similarity:.2f}). {num_strong_matches} source(s) with strong relevance."
        elif max_similarity >= 0.4:
            level = "Medium"
            explanation = f"Found moderately relevant sources (best match: {max_similarity:.2f}). Answer based on best available context."
        else:
            level = "Low"
            explanation = f"No strongly relevant sources found (best match: {max_similarity:.2f}). Answer may require interpretation."
        
        return {"score": score, "level": level, "explanation": explanation}
    
    async def _get_recent_events(self, account_id: int, limit: int = 10):
        """Get the most recent events for an account (fallback when no semantic matches)."""
        from search import SearchResult
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    e.id as event_id,
                    e.account_id,
                    a.name as account_name,
                    e.event_type,
                    e.occurred_at,
                    e.raw_content,
                    e.summary,
                    e.direction
                FROM events e
                JOIN accounts a ON e.account_id = a.id
                WHERE e.account_id = $1
                ORDER BY e.occurred_at DESC
                LIMIT $2
            """, account_id, limit)
        
        return [
            SearchResult(
                event_id=r["event_id"],
                account_id=r["account_id"],
                account_name=r["account_name"],
                event_type=r["event_type"],
                occurred_at=r["occurred_at"],
                raw_content=r["raw_content"],
                summary=r["summary"],
                similarity=0.0,  # No similarity score for fallback
                direction=r["direction"]
            )
            for r in rows
        ]
    
    async def _resolve_account(self, name: str) -> Optional[int]:
        """Resolve account name to ID."""
        async with self.pool.acquire() as conn:
            # Try exact match first
            row = await conn.fetchrow(
                "SELECT id FROM accounts WHERE LOWER(name) = LOWER($1)",
                name
            )
            if row:
                return row["id"]
            
            # Try partial match
            row = await conn.fetchrow(
                "SELECT id FROM accounts WHERE LOWER(name) LIKE LOWER($1) LIMIT 1",
                f"%{name}%"
            )
            if row:
                return row["id"]
        
        return None
    
    async def _get_account_info(self, account_id: int) -> Dict[str, Any]:
        """Get account summary info for context."""
        async with self.pool.acquire() as conn:
            account = await conn.fetchrow(
                "SELECT name, stage, industry, address_state FROM accounts WHERE id = $1",
                account_id
            )
            
            event_counts = await conn.fetch("""
                SELECT event_type, COUNT(*) as count
                FROM events WHERE account_id = $1
                GROUP BY event_type
            """, account_id)
            
            return {
                "name": account["name"] if account else "Unknown",
                "stage": account["stage"] if account else "Unknown",
                "industry": account["industry"] if account else None,
                "state": account["address_state"] if account else None,
                "event_counts": {r["event_type"]: r["count"] for r in event_counts}
            }
    
    async def _generate_answer(
        self,
        question: str,
        context: str,
        account_info: Optional[Dict],
        is_cross_account: bool,
        num_accounts: int = 0
    ) -> tuple[str, dict]:
        """Generate answer using Claude with retrieved context. Returns (answer, confidence)."""
        
        if is_cross_account:
            system_prompt = f"""You are a helpful assistant for Harper Insurance brokerage.
You have access to communications from {num_accounts} client accounts that are relevant to the user's question.

IMPORTANT: 
- Answer based ONLY on the information provided in the communications below.
- ALWAYS cite your sources using [SOURCE X] when referencing specific information.
- Be specific about which clients/accounts you're referring to.
- If the information isn't available, say so clearly."""
        else:
            account_context = ""
            if account_info:
                account_context = f"\nAccount: {account_info['name']}\nStage: {account_info['stage']}"
            system_prompt = f"""You are a helpful assistant for Harper Insurance brokerage.
You have access to communications for a specific client account.{account_context}

IMPORTANT:
- Answer based ONLY on the information provided in the communications below.
- ALWAYS cite your sources using [SOURCE X] when referencing specific information.
- Be specific and reference dates, email subjects, or call details when relevant.
- If the information isn't available, say so clearly."""
        
        user_prompt = f"""Based on these communications:

{context}

Question: {question}

Provide a clear, specific answer based on the communications above.

After your answer, on a new line, provide a confidence assessment in this exact format:
CONFIDENCE: [HIGH/MEDIUM/LOW] | [Your explanation of why you are or aren't confident the context answers the question]"""
        
        response = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )
        
        # Parse response and confidence
        full_response = response.content[0].text
        answer, confidence = self._parse_confidence(full_response)
        
        return answer, confidence
    
    def _parse_confidence(self, response: str) -> tuple[str, dict]:
        """Parse the answer and confidence from LLM response."""
        if "CONFIDENCE:" in response:
            parts = response.rsplit("CONFIDENCE:", 1)
            answer = parts[0].strip()
            confidence_str = parts[1].strip()
            
            # Parse level and explanation
            if "|" in confidence_str:
                level_part, explanation = confidence_str.split("|", 1)
                level = level_part.strip().upper()
                explanation = explanation.strip()
            else:
                level = confidence_str.strip().upper()
                explanation = ""
            
            # Normalize level
            if "HIGH" in level:
                score = 0.85
                level = "High"
            elif "MEDIUM" in level:
                score = 0.55
                level = "Medium"
            else:
                score = 0.25
                level = "Low"
            
            return answer, {"score": score, "level": level, "explanation": explanation}
        else:
            # No confidence found, default to medium
            return response, {"score": 0.5, "level": "Medium", "explanation": "Confidence not assessed."}


async def main():
    pool = await asyncpg.create_pool(
        os.getenv("DATABASE_URL", "postgresql://localhost/memory_system")
    )
    
    engine = MemoryQueryEngine(pool)
    
    print("Memory Query Engine (v2 with Semantic Search)")
    print("=" * 50)
    print("Commands:")
    print("  <question> - Ask about all accounts")
    print("  @<account> <question> - Ask about specific account")
    print("  quit - Exit")
    print("=" * 50)
    
    while True:
        try:
            query = input("\nQuery> ").strip()
            
            if not query:
                continue
            
            if query.lower() == "quit":
                break
            
            if query.startswith("@"):
                parts = query[1:].split(" ", 1)
                if len(parts) < 2:
                    print("Usage: @<account_name> <question>")
                    continue
                account_name, question = parts
                answer = await engine.query(question, account_name=account_name)
            else:
                answer = await engine.query(query)
            
            print(f"\n{answer}")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
    
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())