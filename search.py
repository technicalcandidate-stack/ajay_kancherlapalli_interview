"""
Semantic search service using pgvector.
Retrieves relevant events based on query similarity.
"""
import asyncio
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import asyncpg

from embedding import get_query_embedding, EMBEDDING_DIMENSION


@dataclass
class SearchResult:
    event_id: int
    account_id: int
    account_name: str
    event_type: str
    occurred_at: datetime
    raw_content: str
    summary: str
    similarity: float
    direction: Optional[str] = None


class SemanticSearch:
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
    
    async def search_account_events(
        self,
        query: str,
        account_id: int,
        top_k: int = 15,
        min_similarity: float = 0.3,
        event_types: Optional[List[str]] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> List[SearchResult]:
        """Search for relevant events within a specific account."""
        query_embedding = await get_query_embedding(query)
        embedding_str = f"[{','.join(map(str, query_embedding))}]"
        
        conditions = ["e.account_id = $2"]
        params = [embedding_str, account_id]
        param_idx = 3
        
        if event_types:
            conditions.append(f"e.event_type = ANY(${param_idx})")
            params.append(event_types)
            param_idx += 1
        
        if date_from:
            conditions.append(f"e.occurred_at >= ${param_idx}")
            params.append(date_from)
            param_idx += 1
        
        if date_to:
            conditions.append(f"e.occurred_at <= ${param_idx}")
            params.append(date_to)
            param_idx += 1
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            SELECT 
                e.id as event_id,
                e.account_id,
                a.name as account_name,
                e.event_type,
                e.occurred_at,
                e.raw_content,
                e.summary,
                e.direction,
                1 - (e.content_embedding <=> $1::vector) as similarity
            FROM events e
            JOIN accounts a ON e.account_id = a.id
            WHERE {where_clause}
              AND e.content_embedding IS NOT NULL
              AND 1 - (e.content_embedding <=> $1::vector) >= {min_similarity}
            ORDER BY e.content_embedding <=> $1::vector
            LIMIT {top_k}
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        
        return [
            SearchResult(
                event_id=r["event_id"],
                account_id=r["account_id"],
                account_name=r["account_name"],
                event_type=r["event_type"],
                occurred_at=r["occurred_at"],
                raw_content=r["raw_content"],
                summary=r["summary"],
                similarity=r["similarity"],
                direction=r["direction"]
            )
            for r in rows
        ]
    
    async def search_all_accounts(
        self,
        query: str,
        top_k_total: int = 50,
        top_k_per_account: int = 5,
        min_similarity: float = 0.3,
        event_types: Optional[List[str]] = None
    ) -> Dict[int, List[SearchResult]]:
        """Search across all accounts. Returns results grouped by account_id."""
        query_embedding = await get_query_embedding(query)
        embedding_str = f"[{','.join(map(str, query_embedding))}]"
        
        type_filter = ""
        params = [embedding_str, top_k_per_account, min_similarity, top_k_total]
        if event_types:
            type_filter = "AND e.event_type = ANY($5)"
            params.append(event_types)
        
        sql = f"""
            WITH ranked_events AS (
                SELECT 
                    e.id as event_id,
                    e.account_id,
                    a.name as account_name,
                    e.event_type,
                    e.occurred_at,
                    e.raw_content,
                    e.summary,
                    e.direction,
                    1 - (e.content_embedding <=> $1::vector) as similarity,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.account_id 
                        ORDER BY e.content_embedding <=> $1::vector
                    ) as rank_in_account
                FROM events e
                JOIN accounts a ON e.account_id = a.id
                WHERE e.content_embedding IS NOT NULL
                  AND 1 - (e.content_embedding <=> $1::vector) >= $3
                  {type_filter}
            )
            SELECT * FROM ranked_events
            WHERE rank_in_account <= $2
            ORDER BY similarity DESC
            LIMIT $4
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        
        results: Dict[int, List[SearchResult]] = {}
        for r in rows:
            result = SearchResult(
                event_id=r["event_id"],
                account_id=r["account_id"],
                account_name=r["account_name"],
                event_type=r["event_type"],
                occurred_at=r["occurred_at"],
                raw_content=r["raw_content"],
                summary=r["summary"],
                similarity=r["similarity"],
                direction=r["direction"]
            )
            if r["account_id"] not in results:
                results[r["account_id"]] = []
            results[r["account_id"]].append(result)
        
        return results


def format_results_for_llm(results: List[SearchResult], include_metadata: bool = True) -> str:
    """Format search results into context string for LLM. Uses FULL raw content with citation IDs."""
    if not results:
        return "No relevant communications found."
    
    formatted = []
    for i, r in enumerate(results, 1):
        # Citation header with all metadata
        header = f"[SOURCE {i}]"
        metadata = f"Type: {r.event_type} | Date: {r.occurred_at.strftime('%Y-%m-%d %H:%M')} | Account: {r.account_name}"
        if r.direction:
            metadata += f" | Direction: {r.direction}"
        if r.summary:
            metadata += f"\nSummary: {r.summary}"
        
        formatted.append(f"{header}\n{metadata}\n\nFull Content:\n{r.raw_content or r.summary}")
    
    return "\n\n" + "="*50 + "\n\n".join(formatted)


def format_grouped_results_for_llm(grouped_results: Dict[int, List[SearchResult]]) -> str:
    """Format grouped results (by account) for cross-account queries with citation IDs."""
    if not grouped_results:
        return "No relevant communications found across any accounts."
    
    formatted = []
    source_num = 1
    
    for account_id, results in grouped_results.items():
        account_name = results[0].account_name if results else f"Account {account_id}"
        
        account_section = f"## {account_name}\n\n"
        for r in results:
            metadata = f"Type: {r.event_type} | Date: {r.occurred_at.strftime('%Y-%m-%d %H:%M')}"
            if r.direction:
                metadata += f" | Direction: {r.direction}"
            if r.summary:
                metadata += f"\nSummary: {r.summary}"
            
            account_section += f"[SOURCE {source_num}]\n{metadata}\n\nFull Content:\n{r.raw_content or r.summary}\n\n"
            source_num += 1
        
        formatted.append(account_section)
    
    return "\n---\n\n".join(formatted)