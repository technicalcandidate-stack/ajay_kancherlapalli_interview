"""
Embedding service for semantic search over events.
Supports multiple embedding providers.
"""
import os
import asyncio
from typing import List, Optional
import httpx
from anthropic import Anthropic

# Choose your embedding provider
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "voyage")  # or "openai"
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "voyage-3")  # or "text-embedding-3-small"
EMBEDDING_DIMENSION = 1024  # voyage-3 = 1024, openai = 1536


async def get_embedding(text: str) -> List[float]:
    """Generate embedding for a single text."""
    if EMBEDDING_PROVIDER == "voyage":
        return await _voyage_embedding(text)
    elif EMBEDDING_PROVIDER == "openai":
        return await _openai_embedding(text)
    else:
        raise ValueError(f"Unknown embedding provider: {EMBEDDING_PROVIDER}")


async def get_embeddings_batch(texts: List[str], batch_size: int = 50) -> List[List[float]]:
    """Generate embeddings for multiple texts efficiently."""
    all_embeddings = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        if EMBEDDING_PROVIDER == "voyage":
            embeddings = await _voyage_embeddings_batch(batch)
        elif EMBEDDING_PROVIDER == "openai":
            embeddings = await _openai_embeddings_batch(batch)
        else:
            raise ValueError(f"Unknown provider: {EMBEDDING_PROVIDER}")
        all_embeddings.extend(embeddings)
        
        # Rate limiting
        if i + batch_size < len(texts):
            await asyncio.sleep(0.1)
    
    return all_embeddings


async def _voyage_embedding(text: str) -> List[float]:
    """Get embedding from Voyage AI."""
    api_key = os.getenv("VOYAGE_API_KEY")
    if not api_key:
        raise ValueError("VOYAGE_API_KEY not set")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.voyageai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": EMBEDDING_MODEL,
                "input": text,
                "input_type": "document"
            },
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()["data"][0]["embedding"]


async def _voyage_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """Get embeddings for batch from Voyage AI."""
    api_key = os.getenv("VOYAGE_API_KEY")
    if not api_key:
        raise ValueError("VOYAGE_API_KEY not set")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.voyageai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": EMBEDDING_MODEL,
                "input": texts,
                "input_type": "document"
            },
            timeout=60.0
        )
        response.raise_for_status()
        data = response.json()["data"]
        # Sort by index to maintain order
        sorted_data = sorted(data, key=lambda x: x["index"])
        return [d["embedding"] for d in sorted_data]


async def _openai_embedding(text: str) -> List[float]:
    """Get embedding from OpenAI."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": EMBEDDING_MODEL,
                "input": text
            },
            timeout=30.0
        )
        response.raise_for_status()
        return response.json()["data"][0]["embedding"]


async def _openai_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """Get embeddings for batch from OpenAI."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": EMBEDDING_MODEL,
                "input": texts
            },
            timeout=60.0
        )
        response.raise_for_status()
        data = response.json()["data"]
        sorted_data = sorted(data, key=lambda x: x["index"])
        return [d["embedding"] for d in sorted_data]


def prepare_event_for_embedding(event: dict) -> str:
    """
    Prepare event content for embedding.
    Combines relevant fields into a single searchable text.
    """
    event_type = event.get("event_type", "unknown")
    
    if event_type == "email":
        # Combine subject and body for emails
        subject = event.get("subject", "")
        body = event.get("body", "") or event.get("raw_content", "")
        return f"Email Subject: {subject}\n\n{body}"
    
    elif event_type == "phone_call":
        # Use transcript or summary
        transcript = event.get("transcript", "") or event.get("raw_content", "")
        summary = event.get("summary", "")
        if transcript:
            return f"Phone Call Transcript:\n{transcript}"
        return f"Phone Call Summary:\n{summary}"
    
    elif event_type == "sms":
        return f"SMS Message:\n{event.get('content', '') or event.get('raw_content', '')}"
    
    else:
        # Fallback to raw content or summary
        return event.get("raw_content", "") or event.get("summary", "")


# For query embeddings, we use a different input_type
async def get_query_embedding(query: str) -> List[float]:
    """Generate embedding for a search query (uses query input type for Voyage)."""
    if EMBEDDING_PROVIDER == "voyage":
        api_key = os.getenv("VOYAGE_API_KEY")
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.voyageai.com/v1/embeddings",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": EMBEDDING_MODEL,
                    "input": query,
                    "input_type": "query"  # Different from document!
                },
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()["data"][0]["embedding"]
    else:
        # OpenAI doesn't distinguish query vs document
        return await get_embedding(query)