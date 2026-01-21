Loom link: https://www.loom.com/share/6ffc496cf8e5476abe2ee84b3193d168


# Harper Insurance Memory System Dashboard

Streamlit dashboard for exploring brokerage accounts, events, semantic search,
and follow-up planning backed by Postgres + pgvector.

## Features
- 📊 Dashboard KPIs and activity charts
- 🔍 Natural-language query engine over events
- 👥 Accounts browser and event timeline
- 📅 Follow-up agent plan generation
- 🗄️ Database schema browser

## Requirements
- Python 3.10+
- PostgreSQL with `pgvector` extension
- API keys for Anthropic and Voyage (for LLM + embeddings)

## Setup

1) Create a virtualenv and install dependencies:

```
python -m venv .venv
source .venv/bin/activate
pip install streamlit pandas plotly psycopg2-binary asyncpg anthropic httpx
```

2) Configure environment variables:

```
export DATABASE_URL="postgresql://localhost/memory_system"
export ANTHROPIC_API_KEY="your-anthropic-key"
export VOYAGE_API_KEY="your-voyage-key"
```

3) Initialize the database:

```
createdb memory_system
psql "$DATABASE_URL" -f schema.sql
```

## Run the app

```
streamlit run app.py
```

## Common workflows

### Generate embeddings

Populates `events.content_embedding` for semantic search:

```
python generate_embeddings.py
```

### Generate a daily follow-up plan (CLI)

```
python followup_agent.py
```

## Notes
- `app.py` uses `query_v2.py` and `followup_agent.py` via subprocess.
- `DATABASE_URL` defaults to `postgresql://localhost/memory_system` if unset.
- If you skip embeddings, semantic search will be limited.
