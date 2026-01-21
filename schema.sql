-- ============================================================================
-- MEMORY SYSTEM DATABASE SCHEMA (PostgreSQL 14 Compatible)
-- ============================================================================
-- Run: psql -d memory_system -f schema.sql
-- ============================================================================

-- Extensions (vector is optional - comment out if not installed)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE EXTENSION IF NOT EXISTS vector;  -- Uncomment if you install pgvector

-- ============================================================================
-- TABLE: producers
-- ============================================================================
CREATE TABLE producers (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(100) UNIQUE,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- TABLE: contacts
-- ============================================================================
CREATE TABLE contacts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    role VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_contacts_email ON contacts(LOWER(email));
CREATE INDEX idx_contacts_phone ON contacts(phone);
CREATE INDEX idx_contacts_name_trgm ON contacts USING gin(name gin_trgm_ops);

-- ============================================================================
-- TABLE: accounts
-- ============================================================================
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(100) UNIQUE,
    
    -- Basic info
    name VARCHAR(255) NOT NULL,
    description TEXT,
    website VARCHAR(255),
    primary_email VARCHAR(255),
    primary_phone VARCHAR(50),
    
    -- Industry
    industry VARCHAR(255),
    sub_industry VARCHAR(255),
    naics_code VARCHAR(10),
    sic_code VARCHAR(10),
    
    -- Location
    address_city VARCHAR(100),
    address_state CHAR(2),
    address_postal VARCHAR(20),
    timezone VARCHAR(50),
    
    -- Business metrics
    annual_revenue_usd DECIMAL(15,2),
    annual_payroll_usd DECIMAL(15,2),
    employee_count_ft INTEGER,
    employee_count_pt INTEGER,
    years_in_business INTEGER,
    
    -- Insurance
    coverage_types TEXT[] DEFAULT '{}',
    coverage_details JSONB DEFAULT '{}',
    
    -- Pipeline state (DERIVED)
    -- Stages: intake → application → submission → quote
    stage VARCHAR(50) NOT NULL DEFAULT 'intake',
    stage_detail VARCHAR(100),
    stage_changed_at TIMESTAMPTZ,
    stage_change_event_id INTEGER,  -- FK added later
    source_stage VARCHAR(100),
    
    -- Assignment
    assigned_producer_id INTEGER REFERENCES producers(id),
    
    -- Quote status (DERIVED)
    has_active_quote BOOLEAN DEFAULT FALSE,
    best_quote_premium DECIMAL(12,2),
    best_quote_carrier VARCHAR(255),
    quote_count INTEGER DEFAULT 0,
    
    -- Pending items (DERIVED)
    pending_from_customer TEXT[] DEFAULT '{}',
    pending_from_carrier TEXT[] DEFAULT '{}',
    
    -- Follow-up
    next_action TEXT,
    next_action_date DATE,
    
    -- Activity tracking
    last_customer_contact_at TIMESTAMPTZ,
    last_activity_at TIMESTAMPTZ,
    -- Removed: days_in_current_stage (was causing error, calculate in queries instead)
    
    -- Summary (LLM-generated)
    executive_summary TEXT,
    summary_generated_at TIMESTAMPTZ,
    
    -- Flags
    is_urgent BOOLEAN DEFAULT FALSE,
    urgency_reason TEXT,
    customer_sentiment VARCHAR(20),
    
    -- Source data
    tivly_entry_date_time TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_accounts_stage ON accounts(stage);
CREATE INDEX idx_accounts_external ON accounts(external_id);
CREATE INDEX idx_accounts_name_trgm ON accounts USING gin(name gin_trgm_ops);
CREATE INDEX idx_accounts_email ON accounts(primary_email);

-- ============================================================================
-- TABLE: events
-- ============================================================================
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id),
    
    -- Identification
    external_id VARCHAR(255),
    event_type VARCHAR(50) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    direction VARCHAR(10),
    
    -- Temporal
    occurred_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Participants
    from_address VARCHAR(255),
    from_name VARCHAR(255),
    to_addresses TEXT[],
    
    -- Content
    subject VARCHAR(500),
    raw_content TEXT,
    content_hash VARCHAR(64),
    
    -- Call-specific
    duration_seconds INTEGER,
    
    -- Extracted data
    summary TEXT,
    event_significance VARCHAR(20),
    extracted_stage_signal VARCHAR(50),
    extracted_action_items TEXT[],
    extracted_sentiment VARCHAR(20),
    extraction_data JSONB DEFAULT '{}',
    
    -- Processing
    extraction_status VARCHAR(20) DEFAULT 'pending',
    
    -- Semantic search (uncomment if pgvector installed)
    -- embedding vector(1536),
    
    -- Source
    stage_tags TEXT[],
    source_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_events_dedup ON events(account_id, channel, content_hash) WHERE content_hash IS NOT NULL;
CREATE INDEX idx_events_account_time ON events(account_id, occurred_at DESC);
CREATE INDEX idx_events_pending ON events(extraction_status) WHERE extraction_status = 'pending';
-- Uncomment if pgvector installed:
-- CREATE INDEX idx_events_embedding ON events USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Add FK for stage_change_event_id
ALTER TABLE accounts ADD CONSTRAINT fk_stage_event FOREIGN KEY (stage_change_event_id) REFERENCES events(id);

-- ============================================================================
-- TABLE: quotes
-- ============================================================================
CREATE TABLE quotes (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id),
    source_event_id INTEGER REFERENCES events(id),
    
    carrier_name VARCHAR(255) NOT NULL,
    carrier_state VARCHAR(10),
    carrier_reference VARCHAR(100),
    
    coverage_types TEXT[],
    premium DECIMAL(12,2),
    valid_until DATE,
    
    status VARCHAR(30) DEFAULT 'preliminary',
    subjectivities TEXT[],
    
    raw_quote_data JSONB,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_quotes_account ON quotes(account_id);

-- ============================================================================
-- TABLE: pending_items
-- ============================================================================
CREATE TABLE pending_items (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id),
    
    item_type VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    normalized_key VARCHAR(100),
    responsible_party VARCHAR(20) NOT NULL,
    
    requested_event_id INTEGER REFERENCES events(id),
    requested_at TIMESTAMPTZ,
    
    status VARCHAR(20) DEFAULT 'pending',
    resolved_event_id INTEGER REFERENCES events(id),
    resolved_at TIMESTAMPTZ,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_pending_account ON pending_items(account_id, status);

-- ============================================================================
-- TABLE: account_state_changes (audit log)
-- ============================================================================
CREATE TABLE account_state_changes (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id),
    field_name VARCHAR(100) NOT NULL,
    old_value JSONB,
    new_value JSONB,
    trigger_event_id INTEGER REFERENCES events(id),
    change_type VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_state_changes_account ON account_state_changes(account_id, created_at DESC);

-- ============================================================================
-- ENTITY RESOLUTION TABLES
-- ============================================================================
CREATE TABLE account_email_domains (
    domain VARCHAR(255) PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
    confidence DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE account_phones (
    normalized_phone VARCHAR(20) PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE account_aliases (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
    alias_type VARCHAR(50),
    alias_value VARCHAR(255) NOT NULL,
    confidence DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_aliases_value_trgm ON account_aliases USING gin(alias_value gin_trgm_ops);

-- ============================================================================
-- TABLE: query_log (observability)
-- ============================================================================
CREATE TABLE query_log (
    id SERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    query_type VARCHAR(50),
    resolved_account_ids INTEGER[],
    resolution_method VARCHAR(50),
    events_retrieved INTEGER[],
    response_text TEXT,
    sources_cited INTEGER[],
    processing_time_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================
CREATE OR REPLACE FUNCTION update_account_activity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE accounts SET
        last_activity_at = NEW.occurred_at,
        last_customer_contact_at = CASE 
            WHEN NEW.direction = 'inbound' THEN NEW.occurred_at 
            ELSE last_customer_contact_at 
        END,
        updated_at = NOW()
    WHERE id = NEW.account_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_activity
AFTER INSERT ON events
FOR EACH ROW EXECUTE FUNCTION update_account_activity();

-- ============================================================================
-- HELPER VIEW: days_in_stage (instead of generated column)
-- ============================================================================
CREATE OR REPLACE VIEW accounts_with_stats AS
SELECT 
    a.*,
    EXTRACT(DAY FROM NOW() - COALESCE(a.stage_changed_at, a.created_at))::INTEGER as days_in_current_stage
FROM accounts a;