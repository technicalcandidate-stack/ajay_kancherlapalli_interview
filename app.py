"""
Harper Insurance Memory System Dashboard
Run: streamlit run app.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess

# Page config
st.set_page_config(
    page_title="Harper Insurance - Memory System",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/memory_system")

@st.cache_resource
def get_connection():
    """Create database connection."""
    return psycopg2.connect(DATABASE_URL)

def query_db(sql, params=None):
    """Execute a query and return results as list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if cur.description:
                return [dict(row) for row in cur.fetchall()]
            return []
    except Exception as e:
        conn.rollback()
        raise e

def query_one(sql, params=None):
    """Execute a query and return single result."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        conn.rollback()
        raise e

# Sidebar navigation
st.sidebar.title("🏢 Harper Insurance")
st.sidebar.markdown("### Memory System")

page = st.sidebar.radio(
    "Navigate",
    ["📊 Dashboard", "🔍 Query Engine", "👥 Accounts", "📅 Follow-Up Agent", "🗄️ Database Schema"]
)

# =============================================================================
# DASHBOARD PAGE
# =============================================================================
if page == "📊 Dashboard":
    st.title("📊 Brokerage Dashboard")
    
    # Get summary stats
    stage_counts = query_db("""
        SELECT stage, COUNT(*) as count 
        FROM accounts 
        GROUP BY stage 
        ORDER BY count DESC
    """)
    
    event_stats = query_one("""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT account_id) as accounts_with_events,
            COUNT(CASE WHEN occurred_at > NOW() - INTERVAL '7 days' THEN 1 END) as events_last_7_days
        FROM events
    """)
    
    event_types = query_db("""
        SELECT event_type, COUNT(*) as count
        FROM events
        GROUP BY event_type
        ORDER BY count DESC
    """)
    
    daily_activity = query_db("""
        SELECT 
            DATE(occurred_at) as date,
            COUNT(*) as count
        FROM events
        WHERE occurred_at > NOW() - INTERVAL '14 days'
        GROUP BY DATE(occurred_at)
        ORDER BY date
    """)
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_accounts = sum(s['count'] for s in stage_counts)
        st.metric("Total Accounts", total_accounts)
    
    with col2:
        st.metric("Total Events", event_stats['total_events'] if event_stats else 0)
    
    with col3:
        st.metric("Events (Last 7 Days)", event_stats['events_last_7_days'] if event_stats else 0)
    
    with col4:
        st.metric("Accounts with Activity", event_stats['accounts_with_events'] if event_stats else 0)
    
    st.divider()
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pipeline by Stage")
        if stage_counts:
            df_stages = pd.DataFrame(stage_counts)
            fig = px.pie(df_stages, values='count', names='stage', hole=0.4)
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Events by Type")
        if event_types:
            df_types = pd.DataFrame(event_types)
            fig = px.bar(df_types, x='event_type', y='count', color='count')
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    # Activity timeline
    st.subheader("Activity Timeline (Last 14 Days)")
    if daily_activity:
        df_activity = pd.DataFrame(daily_activity)
        fig = px.line(df_activity, x='date', y='count', markers=True)
        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# QUERY ENGINE PAGE
# =============================================================================
elif page == "🔍 Query Engine":
    st.title("🔍 Query Engine")
    st.markdown("Ask questions about your accounts and communications using natural language.")
    
    # Query input
    query_text = st.text_area(
        "Enter your query",
        placeholder="e.g., What is the status of Sunny Days Childcare's application?\nor: Which accounts are concerned about pricing?",
        height=100
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        search_btn = st.button("🔍 Search", type="primary", use_container_width=True)
    
    if search_btn and query_text:
        with st.spinner("Searching and analyzing..."):
            try:
                # Run query_v2.py as subprocess
                result = subprocess.run(
                    ["python", "-c", f"""
import asyncio
import os
os.environ['DATABASE_URL'] = '{DATABASE_URL}'
from query_v2 import MemoryQueryEngine
import asyncpg

async def run():
    pool = await asyncpg.create_pool('{DATABASE_URL}')
    engine = MemoryQueryEngine(pool)
    result = await engine.query('''{query_text.replace("'", "\\'")}''')
    print(result)
    await pool.close()

asyncio.run(run())
"""],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                if result.returncode == 0:
                    output = result.stdout.strip()
                    
                    # Parse result to separate answer, confidence, and sources
                    parts = output.split("---")
                    answer = parts[0].strip()
                    
                    # Display answer in a nice card
                    st.markdown("### 💬 Answer")
                    st.info(answer)
                    
                    # Parse and display confidence and sources
                    if len(parts) > 1:
                        metadata = "---".join(parts[1:])
                        
                        # Extract confidence
                        confidence_level = "Unknown"
                        confidence_pct = ""
                        confidence_explanation = ""
                        
                        if "**Confidence:" in metadata:
                            conf_section = metadata.split("**Confidence:")[1].split("**Sources")[0]
                            if "High" in conf_section:
                                confidence_level = "High"
                                confidence_color = "🟢"
                            elif "Medium" in conf_section:
                                confidence_level = "Medium"
                                confidence_color = "🟡"
                            else:
                                confidence_level = "Low"
                                confidence_color = "🔴"
                            
                            # Extract percentage
                            import re
                            pct_match = re.search(r'\((\d+)%\)', conf_section)
                            if pct_match:
                                confidence_pct = pct_match.group(1)
                            
                            # Extract explanation
                            lines = conf_section.strip().split('\n')
                            if len(lines) > 1:
                                confidence_explanation = lines[1].strip()
                        
                        # Display confidence
                        st.markdown("### 📊 Confidence")
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.metric(
                                label="Confidence Level",
                                value=f"{confidence_color} {confidence_level}",
                                delta=f"{confidence_pct}%" if confidence_pct else None
                            )
                        with col2:
                            if confidence_explanation:
                                st.caption(confidence_explanation)
                        
                        # Parse and display sources
                        st.markdown("### 📚 Sources Referenced")
                        
                        if "**Sources Referenced:**" in metadata:
                            sources_section = metadata.split("**Sources Referenced:**")[1].strip()
                            source_lines = sources_section.split('\n')
                            
                            sources = []
                            current_source = None
                            
                            for line in source_lines:
                                if line.strip().startswith('[SOURCE'):
                                    if current_source:
                                        sources.append(current_source)
                                    
                                    # Parse source line
                                    # Format: [SOURCE 1] Event #142 | Account | type | date | Direction: X | Similarity: 0.55
                                    current_source = {'raw': line.strip(), 'summary': ''}
                                    
                                    parts_list = line.split('|')
                                    if len(parts_list) >= 4:
                                        # Extract source number and event ID
                                        first_part = parts_list[0].strip()
                                        source_match = re.search(r'\[SOURCE (\d+)\]', first_part)
                                        event_match = re.search(r'Event #(\d+)', first_part)
                                        
                                        current_source['source_num'] = source_match.group(1) if source_match else '?'
                                        current_source['event_id'] = event_match.group(1) if event_match else '?'
                                        current_source['account'] = parts_list[1].strip() if len(parts_list) > 1 else 'Unknown'
                                        current_source['type'] = parts_list[2].strip() if len(parts_list) > 2 else 'Unknown'
                                        current_source['date'] = parts_list[3].strip() if len(parts_list) > 3 else 'Unknown'
                                        
                                        # Direction
                                        dir_match = re.search(r'Direction: (\w+)', line)
                                        current_source['direction'] = dir_match.group(1) if dir_match else 'N/A'
                                        
                                        # Similarity
                                        sim_match = re.search(r'Similarity: ([\d.]+)', line)
                                        current_source['similarity'] = float(sim_match.group(1)) if sim_match else 0
                                
                                elif line.strip().startswith('Summary:') and current_source:
                                    current_source['summary'] = line.replace('Summary:', '').strip()
                            
                            if current_source:
                                sources.append(current_source)
                            
                            # Display sources as cards
                            if sources:
                                for source in sources:
                                    similarity = source.get('similarity', 0)
                                    if similarity >= 0.6:
                                        relevance = "🟢 High"
                                        border_color = "#28a745"
                                    elif similarity >= 0.4:
                                        relevance = "🟡 Medium"
                                        border_color = "#ffc107"
                                    else:
                                        relevance = "🔴 Low"
                                        border_color = "#dc3545"
                                    
                                    # Event type icons
                                    type_icons = {
                                        'email': '📧',
                                        'call': '📞',
                                        'sms': '💬',
                                        'quote': '💰'
                                    }
                                    event_type = source.get('type', '').lower()
                                    icon = next((v for k, v in type_icons.items() if k in event_type), '📄')
                                    
                                    with st.container():
                                        st.markdown(f"""
                                        <div style="border-left: 4px solid {border_color}; padding: 10px; margin: 10px 0; background-color: #f8f9fa; border-radius: 4px;">
                                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                                <strong>{icon} Source {source.get('source_num', '?')}</strong>
                                                <span style="color: gray; font-size: 0.9em;">Event #{source.get('event_id', '?')} | {relevance} ({similarity:.0%})</span>
                                            </div>
                                            <div style="margin-top: 8px;">
                                                <span style="background: #e9ecef; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; margin-right: 8px;">
                                                    {source.get('type', 'Unknown')}
                                                </span>
                                                <span style="background: #e9ecef; padding: 2px 8px; border-radius: 4px; font-size: 0.85em; margin-right: 8px;">
                                                    {source.get('date', 'Unknown')}
                                                </span>
                                                <span style="background: #e9ecef; padding: 2px 8px; border-radius: 4px; font-size: 0.85em;">
                                                    {source.get('direction', 'N/A')}
                                                </span>
                                            </div>
                                            <div style="margin-top: 8px; color: #495057;">
                                                <strong>Account:</strong> {source.get('account', 'Unknown')}
                                            </div>
                                        """, unsafe_allow_html=True)
                                        
                                        if source.get('summary'):
                                            st.markdown(f"""
                                            <div style="margin-top: 8px; color: #6c757d; font-size: 0.9em; font-style: italic;">
                                                {source.get('summary', '')}
                                            </div>
                                        """, unsafe_allow_html=True)
                                        
                                        st.markdown("</div>", unsafe_allow_html=True)
                            else:
                                st.caption("No sources parsed")
                else:
                    st.error(f"Error: {result.stderr}")
                
            except subprocess.TimeoutExpired:
                st.error("Query timed out. Please try a simpler query.")
            except Exception as e:
                st.error(f"Error: {e}")
                import traceback
                st.code(traceback.format_exc())
    
    # Example queries
    st.divider()
    st.markdown("### 💡 Example Queries")
    
    examples = [
        ("🏢 Account Status", "What is the status of Sunny Days Childcare's application?"),
        ("📊 Pipeline", "Which accounts are in the quote stage?"),
        ("⏰ Follow-up", "List all accounts that need follow-up"),
        ("💰 Pricing", "What concerns have clients raised about pricing?"),
        ("📈 Overview", "Give me a pipeline breakdown by stage")
    ]
    
    cols = st.columns(3)
    for i, (label, example) in enumerate(examples):
        with cols[i % 3]:
            if st.button(f"{label}", key=f"example_{i}", use_container_width=True):
                st.session_state['pending_query'] = example
                st.rerun()

# =============================================================================
# ACCOUNTS PAGE
# =============================================================================
elif page == "👥 Accounts":
    st.title("👥 Accounts")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    stages = [s['stage'] for s in query_db("SELECT DISTINCT stage FROM accounts ORDER BY stage")]
    industries = [i['industry'] for i in query_db("SELECT DISTINCT industry FROM accounts WHERE industry IS NOT NULL ORDER BY industry")]
    
    with col1:
        selected_stage = st.selectbox("Filter by Stage", ["All"] + stages)
    with col2:
        selected_industry = st.selectbox("Filter by Industry", ["All"] + industries)
    with col3:
        search_name = st.text_input("Search by Name")
    
    # Build query
    query = """
        SELECT 
            a.id, a.name, a.stage, a.stage_detail, a.industry,
            a.address_state, a.primary_email, a.primary_phone,
            COUNT(e.id) as event_count,
            MAX(e.occurred_at) as last_event
        FROM accounts a
        LEFT JOIN events e ON a.id = e.account_id
        WHERE 1=1
    """
    params = []
    
    if selected_stage != "All":
        query += " AND a.stage = %s"
        params.append(selected_stage)
    
    if selected_industry != "All":
        query += " AND a.industry = %s"
        params.append(selected_industry)
    
    if search_name:
        query += " AND a.name ILIKE %s"
        params.append(f"%{search_name}%")
    
    query += " GROUP BY a.id ORDER BY a.name LIMIT 100"
    
    accounts = query_db(query, params)
    
    st.markdown(f"**{len(accounts)} accounts found**")
    
    # Display as dataframe
    if accounts:
        df = pd.DataFrame(accounts)
        if 'last_event' in df.columns:
            df['last_event'] = pd.to_datetime(df['last_event']).dt.strftime('%Y-%m-%d %H:%M')
        
        st.dataframe(
            df[['id', 'name', 'stage', 'industry', 'address_state', 'event_count', 'last_event']],
            use_container_width=True,
            hide_index=True
        )
        
        # Account detail view
        st.divider()
        selected_account = st.selectbox(
            "Select account for details",
            options=[a['id'] for a in accounts],
            format_func=lambda x: next((a['name'] for a in accounts if a['id'] == x), x)
        )
        
        if selected_account:
            account = query_one("SELECT * FROM accounts WHERE id = %s", (selected_account,))
            events = query_db("""
                SELECT id, event_type, occurred_at, direction, summary, raw_content,
                       extracted_sentiment, from_address, to_addresses, subject
                FROM events
                WHERE account_id = %s
                ORDER BY occurred_at DESC
                LIMIT 20
            """, (selected_account,))
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.markdown("### 🏢 Account Details")
                
                # Account info card
                stage_colors = {
                    'lead': '🔵',
                    'intake': '🟣',
                    'application': '🟠',
                    'submission': '🟡',
                    'quote': '🟢',
                    'closing': '💚',
                    'policy': '✅'
                }
                stage_icon = stage_colors.get(account['stage'].lower(), '⚪')
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; color: white;">
                    <h3 style="margin: 0; color: white;">{account['name']}</h3>
                    <p style="margin: 5px 0; opacity: 0.9;">{stage_icon} {account['stage'].title()}{' / ' + account['stage_detail'] if account.get('stage_detail') else ''}</p>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("")
                
                # Details grid
                details = [
                    ("🏭 Industry", account.get('industry') or 'N/A'),
                    ("📍 State", account.get('address_state') or 'N/A'),
                    ("📧 Email", account.get('primary_email') or 'N/A'),
                    ("📞 Phone", account.get('primary_phone') or 'N/A'),
                ]
                
                for label, value in details:
                    st.markdown(f"**{label}:** {value}")
            
            with col2:
                st.markdown("### 📅 Event Timeline")
                
                if events:
                    # Event type icons and colors
                    type_config = {
                        'email_outbound': ('📤', '#17a2b8', 'Outbound Email'),
                        'email_inbound': ('📥', '#28a745', 'Inbound Email'),
                        'call_outgoing': ('📞', '#6f42c1', 'Outgoing Call'),
                        'call_incoming': ('📲', '#20c997', 'Incoming Call'),
                        'sms_outgoing': ('💬', '#fd7e14', 'Outgoing SMS'),
                        'sms_incoming': ('📱', '#e83e8c', 'Incoming SMS'),
                        'quote_received': ('💰', '#ffc107', 'Quote Received'),
                    }
                    
                    sentiment_icons = {
                        'positive': '😊',
                        'negative': '😟',
                        'neutral': '😐'
                    }
                    
                    for e in events:
                        event_type = e['event_type'] or 'unknown'
                        config = type_config.get(event_type, ('📄', '#6c757d', event_type))
                        icon, color, label = config
                        
                        occurred = e['occurred_at'].strftime('%b %d, %Y %H:%M') if e['occurred_at'] else 'N/A'
                        sentiment = e.get('extracted_sentiment')
                        sentiment_icon = sentiment_icons.get(sentiment, '') if sentiment else ''
                        
                        # Build event card
                        st.markdown(f"""
                        <div style="border-left: 4px solid {color}; padding: 12px 15px; margin: 10px 0; background: #f8f9fa; border-radius: 0 8px 8px 0;">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                <span style="font-weight: bold; color: {color};">{icon} {label}</span>
                                <span style="color: #6c757d; font-size: 0.85em;">🕐 {occurred} {sentiment_icon}</span>
                            </div>
                        """, unsafe_allow_html=True)
                        
                        # Subject line for emails
                        if e.get('subject'):
                            st.markdown(f"""
                            <div style="font-weight: 500; margin-bottom: 5px;">📋 {e['subject']}</div>
                            """, unsafe_allow_html=True)
                        
                        # Summary
                        if e.get('summary'):
                            st.markdown(f"""
                            <div style="color: #495057; font-size: 0.9em;">{e['summary'][:300]}{'...' if len(e['summary'] or '') > 300 else ''}</div>
                            """, unsafe_allow_html=True)
                        
                        # Show raw content in expander
                        st.markdown("</div>", unsafe_allow_html=True)
                        
                        if e.get('raw_content'):
                            with st.expander(f"View full content (Event #{e['id']})", expanded=False):
                                st.text(e['raw_content'][:2000])
                else:
                    st.info("No events found for this account.")

# =============================================================================
# FOLLOW-UP AGENT PAGE
# =============================================================================
elif page == "📅 Follow-Up Agent":
    st.title("📅 Follow-Up Agent")
    st.markdown("AI-powered follow-up planning and execution.")
    
    # Generate plan button
    if st.button("🔄 Generate Daily Follow-Up Plan", type="primary"):
        with st.spinner("Analyzing accounts and generating follow-up plan..."):
            try:
                # Run followup_agent.py as subprocess
                result = subprocess.run(
                    ["python", "followup_agent.py"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=os.path.dirname(os.path.abspath(__file__))
                )
                
                if result.returncode == 0:
                    st.session_state['followup_output'] = result.stdout
                else:
                    st.error(f"Error: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                st.error("Plan generation timed out.")
            except Exception as e:
                st.error(f"Error: {e}")
    
    # Display output if exists
    if 'followup_output' in st.session_state:
        output = st.session_state['followup_output']
        
        # Parse the output to extract metrics
        lines = output.split('\n')
        
        # Find summary section
        in_summary = False
        in_actions = False
        actions = []
        current_action = {}
        
        for line in lines:
            if "SUMMARY" in line:
                in_summary = True
            elif "ACTION ITEMS" in line:
                in_summary = False
                in_actions = True
            elif in_actions and line.strip().startswith(tuple('0123456789')):
                if current_action:
                    actions.append(current_action)
                current_action = {'header': line.strip()}
            elif in_actions and current_action:
                if 'Stage:' in line:
                    current_action['stage'] = line.split('Stage:')[1].strip()
                elif 'Channel:' in line:
                    current_action['channel'] = line.split('Channel:')[1].strip()
                elif 'Reason:' in line:
                    current_action['reason'] = line.split('Reason:')[1].strip()
                elif 'Context:' in line:
                    current_action['context'] = line.split('Context:')[1].strip()
        
        if current_action:
            actions.append(current_action)
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Actions", len(actions))
        with col2:
            critical = sum(1 for a in actions if '🔴' in a.get('header', ''))
            st.metric("🔴 Critical", critical)
        with col3:
            high = sum(1 for a in actions if '🟠' in a.get('header', ''))
            st.metric("🟠 High Priority", high)
        
        # Display actions
        st.markdown("### 📋 Action Items")
        
        for action in actions:
            header = action.get('header', 'Unknown')
            with st.expander(header, expanded=('🔴' in header)):
                if 'stage' in action:
                    st.markdown(f"**Stage:** {action['stage']}")
                if 'channel' in action:
                    st.markdown(f"**Channel:** {action['channel']}")
                if 'reason' in action:
                    st.markdown(f"**Reason:** {action['reason']}")
                if 'context' in action:
                    st.markdown(f"**Context:** {action['context']}")
        
        # Raw output
        with st.expander("📄 Raw Output"):
            st.code(output)

# =============================================================================
# DATABASE SCHEMA PAGE
# =============================================================================
elif page == "🗄️ Database Schema":
    st.title("🗄️ Database Schema")
    
    # Get table info
    tables = query_db("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name
    """)
    
    for table in tables:
        table_name = table['table_name']
        
        columns = query_db("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        count_result = query_one(f"SELECT COUNT(*) as count FROM {table_name}")
        row_count = count_result['count'] if count_result else 0
        
        with st.expander(f"📋 {table_name} ({row_count} rows)", expanded=(table_name in ['accounts', 'events'])):
            df = pd.DataFrame(columns)
            st.dataframe(df, use_container_width=True, hide_index=True)
    
    # Architecture diagram
    st.divider()
    st.markdown("### 🏗️ System Architecture")
    
    st.code("""
    ┌─────────────────────────────────────────────────────────────────────┐
    │                         STREAMLIT DASHBOARD                         │
    ├─────────────────────────────────────────────────────────────────────┤
    │  📊 Dashboard  │  🔍 Query  │  👥 Accounts  │  📅 Follow-Up         │
    └────────┬───────────────┬────────────────┬───────────────┬──────────┘
             │               │                │               │
             ▼               ▼                ▼               ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                         PYTHON MODULES                              │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐ │
    │  │ query_v2.py │  │ search.py   │  │embedding.py │  │followup_   │ │
    │  │             │  │             │  │             │  │agent.py    │ │
    │  │ • Query     │  │ • Vector    │  │ • Voyage AI │  │ • Planning │ │
    │  │   routing   │  │   search    │  │ • Embeddings│  │ • Actions  │ │
    │  │ • LLM calls │  │ • pgvector  │  │             │  │ • Drafts   │ │
    │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘ │
    └────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                      POSTGRESQL + PGVECTOR                          │
    │  ┌─────────────────────────┐    ┌─────────────────────────────────┐ │
    │  │       accounts          │    │           events                │ │
    │  │  (State/Metadata)       │    │  (Temporal/Communications)      │ │
    │  │                         │    │                                 │ │
    │  │  • id, name, stage      │    │  • id, account_id, event_type   │ │
    │  │  • industry, state      │    │  • occurred_at, raw_content     │ │
    │  │  • primary_email        │    │  • summary, content_embedding   │ │
    │  └─────────────────────────┘    └─────────────────────────────────┘ │
    └─────────────────────────────────────────────────────────────────────┘
    """, language=None)

# Footer
st.sidebar.divider()
st.sidebar.caption("Harper Insurance Memory System v1.0")
st.sidebar.caption(f"DB: {DATABASE_URL.split('/')[-1]}")