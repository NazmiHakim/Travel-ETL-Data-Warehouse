import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent.sql_agent import TextToSQLAgent
import agent.sql_agent as _sql_agent_module
from agent.db_tools import get_schema_info, get_db_status

# ---------------------------------------------------------------------------
# Page Config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="TravelNusantara AI Analyst",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
* { font-family: 'Inter', sans-serif; }
.main-header {
    font-size: 2rem; font-weight: 700;
    background: linear-gradient(135deg, #1e3a8a, #2563eb);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.sub-header { color: #6b7280; font-size: 1rem; margin-bottom: 1rem; }
.metric-badge {
    display: inline-block; padding: 2px 8px; border-radius: 12px;
    font-size: 0.75rem; font-weight: 600;
    background: #dbeafe; color: #1e40af; margin: 2px;
}
.elapsed-badge {
    font-size: 0.72rem; color: #6b7280; font-style: italic;
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session State Init
# ---------------------------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = []

# ---------------------------------------------------------------------------
# Agent (cached resource — one instance per server process)
# ---------------------------------------------------------------------------
@st.cache_resource
def load_agent():
    return TextToSQLAgent()

agent = load_agent()

# ---------------------------------------------------------------------------
# DB Status (cached for 30 s to avoid hitting DB on every rerender)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=30)
def fetch_db_status():
    return get_db_status()

# ---------------------------------------------------------------------------
# Smart Chart Renderer
# ---------------------------------------------------------------------------
def render_chart(df: pd.DataFrame, chart_key: str):
    """
    Renders a Plotly chart with smart type detection:
    - Temporal x-axis (year/month/quarter) → line chart
    - Two numeric columns → grouped bar
    - Single numeric column → horizontal bar
    """
    cols = list(df.columns)
    if len(cols) < 2:
        st.dataframe(df, use_container_width=True)
        return

    x_col = cols[0]
    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if not num_cols:
        st.dataframe(df, use_container_width=True)
        return

    y_col = num_cols[0]
    x_title = x_col.replace("_", " ").title()
    y_title = y_col.replace("_", " ").title()

    is_temporal = any(k in x_col.lower() for k in ["year", "month", "quarter", "date"])

    if is_temporal and len(num_cols) >= 2:
        # Line chart with dual y-axes for temporal data
        fig = go.Figure()
        colors = ["#2563eb", "#16a34a", "#dc2626", "#d97706"]
        for i, nc in enumerate(num_cols[:2]):
            fig.add_trace(go.Scatter(
                x=df[x_col].astype(str) if len(df.columns) == 2
                  else df[cols[0]].astype(str) + "-" + df[cols[1]].astype(str),
                y=df[nc],
                name=nc.replace("_", " ").title(),
                mode="lines+markers",
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=6),
            ))
        fig.update_layout(
            title=f"{x_title} Trend",
            template="plotly_white",
            legend=dict(orientation="h", y=-0.2),
        )
    elif len(num_cols) >= 2:
        # Grouped bar for multi-metric comparison
        fig = go.Figure()
        colors = ["#2563eb", "#16a34a"]
        for i, nc in enumerate(num_cols[:2]):
            fig.add_trace(go.Bar(
                name=nc.replace("_", " ").title(),
                x=df[x_col],
                y=df[nc],
                marker_color=colors[i % len(colors)],
                text=df[nc].apply(lambda v: f"{v:,.0f}" if v >= 1 else f"{v:.2f}"),
                textposition="outside",
            ))
        fig.update_layout(
            barmode="group",
            title=f"{x_title} — Multi-Metric Comparison",
            template="plotly_white",
            xaxis_tickangle=-30,
        )
    else:
        # Single metric bar chart with color gradient
        fig = px.bar(
            df, x=x_col, y=y_col,
            title=f"{y_title} by {x_title}",
            color=y_col,
            color_continuous_scale="Blues",
            template="plotly_white",
            text_auto=True,
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(xaxis_tickangle=-30, coloraxis_showscale=False)

    st.plotly_chart(fig, use_container_width=True, key=chart_key)


# ---------------------------------------------------------------------------
# Result Card Renderer (shared between history loop and new response)
# ---------------------------------------------------------------------------
def render_result_card(res: dict, card_key: str):
    if res.get("is_greeting"):
        st.info(res["summary"])
        return

    # Thought process expander
    expanded = card_key.startswith("new_")
    with st.expander("🧠 Agent Thought Process & Reflection Logs", expanded=expanded):
        for log in res["logs"]:
            if "✅ SUCCESS" in log or "Cache hit" in log:
                st.success(log)
            elif "🔴" in log or "❌" in log or "ERROR" in log:
                st.error(log)
            elif "⚠️" in log or "WARNING" in log or "🔁" in log:
                st.warning(log)
            elif "Generated SQL" in log:
                st.caption("**Generated SQL:**")
                st.code(res["sql_query"], language="sql")
            else:
                st.caption(log)

    if res["success"] and res["data"] is not None and not res["data"].empty:
        df = res["data"]

        # Elapsed / cache badge
        badges = []
        if res.get("from_cache"):
            badges.append("⚡ From cache")
        elif res.get("elapsed_seconds", 0) > 0:
            badges.append(f"⏱ {res['elapsed_seconds']:.3f}s")
        if badges:
            st.markdown(
                " &nbsp;".join(f'<span class="elapsed-badge">{b}</span>' for b in badges),
                unsafe_allow_html=True,
            )

        st.markdown(res["summary"])

        t1, t2, t3, t4 = st.tabs(
            ["📊 Chart", "📋 Data Table", "💻 SQL Query", "⬇️ Download"]
        )
        with t1:
            render_chart(df, chart_key=f"chart_{card_key}")
        with t2:
            st.dataframe(df, use_container_width=True, hide_index=True)
        with t3:
            st.code(res["sql_query"], language="sql")
        with t4:
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download as CSV",
                data=csv,
                file_name="query_result.csv",
                mime="text/csv",
                key=f"dl_{card_key}",
            )

    elif res["success"] and res["data"] is not None and res["data"].empty:
        st.warning("⚠️ Query succeeded but returned 0 records for the given criteria.")
    else:
        st.error(res["summary"])


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## ✈️ TravelNusantara")
    st.markdown("**AI Data Analyst Dashboard**")
    st.markdown("---")

    # Live DB Status
    st.markdown("### 🗄️ Database Status")
    db_status = fetch_db_status()
    if db_status["connected"]:
        st.success("🟢 PostgreSQL — Connected")
        for tbl, cnt in db_status["tables"].items():
            st.markdown(
                f'<span class="metric-badge">{tbl}: {cnt:,} rows</span>',
                unsafe_allow_html=True,
            )
    else:
        st.error(
            f"🔴 PostgreSQL — Offline\n\n"
            f"Ensure PostgreSQL is running on `{os.getenv('DB_HOST', 'localhost')}:"
            f"{os.getenv('DB_PORT', '5432')}` and retry."
        )

    st.markdown("---")

    # API Key
    st.markdown("### 🔑 Gemini API Key")
    user_api_key = st.text_input(
        "API Key (optional):",
        type="password",
        help="Enter your Gemini API key for full LLM reasoning. "
             "Leave empty to use the local NLP engine. "
             "Get a free key at aistudio.google.com",
    )
    if user_api_key:
        st.success("✅ Gemini API Active")
    else:
        st.info("💡 Local NLP Engine active")

    st.markdown("---")

    # Sample Questions
    st.markdown("### 💡 Quick Questions")
    sample_queries = [
        "Which is the richest airline?",
        "Which airline has the most bad reviews?",
        "Show top 5 airlines by total revenue",
        "Which airline has the highest average departure delay?",
        "What are the top complaint categories?",
        "List top 5 destination cities by passenger volume",
        "Show monthly revenue trend",
    ]
    selected_sample = None
    for q in sample_queries:
        if st.button(f"📌 {q}", use_container_width=True):
            selected_sample = q

    if st.button("🗑️ Clear Chat + Cache", use_container_width=True, type="secondary"):
        # Clear Streamlit's resource and data caches (forces agent reload)
        st.cache_resource.clear()
        st.cache_data.clear()
        # Clear the in-process SQL result cache
        _sql_agent_module._query_cache.clear()
        st.session_state.messages = []
        st.toast("✅ Chat history and all caches cleared.", icon="🗑️")
        st.rerun()

    st.markdown("---")
    with st.expander("🔍 Live Schema & RAG Inspector"):
        st.caption("📚 **Vector RAG Engine:** TF-IDF Cosine Similarity Active")
        st.caption("🔍 **Live PostgreSQL Schema:**")
        schema_inspector_obj = getattr(agent, "schema_inspector", None)
        summary_text = schema_inspector_obj.format_schema_summary() if schema_inspector_obj else get_schema_info()
        st.code(summary_text, language="markdown")

# ---------------------------------------------------------------------------
# Main Header
# ---------------------------------------------------------------------------
st.markdown(
    '<div class="main-header">✈️ TravelNusantara — AI Data Analyst</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="sub-header">'
    "Conversational Text-to-SQL · Reflection Loop · AI-Enriched Feedback Analytics"
    "</div>",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Chat History
# ---------------------------------------------------------------------------
for idx, msg in enumerate(st.session_state.messages):
    if msg["role"] == "user":
        with st.chat_message("user"):
            st.write(msg["content"])
    elif msg["role"] == "assistant":
        with st.chat_message("assistant"):
            render_result_card(msg["result"], card_key=f"hist_{idx}")

# ---------------------------------------------------------------------------
# New User Input
# ---------------------------------------------------------------------------
user_prompt = st.chat_input(
    "Ask about flights, revenue, delays, destinations, or customer reviews..."
)
if selected_sample:
    user_prompt = selected_sample

if user_prompt:
    st.session_state.messages.append({"role": "user", "content": user_prompt})
    with st.chat_message("user"):
        st.write(user_prompt)

    with st.chat_message("assistant"):
        with st.spinner("🤖 Analyzing, generating SQL, and querying the data warehouse..."):
            res = agent.process_query(
                user_prompt,
                api_key=user_api_key if user_api_key else None,
            )
        new_key = f"new_{len(st.session_state.messages)}"
        render_result_card(res, card_key=new_key)

    st.session_state.messages.append({"role": "assistant", "result": res})

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    "TravelNusantara · ETL & Data Warehouse + AI Agent Portfolio Project · "
    "Powered by PostgreSQL, SQLAlchemy, Streamlit & Google Gemini"
)
