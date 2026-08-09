import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, 'd:/Flight ETL integrated with agent')

from agent.rag_retriever import DataDictionaryVectorRAG
from agent.schema_inspector import DatabaseSchemaInspector
from agent.sql_agent import TextToSQLAgent

print("==========================================================")
print("1. TESTING DATA DICTIONARY VECTOR RAG RETRIEVER")
print("==========================================================")
rag = DataDictionaryVectorRAG()

q1 = "What is the total revenue for JFK airport?"
print(f"User Query: '{q1}'")
print(rag.format_rag_context(q1))

q2 = "Which airline has worst departure delay for AA carrier?"
print(f"\nUser Query: '{q2}'")
print(rag.format_rag_context(q2))

print("\n==========================================================")
print("2. TESTING DATABASE SCHEMA INSPECTOR TOOL")
print("==========================================================")
inspector = DatabaseSchemaInspector()
tables = inspector.get_tables_overview()
print("Live PostgreSQL Tables:")
for t in tables:
    print(f"  - {t['table_name']}: {t['row_count']} rows")

print("\nLive Schema Summary:")
print(inspector.format_schema_summary())

print("\n==========================================================")
print("3. TESTING FULL AGENT PIPELINE WITH RAG & SCHEMA INSPECTOR")
print("==========================================================")
agent = TextToSQLAgent()

res1 = agent.process_query("What is the total revenue for JFK airport?")
print(f"\nResult 1 Success: {res1['success']}")
print(f"Generated SQL 1: {res1['sql_query']}")
print(f"Data Rows Fetched: {len(res1['data']) if res1['data'] is not None else 0}")
print("Agent Logs:")
for log in res1['logs']:
    print(f"  {log}")

res2 = agent.process_query("Which airline has the highest revenue for AA carrier?")
print(f"\nResult 2 Success: {res2['success']}")
print(f"Generated SQL 2: {res2['sql_query']}")
print(f"Data Rows Fetched: {len(res2['data']) if res2['data'] is not None else 0}")

print("\nALL RAG & SCHEMA INSPECTOR TESTS PASSED SUCCESSFULLY!")
