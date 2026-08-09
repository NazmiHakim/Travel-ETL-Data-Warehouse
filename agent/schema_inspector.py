from typing import List, Dict, Any, Optional
from agent.db_tools import execute_sql


class DatabaseSchemaInspector:
    """
    Database Schema Inspector Tool / MCP Component
    Provides real-time inspection into PostgreSQL database tables,
    columns, data types, constraints, and distinct value distributions.
    """

    def __init__(self, db_name: str = "db_dwh"):
        self.db_name = db_name

    def get_tables_overview(self) -> List[Dict[str, Any]]:
        """
        Returns all user tables in the PostgreSQL database along with row counts.
        """
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        success, df, _ = execute_sql(query)
        if not success or df.empty:
            return []

        tables_info = []
        for tbl in df["table_name"].tolist():
            count_query = f"SELECT COUNT(*) AS total_rows FROM {tbl};"
            s_cnt, df_cnt, _ = execute_sql(count_query)
            row_cnt = int(df_cnt["total_rows"].iloc[0]) if (s_cnt and not df_cnt.empty) else 0
            tables_info.append({"table_name": tbl, "row_count": row_cnt})

        return tables_info

    def get_column_details(self, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Queries information_schema.columns to retrieve exact column names,
        data types, and nullability constraints.
        """
        where_clause = f"AND table_name = '{table_name}'" if table_name else ""
        query = f"""
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' {where_clause}
        ORDER BY table_name, ordinal_position;
        """
        success, df, _ = execute_sql(query)
        if not success or df.empty:
            return []

        return df.to_dict(orient="records")

    def get_distinct_values(self, table_name: str, column_name: str, limit: int = 10) -> List[str]:
        """
        Dynamically queries PostgreSQL to fetch actual unique string values in a column.
        Useful for inspecting categorical column values (e.g. sentiment, categories).
        """
        query = f"SELECT DISTINCT {column_name} FROM {table_name} LIMIT {limit};"
        success, df, _ = execute_sql(query)
        if not success or df.empty:
            return []
        return [str(v) for v in df[column_name].tolist() if v is not None]

    def format_schema_summary(self) -> str:
        """
        Formats a comprehensive markdown summary of live database tables,
        columns, and types for agent system prompts and user inspection.
        """
        cols = self.get_column_details()
        if not cols:
            return "Unable to inspect database schema."

        schema_by_table: Dict[str, List[str]] = {}
        for col in cols:
            tbl = col["table_name"]
            c_str = f"{col['column_name']} ({col['data_type']})"
            schema_by_table.setdefault(tbl, []).append(c_str)

        lines = ["[Live PostgreSQL Schema Summary]"]
        for tbl, col_list in schema_by_table.items():
            lines.append(f"- **{tbl}**: " + ", ".join(col_list))

        return "\n".join(lines)
