import json
from typing import Any, Generator

import vertica_python

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class ExecuteQueryTool(Tool):
    """
    Tool for executing SQL queries against Vertica database
    """

    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        Execute SQL query against Vertica database
        """
        try:
            # Get credentials from runtime
            credentials = self.runtime.credentials

            # Parse configuration
            config = self._build_config(credentials)

            # Get query parameters
            sql = tool_parameters.get("sql", "").strip()
            params = tool_parameters.get("params", [])

            if not sql:
                yield self.create_text_message("Error: SQL query is required")
                return

            # Validate query for readonly mode
            self._validate_readonly_query(sql, config)

            # Execute query
            result = self._execute_query(sql, params, config)

            # Format and return result
            formatted_result = self._format_result(result, sql)
            yield self.create_json_message(formatted_result)

        except Exception as e:
            error_result = {
                "success": False,
                "error": str(e),
                "query": tool_parameters.get("sql", ""),
                "executed_at": self._get_current_timestamp()
            }
            yield self.create_json_message(error_result)

    def _build_config(self, credentials: dict[str, Any]) -> dict[str, Any]:
        """Build database configuration from credentials"""
        return {
            "host": credentials.get("vertica_host"),
            "port": int(credentials.get("vertica_port", 5433)),
            "database": credentials.get("vertica_database"),
            "user": credentials.get("vertica_user"),
            "password": credentials.get("vertica_password", ""),
            "readonly_mode": credentials.get("vertica_readonly_mode", "true").lower() == "true",
            "connection_limit": int(credentials.get("vertica_connection_limit", 10)),
            "query_timeout": int(credentials.get("vertica_query_timeout", 60000)),
            "ssl": credentials.get("vertica_ssl", "false").lower() == "true",
            "default_schema": credentials.get("vertica_default_schema", "public")
        }

    def _validate_readonly_query(self, sql: str, config: dict[str, Any]) -> None:
        """Validate that query is readonly if readonly mode is enabled"""
        if not config["readonly_mode"]:
            return

        readonly_prefixes = ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"]
        trimmed_sql = sql.strip().upper()

        is_readonly = any(trimmed_sql.startswith(prefix) for prefix in readonly_prefixes)

        if not is_readonly:
            raise ValueError(
                f"Only readonly queries are allowed (readonly mode is enabled). "
                f"Query must start with: {', '.join(readonly_prefixes)}. "
                f"To allow all queries, set vertica_readonly_mode to 'false'."
            )

    def _execute_query(self, sql: str, params: list, config: dict[str, Any]) -> dict[str, Any]:
        """Execute query against database"""
        connection = None
        try:
            # Build connection parameters
            conn_params = {
                "host": config["host"],
                "port": config["port"],
                "database": config["database"],
                "user": config["user"],
                "password": config["password"],
                "connection_timeout": config["query_timeout"] // 1000,  # Convert to seconds
            }

            # Add SSL configuration
            if config["ssl"]:
                conn_params["ssl"] = True
            else:
                conn_params["ssl"] = False

            # Connect to database
            connection = vertica_python.connect(**conn_params)

            # Execute query
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                return {
                    "rows": [dict(zip(columns, row)) for row in rows],
                    "row_count": len(rows),
                    "fields": columns,
                    "command": self._get_command_type(sql)
                }

        finally:
            if connection:
                connection.close()

    def _get_command_type(self, sql: str) -> str:
        """Extract command type from SQL"""
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("SELECT"):
            return "SELECT"
        elif sql_upper.startswith("INSERT"):
            return "INSERT"
        elif sql_upper.startswith("UPDATE"):
            return "UPDATE"
        elif sql_upper.startswith("DELETE"):
            return "DELETE"
        elif sql_upper.startswith("CREATE"):
            return "CREATE"
        elif sql_upper.startswith("DROP"):
            return "DROP"
        elif sql_upper.startswith("ALTER"):
            return "ALTER"
        elif sql_upper.startswith("SHOW"):
            return "SHOW"
        elif sql_upper.startswith("DESCRIBE"):
            return "DESCRIBE"
        elif sql_upper.startswith("EXPLAIN"):
            return "EXPLAIN"
        else:
            return "UNKNOWN"

    def _format_result(self, result: dict[str, Any], sql: str) -> dict[str, Any]:
        """Format query result for output"""
        return {
            "success": True,
            "data": result["rows"],
            "row_count": result["row_count"],
            "fields": result["fields"],
            "command": result["command"],
            "query": sql,
            "executed_at": self._get_current_timestamp()
        }

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.utcnow().isoformat() + "Z"
