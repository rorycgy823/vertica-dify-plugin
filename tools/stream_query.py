from typing import Any, Generator

import vertica_python

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class StreamQueryTool(Tool):
    """
    Tool for streaming large query results in batches
    """

    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        Execute query and stream results in batches
        """
        try:
            # Get credentials from runtime
            credentials = self.runtime.credentials

            # Parse configuration
            config = self._build_config(credentials)

            # Get query parameters
            sql = tool_parameters.get("sql", "").strip()
            batch_size = min(max(int(tool_parameters.get("batch_size", 1000)), 1), 10000)
            max_rows = tool_parameters.get("max_rows")

            if max_rows is not None:
                max_rows = max(int(max_rows), 1)

            if not sql:
                yield self.create_text_message("Error: SQL query is required")
                return

            # Validate query
            self._validate_query(sql)

            # Stream query results
            for batch_result in self._stream_query(sql, batch_size, max_rows, config):
                yield self.create_json_message(batch_result)

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
            "query_timeout": int(credentials.get("vertica_query_timeout", 60000)),
            "ssl": credentials.get("vertica_ssl", "false").lower() == "true",
        }

    def _validate_query(self, sql: str) -> None:
        """Validate that query doesn't contain LIMIT/OFFSET"""
        sql_upper = sql.upper()
        if " LIMIT " in sql_upper or " OFFSET " in sql_upper:
            raise ValueError(
                "Query should not contain LIMIT or OFFSET clauses when using streamQuery. "
                "Use the batch_size and max_rows parameters instead."
            )

    def _stream_query(self, sql: str, batch_size: int, max_rows: int | None, config: dict[str, Any]) -> Generator[dict[str, Any]]:
        """Stream query results in batches"""
        connection = None
        try:
            # Build connection parameters
            conn_params = {
                "host": config["host"],
                "port": config["port"],
                "database": config["database"],
                "user": config["user"],
                "password": config["password"],
                "connection_timeout": config["query_timeout"] // 1000,
            }

            if config["ssl"]:
                conn_params["ssl"] = True
            else:
                conn_params["ssl"] = False

            # Connect to database
            connection = vertica_python.connect(**conn_params)

            offset = 0
            batch_number = 0
            total_fetched = 0

            with connection.cursor() as cursor:
                while True:
                    # Add LIMIT and OFFSET to query
                    limited_sql = f"{sql} LIMIT {batch_size} OFFSET {offset}"
                    cursor.execute(limited_sql)
                    rows = cursor.fetchall()

                    if not rows:
                        break

                    # Convert rows to dict format
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    batch_data = [dict(zip(columns, row)) for row in rows]

                    batch_number += 1
                    total_fetched += len(rows)

                    # Check if we have more data
                    has_more = len(rows) == batch_size and (max_rows is None or total_fetched < max_rows)

                    batch_result = {
                        "success": True,
                        "batch": batch_data,
                        "batch_number": batch_number,
                        "batch_size": len(rows),
                        "total_fetched": total_fetched,
                        "has_more": has_more,
                        "fields": columns,
                        "query": sql,
                        "executed_at": self._get_current_timestamp()
                    }

                    yield batch_result

                    # Check if we've reached the limit
                    if not has_more or (max_rows is not None and total_fetched >= max_rows):
                        break

                    offset += batch_size

        finally:
            if connection:
                connection.close()

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.utcnow().isoformat() + "Z"
