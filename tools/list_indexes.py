from typing import Any, Generator

import vertica_python

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class ListIndexesTool(Tool):
    """
    Tool for listing projections (Vertica's equivalent of indexes) for a table
    """

    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        List projections for the specified table
        """
        try:
            # Get credentials from runtime
            credentials = self.runtime.credentials

            # Parse configuration
            config = self._build_config(credentials)

            # Get parameters
            table_name = tool_parameters.get("table_name", "").strip()
            schema_name = tool_parameters.get("schema_name", "public").strip()

            if not table_name:
                yield self.create_text_message("Error: table_name is required")
                return

            # Execute query
            result = self._list_indexes(table_name, schema_name, config)

            # Format and return result
            formatted_result = self._format_result(result, table_name, schema_name)
            yield self.create_json_message(formatted_result)

        except Exception as e:
            error_result = {
                "success": False,
                "error": str(e),
                "table": tool_parameters.get("table_name", ""),
                "schema": tool_parameters.get("schema_name", "public"),
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
            "query_timeout": int(credentials.get("vertica_query_timeout", 60000)),
            "ssl": credentials.get("vertica_ssl", "false").lower() == "true",
        }

    def _list_indexes(self, table_name: str, schema_name: str, config: dict[str, Any]) -> list[dict[str, Any]]:
        """List projections for the table"""
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

            # Query for projections (Vertica's equivalent of indexes)
            query = """
                SELECT
                    p.projection_name as index_name,
                    p.anchor_table_name as table_name,
                    pc.projection_column_name as column_name,
                    CASE WHEN p.is_key_constraint_projection THEN true ELSE false END as is_unique,
                    pc.sort_position as ordinal_position,
                    'projection' as index_type
                FROM v_catalog.projection_columns pc
                JOIN v_catalog.projections p ON pc.projection_id = p.projection_id
                WHERE p.projection_schema = ? AND p.anchor_table_name = ?
                ORDER BY p.projection_name, pc.sort_position
            """

            with connection.cursor() as cursor:
                cursor.execute(query, [schema_name, table_name])
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                return [dict(zip(columns, row)) for row in rows]

        finally:
            if connection:
                connection.close()

    def _format_result(self, indexes: list[dict[str, Any]], table_name: str, schema_name: str) -> dict[str, Any]:
        """Format query result for output"""
        return {
            "success": True,
            "table": table_name,
            "schema": schema_name,
            "indexes": indexes,
            "index_count": len(indexes),
            "executed_at": self._get_current_timestamp()
        }

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.utcnow().isoformat() + "Z"
