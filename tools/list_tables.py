from typing import Any, Generator

import vertica_python

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class ListTablesTool(Tool):
    """
    Tool for listing all tables in a schema
    """

    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        List all tables in the specified schema
        """
        try:
            # Get credentials from runtime
            credentials = self.runtime.credentials

            # Parse configuration
            config = self._build_config(credentials)

            # Get schema name
            schema_name = tool_parameters.get("schema_name", "public").strip()

            # Execute query
            result = self._list_tables(schema_name, config)

            # Format and return result
            formatted_result = self._format_result(result, schema_name)
            yield self.create_json_message(formatted_result)

        except Exception as e:
            error_result = {
                "success": False,
                "error": str(e),
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

    def _list_tables(self, schema_name: str, config: dict[str, Any]) -> list[dict[str, Any]]:
        """List all tables in the schema"""
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

            # Query for tables
            query = """
                SELECT
                    table_schema,
                    table_name,
                    owner_name,
                    CASE
                        WHEN is_temp_table = 't' THEN 'TEMPORARY'
                        WHEN is_system_table = 't' THEN 'SYSTEM'
                        WHEN is_flextable = 't' THEN 'FLEX'
                        ELSE 'STANDARD'
                    END as table_type
                FROM v_catalog.tables
                WHERE table_schema = ?
                ORDER BY table_name
            """

            with connection.cursor() as cursor:
                cursor.execute(query, [schema_name])
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                return [dict(zip(columns, row)) for row in rows]

        finally:
            if connection:
                connection.close()

    def _format_result(self, tables: list[dict[str, Any]], schema_name: str) -> dict[str, Any]:
        """Format query result for output"""
        return {
            "success": True,
            "schema": schema_name,
            "tables": tables,
            "table_count": len(tables),
            "executed_at": self._get_current_timestamp()
        }

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.utcnow().isoformat() + "Z"
