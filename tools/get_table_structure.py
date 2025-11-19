from typing import Any, Generator

import vertica_python

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class GetTableStructureTool(Tool):
    """
    Tool for getting detailed table structure information
    """

    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        Get detailed structure information for the specified table
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

            # Get table structure
            result = self._get_table_structure(table_name, schema_name, config)

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

    def _get_table_structure(self, table_name: str, schema_name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Get detailed table structure information"""
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

            # Get column information
            columns_query = """
                SELECT
                    column_name,
                    data_type,
                    CASE WHEN is_nullable = 'YES' THEN true ELSE false END as is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position
                FROM v_catalog.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """

            # Get table metadata
            table_query = """
                SELECT
                    owner_name,
                    CASE
                        WHEN is_temp_table = 't' THEN 'TEMPORARY'
                        WHEN is_system_table = 't' THEN 'SYSTEM'
                        WHEN is_flextable = 't' THEN 'FLEX'
                        ELSE 'STANDARD'
                    END as table_type
                FROM v_catalog.tables
                WHERE table_schema = ? AND table_name = ?
            """

            with connection.cursor() as cursor:
                # Get columns
                cursor.execute(columns_query, [schema_name, table_name])
                columns_rows = cursor.fetchall()
                columns_columns = [desc[0] for desc in cursor.description] if cursor.description else []
                columns = [dict(zip(columns_columns, row)) for row in columns_rows]

                # Get table info
                cursor.execute(table_query, [schema_name, table_name])
                table_rows = cursor.fetchall()

                if not table_rows:
                    raise ValueError(f"Table {schema_name}.{table_name} not found")

                table_info = dict(zip([desc[0] for desc in cursor.description], table_rows[0]))

                return {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "columns": columns,
                    "table_type": table_info.get("table_type", "STANDARD"),
                    "owner": table_info.get("owner_name", ""),
                    "column_count": len(columns)
                }

        finally:
            if connection:
                connection.close()

    def _format_result(self, structure: dict[str, Any], table_name: str, schema_name: str) -> dict[str, Any]:
        """Format table structure result for output"""
        return {
            "success": True,
            "table": table_name,
            "schema": schema_name,
            "structure": structure,
            "executed_at": self._get_current_timestamp()
        }

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.utcnow().isoformat() + "Z"
