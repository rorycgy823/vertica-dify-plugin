from typing import Any

from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError

from tools.execute_query import ExecuteQueryTool
from tools.stream_query import StreamQueryTool
from tools.list_tables import ListTablesTool
from tools.list_views import ListViewsTool
from tools.list_indexes import ListIndexesTool
from tools.get_table_structure import GetTableStructureTool


class VerticaProvider(ToolProvider):
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        try:
            # Test connection with a simple query
            tool = ExecuteQueryTool()
            result = tool.invoke({"sql": "SELECT 1 as test"}, credentials)
            if not result or len(result) == 0:
                raise Exception("Connection test failed")
        except Exception as e:
            raise ToolProviderCredentialValidationError(str(e))
