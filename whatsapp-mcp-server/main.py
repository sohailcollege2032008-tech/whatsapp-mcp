"""WhatsApp MCP server over stdio. Tools live in wa_tools.py (shared with http_server.py)."""
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

import wa_tools

mcp = FastMCP("whatsapp")


def annotations(kind: str) -> ToolAnnotations:
    return ToolAnnotations(readOnlyHint=kind == "read", destructiveHint=kind == "destructive", openWorldHint=True)


wa_tools.register(mcp, annotations)

if __name__ == "__main__":
    mcp.run(transport="stdio")
