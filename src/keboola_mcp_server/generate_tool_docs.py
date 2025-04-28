import asyncio
import json
import logging
import re
import sys
from typing import Optional

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server

LOG = logging.getLogger(__name__)


class ToolDocumentationGenerator:
    """Generates documentation for tools."""

    def __init__(self, tools, output_path: str = "TOOLS.md"):
        self.tools = tools
        self.output_path = output_path

    def generate(self):
        with open(self.output_path, 'w') as f:
            self._write_header(f)
            self._write_index(f)
            self._write_tool_details(f)

    def _write_header(self, f):
        f.write("# Tools Documentation\n")
        f.write("This document provides details about the tools available in the MCP server.\n\n")

    def _write_index(self, f):
        f.write("## Index\n")
        for tool in self.tools:
            anchor = self._generate_anchor(tool.name)
            f.write(f"- [{tool.name}](#{anchor})\n")

    def _generate_anchor(self, text: str) -> str:
        """Generate GitHub-style markdown anchor from a header text."""
        anchor = text.lower()
        anchor = re.sub(r'[^\w\s-]', '', anchor)
        anchor = re.sub(r'\s+', '-', anchor)
        return anchor

    def _write_tool_details(self, f):
        for tool in self.tools:
            anchor = self._generate_anchor(tool.name)
            f.write(f'<a name="{anchor}"></a>\n')
            f.write(f"## {tool.name}\n")
            f.write(f"**Description**: {tool.description}\n\n")

            self._write_parameters(f, tool)
            self._write_json_schema(f, tool)
            self._write_return_type(f, tool)
            self._write_parameterized_name(f, tool)

            f.write("\n---\n")

    def _write_parameters(self, f, tool):
        f.write("### Parameters\n")
        if hasattr(tool, 'model_fields'):
            for param, param_info in tool.model_fields.items():
                param_type = getattr(param_info, 'annotation', 'Unknown')
                param_desc = getattr(param_info, 'description', 'No description available.')
                f.write(f"- **{param}**: {param_type} - {param_desc}\n")
        else:
            f.write("No parameters available.\n")

    def _write_json_schema(self, f, tool):
        if hasattr(tool, 'model_json_schema'):
            f.write(f"\n**Input JSON Schema**:\n")
            f.write("```json\n")
            f.write(json.dumps(tool.inputSchema, indent=2))
            f.write("\n```\n")
        else:
            f.write("No JSON schema available for this tool.\n")

    def _write_return_type(self, f, tool):
        return_type = getattr(tool, 'return_type', 'Unknown')
        f.write(f"\n**Return type**: {return_type}\n")

    def _write_parameterized_name(self, f, tool):
        if hasattr(tool, 'model_parameterized_name'):
            f.write(f"\n**Parameterized Name**: {tool.model_parameterized_name}\n")


async def generate_tools_docs(mcp) -> None:
    """Fetch tools and generate their documentation."""
    tools = await mcp.list_tools()
    doc_gen = ToolDocumentationGenerator(tools)
    doc_gen.generate()


async def generate_docs() -> None:
    """Main function to generate docs."""
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        level=logging.INFO,
        stream=sys.stderr,
    )

    config = Config.from_dict(
        {
            'storage_api_url': 'https://connection.keboola.com',
            'log_level': 'INFO',
        }
    )

    try:
        mcp = create_server(config)
        await generate_tools_docs(mcp)
    except Exception as e:
        LOG.exception(f"Failed to generate documentation: {e}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(generate_docs())
