import asyncio
import json
import logging
import re
import sys
from collections import defaultdict

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server

LOG = logging.getLogger(__name__)


class ToolCategory:
    """Encapsulates rules for categorizing tools based on their name."""

    def __init__(self, name: str, keywords: list):
        self.name = name
        self.keywords = keywords

    def matches(self, tool_name: str) -> bool:
        """Checks if the tool name matches any of the categorization rules."""
        if any(keyword.lower() in tool_name.lower() for keyword in self.keywords):
            return True
        return False


class ToolCategorizer:
    """Handles categorizing tools based on defined rules."""

    def __init__(self):
        self.categories = []

    def add_category(self, category: ToolCategory):
        self.categories.append(category)

    def categorize_tool(self, tool_name: str) -> str:
        """Categorize a tool based on its name."""
        for category in self.categories:
            if category.matches(tool_name):
                return category.name
        return 'Other'


class ToolDocumentationGenerator:
    """Generates documentation for tools."""

    def __init__(self, tools, categorizer, output_path: str = 'TOOLS.md'):
        self.tools = tools
        self.categorizer = categorizer
        self.output_path = output_path

    def generate(self):
        with open(self.output_path, 'w') as f:
            self._write_header(f)
            self._write_index(f)
            self._write_tool_details(f)

    def _group_tools(self):
        grouped = defaultdict(list)
        for tool in self.tools:
            category = self.categorizer.categorize_tool(tool.name)
            grouped[category].append(tool)
        return grouped

    def _write_header(self, f):
        f.write('# Tools Documentation\n')
        f.write(
            'This document provides details about the tools available in the Keboola MCP server.\n\n'
        )

    def _write_index(self, f):
        f.write("## Index\n")
        grouped = self._group_tools()
        for group in sorted(grouped):
            f.write(f"\n### {group}\n")
            for tool in grouped[group]:
                anchor = self._generate_anchor(tool.name)
                first_sentence = self._get_first_sentence(tool.description)
                f.write(f"- [{tool.name}](#{anchor}): {first_sentence}\n")
        f.write("\n---\n")

    def _get_first_sentence(self, text: str) -> str:
        """Extracts the first sentence from the given text."""
        if not text:
            return 'No description available.'
        first_sentence = text.split('.')[0] + '.'
        return first_sentence.strip()

    def _generate_anchor(self, text: str) -> str:
        """Generate GitHub-style markdown anchor from a header text."""
        anchor = text.lower()
        anchor = re.sub(r'[^\w\s-]', '', anchor)
        anchor = re.sub(r'\s+', '-', anchor)
        return anchor

    def _write_tool_details(self, f):
        grouped = self._group_tools()
        for group in sorted(grouped):
            f.write(f'\n# {group} Tools\n')
            for tool in grouped[group]:
                anchor = self._generate_anchor(tool.name)
                f.write(f'<a name="{anchor}"></a>\n')
                f.write(f'## {tool.name}\n')
                f.write(f'**Description**:\n\n{tool.description}\n\n')
                self._write_json_schema(f, tool)
                f.write('\n---\n')

    def _write_json_schema(self, f, tool):
        if hasattr(tool, 'model_json_schema'):
            f.write(f'\n**Input JSON Schema**:\n')
            f.write('```json\n')
            f.write(json.dumps(tool.inputSchema, indent=2))
            f.write('\n```\n')
        else:
            f.write('No JSON schema available for this tool.\n')


def setup_tool_categorizer():
    """Set up categories for tool categorization."""
    categorizer = ToolCategorizer()

    storage_category = ToolCategory(
        name='Storage Tools', keywords=['bucket_', 'buckets', 'table_', 'tables']
    )
    sql_category = ToolCategory(name='SQL Tools', keywords=['dialect', 'query_'])
    jobs_category = ToolCategory(name='Jobs Tools', keywords=['job'])
    docs_category = ToolCategory(name='Documentation Tools', keywords=['docs'])
    components_category = ToolCategory(
        name='Component Tools', keywords=['component', 'transformation']
    )

    categorizer.add_category(storage_category)
    categorizer.add_category(sql_category)
    categorizer.add_category(jobs_category)
    categorizer.add_category(docs_category)
    categorizer.add_category(components_category)

    return categorizer


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
        tools = await mcp.list_tools()
        categorizer = setup_tool_categorizer()
        doc_gen = ToolDocumentationGenerator(tools, categorizer)
        doc_gen.generate()
    except Exception as e:
        LOG.exception(f'Failed to generate documentation: {e}')
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(generate_docs())
