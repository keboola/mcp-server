import asyncio
import json
import logging
import re
import sys
from collections import defaultdict
from operator import attrgetter
from typing import Iterable, Mapping

from mcp.types import Tool

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server

LOG = logging.getLogger(__name__)


class ToolCategory:
    """Encapsulates rules for categorizing tools based on their name."""

    def __init__(self, name: str, pattern: re.Pattern):
        self.name = name
        self._pattern = pattern

    def matches(self, tool_name: str) -> bool:
        """Checks if the tool name matches any of the categorization rules."""
        return self._pattern.search(tool_name) is not None


class ToolCategorizer:
    """Handles categorizing tools based on defined rules."""

    def __init__(self):
        self._categories: list[ToolCategory] = []

    def add_category(self, category: ToolCategory):
        self._categories.append(category)

    def get_tool_category(self, tool_name: str) -> ToolCategory:
        """Categorize a tool based on its name."""
        for category in self._categories:
            if category.matches(tool_name):
                return category
        raise ValueError(f'Tool {tool_name} does not match any category.')

    def get_categories(self) -> Iterable[ToolCategory]:
        yield from self._categories


class ToolDocumentationGenerator:
    """Generates documentation for tools."""

    def __init__(self, tools: list[Tool], categorizer: ToolCategorizer, output_path: str = 'TOOLS.md'):
        self._tools = tools
        self._categorizer = categorizer
        self._output_path = output_path

    def generate(self):
        with open(self._output_path, mode='w', encoding='utf-8') as f:
            self._write_header(f)
            self._write_index(f)
            self._write_tool_details(f)

    def _group_tools(self) -> Mapping[ToolCategory, list[Tool]]:
        tools_by_category: dict[ToolCategory, list[Tool]] = defaultdict(list)
        for tool in self._tools:
            category = self._categorizer.get_tool_category(tool.name)
            tools_by_category[category].append(tool)
        return tools_by_category

    def _write_header(self, f):
        f.write('# Tools Documentation\n')
        f.write('This document provides details about the tools available in the Keboola MCP server.\n\n')

    def _write_index(self, f):
        f.write('## Index\n')
        tools_by_category = self._group_tools()
        for category in self._categorizer.get_categories():
            if not (tools := tools_by_category[category]):
                continue

            f.write(f'\n### {category.name}\n')
            for tool in sorted(tools, key=attrgetter('name')):
                anchor = self._generate_anchor(tool.name)
                first_sentence = self._get_first_sentence(tool.description)
                f.write(f'- [{tool.name}](#{anchor}): {first_sentence}\n')
        f.write('\n---\n')

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
        tools_by_category = self._group_tools()
        for category in self._categorizer.get_categories():
            if not (tools := tools_by_category[category]):
                continue

            f.write(f'\n# {category.name}\n')
            for tool in sorted(tools, key=attrgetter('name')):
                anchor = self._generate_anchor(tool.name)
                f.write(f'<a name="{anchor}"></a>\n')
                f.write(f'## {tool.name}\n')
                f.write(f'**Description**:\n\n{tool.description}\n\n')
                self._write_json_schema(f, tool)
                f.write('\n---\n')

    def _write_json_schema(self, f, tool):
        if hasattr(tool, 'model_json_schema'):
            f.write('\n**Input JSON Schema**:\n')
            f.write('```json\n')
            f.write(json.dumps(tool.inputSchema, indent=2))
            f.write('\n```\n')
        else:
            f.write('No JSON schema available for this tool.\n')


def setup_tool_categorizer():
    """Set up categories for tool categorization."""
    categorizer = ToolCategorizer()

    categorizer.add_category(
        ToolCategory('Storage Tools', re.compile(r'(bucket_|buckets|table_|tables|column_|columns)', re.IGNORECASE))
    )
    categorizer.add_category(ToolCategory('SQL Tools', re.compile(r'(dialect|query_)', re.IGNORECASE)))
    categorizer.add_category(ToolCategory('Component Tools', re.compile(r'(component|transformation)')))
    categorizer.add_category(ToolCategory('Jobs Tools', re.compile(r'job', re.IGNORECASE)))
    categorizer.add_category(ToolCategory('Documentation Tools', re.compile(r'docs', re.IGNORECASE)))
    categorizer.add_category(
        ToolCategory('Other Tools', re.compile(r'.+', re.IGNORECASE))
    )  # catch-all category should be last

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
