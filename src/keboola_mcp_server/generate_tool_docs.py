import asyncio
import json
import logging
import re
import sys
from collections import defaultdict
from operator import attrgetter
from typing import Iterable, Mapping, Optional

from fastmcp import FastMCP
from fastmcp.tools import Tool
from mcp.types import ToolAnnotations

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components.tools import COMPONENT_TOOLS_TAG
from keboola_mcp_server.tools.doc import DOC_TOOLS_TAG
from keboola_mcp_server.tools.flow.tools import FLOW_TOOLS_TAG
from keboola_mcp_server.tools.jobs import JOB_TOOLS_TAG
from keboola_mcp_server.tools.oauth import OAUTH_TOOLS_TAG
from keboola_mcp_server.tools.project import PROJECT_TOOLS_TAG
from keboola_mcp_server.tools.search import SEARCH_TOOLS_TAG
from keboola_mcp_server.tools.sql import SQL_TOOLS_TAG
from keboola_mcp_server.tools.storage import STORAGE_TOOLS_TAG

LOG = logging.getLogger(__name__)


class ToolCategory:
    """Encapsulates rules for categorizing tools based on their name."""

    def __init__(self, name: str, tag: str):
        self.name = name
        self.tag = tag

    def matches(self, tool_tag: str | Iterable[str]) -> bool:
        """Checks if the category matches the tool tag, extended to check existance in the lists of tags."""
        tool_tags = [tool_tag] if isinstance(tool_tag, str) else tool_tag
        return self.tag in tool_tags

    def __str__(self):
        return self.name


OTHER_CATEGORY = ToolCategory('Other Tools', 'other')


class ToolDocumentationGenerator:
    """Generates documentation for tools."""

    def __init__(self, tools: list[Tool], categories: list[ToolCategory], output_path: str = 'TOOLS.md'):
        self._tools = tools
        self._categories = categories
        self._output_path = output_path
        self._categorizer = None

    def generate(self):
        self._categorizer = self._group_tools(self._categories)
        with open(self._output_path, mode='w', encoding='utf-8') as f:
            self._write_header(f)
            self._write_index(f, self._categorizer)
            self._write_tool_details(f, self._categorizer)

    def _group_tools(self, categories: list[ToolCategory]) -> Mapping[ToolCategory, list[Tool]]:
        assert categories, 'Categories are required'
        tools_by_category: dict[ToolCategory, list[Tool]] = defaultdict(list)
        for tool in self._tools:
            has_category = False
            for category in categories:
                if category.matches(list(tool.tags)):
                    # We assume that the category we search for is unique per tool tags and exclusive to other
                    # categories
                    if not has_category:
                        has_category = True
                        LOG.info(f'Tool {tool.name} has category: {category}')
                        tools_by_category[category].append(tool)
                    else:
                        LOG.warning(f'Tool {tool.name} has multiple main mutually exclusive categories: {tool.tags}')
            if not has_category:
                LOG.info(f'Tool {tool.name} has no category, adding to: {OTHER_CATEGORY}')
                tools_by_category[OTHER_CATEGORY].append(tool)
        return tools_by_category

    def _write_header(self, f):
        LOG.info(f'Writing header to {self._output_path}')
        f.write('# Tools Documentation\n')
        f.write('This document provides details about the tools available in the Keboola MCP server.\n\n')

    def _write_index(self, f, categorizer: Mapping[ToolCategory, list[Tool]]):
        LOG.info(f'Writing index to {self._output_path}')
        f.write('## Index\n')
        for category in sorted(categorizer, key=attrgetter('name')):
            if tools := categorizer[category]:
                LOG.info(f'Writing category {category} and its tools ({len(tools)}) to {self._output_path}')

                f.write(f'\n### {category}\n')
                for tool in sorted(tools, key=attrgetter('name')):
                    anchor = self._generate_anchor(tool.name)
                    first_sentence = self._get_first_sentence(tool.description)
                    f.write(f'- [{tool.name}](#{anchor}): {first_sentence}\n')
            else:
                LOG.warning(f'Category {category} has no tools')
        f.write('\n---\n')

    def _get_annotations(self, annotations: Optional[ToolAnnotations]) -> str:
        if annotations is None:
            return ''
        str_annotations = []
        if annotations.readOnlyHint:
            str_annotations.append('read-only')
        if annotations.destructiveHint:
            str_annotations.append('destructive')
        if annotations.idempotentHint:
            str_annotations.append('idempotent')
        return f'`{", ".join(sorted(str_annotations))}`' if str_annotations else ''

    def _get_tags(self, tags: set[str]) -> str:
        return f'`{", ".join(sorted(tags))}`' if tags else ''

    def _get_first_sentence(self, text: Optional[str]) -> str:
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

    def _write_tool_details(self, f, categorizer: Mapping[ToolCategory, list[Tool]]):
        LOG.info(f'Writing tool details to {self._output_path}')
        for category in categorizer:
            if not (tools := categorizer[category]):
                LOG.warning(f'Category {category} has no tools')
                continue

            f.write(f'\n# {category.name}\n')
            for tool in sorted(tools, key=attrgetter('name')):
                anchor = self._generate_anchor(tool.name)
                f.write(f'<a name="{anchor}"></a>\n')
                f.write(f'## {tool.name}\n')
                annotations = self._get_annotations(tool.annotations)
                f.write(f'**Annotations**: {annotations}\n\n')
                tags = self._get_tags(tool.tags)
                f.write(f'**Tags**: {tags}\n\n')
                f.write(f'**Description**:\n\n{tool.description}\n\n')
                self._write_json_schema(f, tool)
                f.write('\n---\n')

    def _write_json_schema(self, f, tool):
        if hasattr(tool, 'model_json_schema'):
            f.write('\n**Input JSON Schema**:\n')
            f.write('```json\n')
            f.write(json.dumps(tool.parameters, indent=2))
            f.write('\n```\n')
        else:
            f.write('No JSON schema available for this tool.\n')


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
        mcp = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(mcp, FastMCP)
        tools = await mcp.get_tools()
        categories = [
            ToolCategory('Storage Tools', STORAGE_TOOLS_TAG),
            ToolCategory('SQL Tools', SQL_TOOLS_TAG),
            ToolCategory('Component Tools', COMPONENT_TOOLS_TAG),
            ToolCategory('Flow Tools', FLOW_TOOLS_TAG),
            ToolCategory('Jobs Tools', JOB_TOOLS_TAG),
            ToolCategory('Documentation Tools', DOC_TOOLS_TAG),
            ToolCategory('Search Tools', SEARCH_TOOLS_TAG),
            ToolCategory('OAuth Tools', OAUTH_TOOLS_TAG),
            ToolCategory('Project Tools', PROJECT_TOOLS_TAG),
            # OTHER_CATEGORY
        ]
        doc_gen = ToolDocumentationGenerator(list(tools.values()), categories)
        doc_gen.generate()
    except Exception as e:
        LOG.exception(f'Failed to generate documentation: {e}')
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(generate_docs())
