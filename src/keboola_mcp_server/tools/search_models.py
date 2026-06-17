"""Shared models, type aliases and constants for the `search` tool.

Extracted from `search.py` so the textual (global-search) and the legacy enumeration paths
can both depend on these without importing each other (avoids a circular import between
`search.py` and `search_global.py`).
"""

import json
import logging
import re
from collections import defaultdict
from typing import Any, Iterable, Literal, Mapping, Sequence

import jsonpath_ng
from jsonpath_ng.jsonpath import JSONPath
from pydantic import BaseModel, Field, PrivateAttr, model_validator

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.storage import ItemType as ApiItemType
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components.utils import _normalize_jsonpath

LOG = logging.getLogger(__name__)

MAX_GLOBAL_SEARCH_LIMIT = 100
DEFAULT_GLOBAL_SEARCH_LIMIT = 50

SearchItemType = Literal[
    'bucket',
    'table',
    'data-app',
    'flow',
    'transformation',
    'component',
    'configuration',
    'configuration-row',
    'workspace',
    'shared-code',
    'rows',
    'state',
]


SearchComponentItemType = Literal[
    'flow',
    'transformation',
    'component',
    'configuration',
    'configuration-row',
    'workspace',
]


SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES: Mapping[SearchItemType, Sequence[str]] = {
    'data-app': ['other'],
    'flow': ['other'],
    'transformation': ['transformation'],
    'configuration': ['extractor', 'writer', 'application'],
    'configuration-row': ['extractor', 'writer', 'application'],
    'component': ['extractor', 'writer', 'application'],
    'workspace': ['other'],
}

GLOBAL_SEARCH_FEATURE = 'global-search'
WORKSPACE_COMPONENT_ID = 'keboola.sandboxes'

# Maps the tool's item types to the API types requested from the global-search endpoint. Some tool
# types (data-app, flow, workspace) exist server-side as 'configuration' items distinguished only
# by their component ID, so 'configuration' is over-fetched and narrowed client-side after re-typing.
SEARCH_ITEM_TYPE_TO_API_TYPES: Mapping[SearchItemType, Sequence[ApiItemType]] = {
    'bucket': ('bucket',),
    'table': ('table',),
    'transformation': ('transformation',),
    'configuration': ('configuration',),
    'configuration-row': ('configuration-row',),
    'component': ('configuration', 'configuration-row'),
    'flow': ('flow', 'configuration'),
    'data-app': ('configuration',),
    'workspace': ('workspace', 'configuration'),
    'shared-code': ('shared-code',),
    'rows': ('rows',),
    'state': ('state',),
}

SearchType = Literal['textual', 'config-based']
SearchPatternMode = Literal['regex', 'literal']
SearchBranchScope = Literal['current-branch', 'all-branches']


class PatternMatch(BaseModel):
    scope: str | None
    patterns: list[str]


class SearchHit(BaseModel):
    bucket_id: str | None = Field(default=None, description='The ID of the bucket.')
    table_id: str | None = Field(default=None, description='The ID of the table.')
    component_id: str | None = Field(default=None, description='The ID of the component.')
    configuration_id: str | None = Field(default=None, description='The ID of the configuration.')
    configuration_row_id: str | None = Field(default=None, description='The ID of the configuration row.')

    item_type: SearchItemType = Field(description='The type of the item (e.g. table, bucket, configuration, etc.).')
    updated: str = Field(
        description='The date and time the item was last updated (or created, when the update time is not '
        'available) in ISO 8601 format.'
    )

    name: str | None = Field(default=None, description='Name of the item.')
    display_name: str | None = Field(default=None, description='Display name of the item.')
    description: str | None = Field(default=None, description='Description of the item.')
    branch_id: str | None = Field(
        default=None, description='ID of the branch the item belongs to, when reported by the search backend.'
    )
    branch_name: str | None = Field(
        default=None, description='Name of the branch the item belongs to, when reported by the search backend.'
    )
    matches: list[PatternMatch] = Field(
        default_factory=list,
        description='Most specific JSONPath scopes with grouped matched patterns (config-based search only).',
    )
    links: list[Link] = Field(default_factory=list, description='Links to the item.')

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SearchHit):
            return self.model_dump() == other.model_dump()
        return False

    @model_validator(mode='after')
    def check_id_fields(self) -> 'SearchHit':
        id_fields = [
            self.bucket_id,
            self.table_id,
            self.component_id,
            self.configuration_id,
            self.configuration_row_id,
        ]

        if not any(field for field in id_fields if field):
            raise ValueError('At least one ID field must be filled.')

        if self.configuration_row_id and not all([self.component_id, self.configuration_id]):
            raise ValueError(
                'If configuration_row_id is filled, ' 'both component_id and configuration_id must be filled.'
            )

        if self.configuration_id and not self.component_id:
            raise ValueError('If configuration_id is filled, component_id must be filled.')

        return self

    def set_matches(self, matches: list['PatternMatch']) -> 'SearchHit':
        """Assign pattern matches to this search hit and return self for chaining."""
        patterns_by_scope: dict[str, set[str]] = defaultdict(set)
        for match in matches:
            if not match.scope:
                continue
            patterns_by_scope[match.scope].update(match.patterns)

        unique_scopes = list(patterns_by_scope)
        most_specific_scopes = [
            scope
            for scope in unique_scopes
            if not any(
                other.startswith(scope) and len(other) > len(scope) and other[len(scope)] in ('.', '[')
                for other in unique_scopes
            )
        ]
        self.matches = [
            PatternMatch(scope=scope, patterns=sorted(patterns_by_scope[scope])) for scope in most_specific_scopes
        ]
        return self


class SearchOutput(BaseModel):
    """Paginated search results with total counts."""

    hits: list[SearchHit] = Field(description='The matching items (paginated).')
    total: int = Field(
        description='Approximate total number of matching items before pagination; treat it as an upper bound on '
        'the items reachable via pagination. With multiple patterns, an item matching more than one pattern is '
        'counted once per pattern; for textual search the count may also include items later removed by client-side '
        'type narrowing (e.g. configurations re-typed to data-apps/flows/workspaces).'
    )
    by_type: dict[str, int] = Field(
        default_factory=dict,
        description='Number of matching items per item type (before pagination and client-side narrowing).',
    )
    branch_scope: SearchBranchScope = Field(
        default='current-branch',
        description="Branch scope the hits come from. 'all-branches' means nothing was found in the current "
        "branch context and the search was widened to the whole project; check each hit's branch_id/branch_name "
        'to see where it lives.',
    )


class SearchSpec(BaseModel):
    patterns: Sequence[str]
    item_types: Sequence[SearchItemType]
    pattern_mode: SearchPatternMode = 'regex'
    case_sensitive: bool = False
    search_scopes: Sequence[str] = tuple()
    search_type: SearchType = 'textual'
    return_all_matched_patterns: bool = False

    _component_types: Sequence[str] = PrivateAttr(default_factory=tuple)
    _compiled_patterns: list[re.Pattern] = PrivateAttr(default_factory=list)
    _clean_patterns: list[str] = PrivateAttr(default_factory=list)
    _all_nodes_expr: JSONPath | None = PrivateAttr(default=None)
    # Tuple fields: (original_scope, parsed_scope_expr, parsed_descendants_expr)
    _scope_exprs: list[tuple[str, JSONPath, JSONPath]] = PrivateAttr(default_factory=list)

    @model_validator(mode='after')
    def _compile_patterns(self) -> 'SearchSpec':
        cleaned_patterns = [str(item).strip() for item in self.patterns if item is not None and str(item).strip()]
        if not cleaned_patterns:
            raise ValueError('At least one search pattern must be provided.')

        self.patterns = cleaned_patterns
        flags = 0 if self.case_sensitive else re.IGNORECASE
        if self.pattern_mode == 'literal':
            self._compiled_patterns = [re.compile(re.escape(pattern), flags) for pattern in cleaned_patterns]
        else:
            self._compiled_patterns = [re.compile(pattern, flags) for pattern in cleaned_patterns]

        self._clean_patterns = cleaned_patterns
        return self

    @model_validator(mode='after')
    def _validate_component_args(self) -> 'SearchSpec':
        if not self._component_types:
            self._component_types = list(
                set(
                    component_type
                    for item in self.item_types
                    for component_type in SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES.get(item, [])
                )
            )
        return self

    @model_validator(mode='after')
    def _validate_item_types(self) -> 'SearchSpec':
        if 'component' in self.item_types:
            self.item_types = list({*self.item_types, 'configuration', 'configuration-row'})
        return self

    @model_validator(mode='after')
    def _compile_jsonpath_exprs(self) -> 'SearchSpec':
        # Compile commonly used expressions once per SearchSpec instance.
        self._all_nodes_expr = jsonpath_ng.parse('$..*')
        self._scope_exprs = []
        for scope in self.search_scopes:
            normalized = _normalize_jsonpath(scope if scope.startswith('$') else f'$.{scope}')
            try:
                self._scope_exprs.append((scope, jsonpath_ng.parse(normalized), jsonpath_ng.parse(f'{normalized}..*')))
            except Exception as e:
                LOG.warning(f'Invalid JSONPath scope "{scope}": {e}')
        return self

    @staticmethod
    def _stringify(value: Any) -> str:
        try:
            return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(value)

    def match_patterns(self, value: str | JsonDict | None) -> list[str]:
        """
        Matches a string or dictionary value against the patterns.

        :param value: The value to match against the patterns.
        :return: A list of patterns that matched the value; empty list if no matches.
        """
        if value is None:
            return []
        haystack = value if isinstance(value, str) else self._stringify(value)
        if not haystack:
            return []

        matches: list[str] = []
        for pattern, compiled in zip(self._clean_patterns, self._compiled_patterns):
            if compiled.search(haystack):
                matches.append(pattern)
                if not self.return_all_matched_patterns:
                    break

        return matches

    def _find_matches_for_expr(
        self, configuration: JsonDict, parsed_expr: JSONPath, scalar_only: bool = False
    ) -> list[PatternMatch]:
        """Find pattern matches on JSON nodes matched by a JSONPath expression. If scalar_only is True, only scalar
        nodes are matched."""
        matches: list[PatternMatch] = []
        for jpath_match in parsed_expr.find(configuration):
            value = jpath_match.value
            if scalar_only and isinstance(value, (dict, list)):
                continue
            if matched := self.match_patterns(value):
                matches.append(
                    PatternMatch(
                        scope=_clean_jsonpath_path_str(str(jpath_match.full_path)),
                        patterns=matched,
                    )
                )
                if not self.return_all_matched_patterns:
                    return matches
        return matches

    def match_configuration_scopes(self, configuration: JsonDict | None) -> list[PatternMatch]:
        """
        Checks configuration fields within specified JSONPath scopes for pattern matches.
        Walks matching nodes within each scope and returns the exact path where the match
        was found. When no scopes are specified, walks the entire configuration.

        :param configuration: The configuration to match against the patterns.
        :return: List of PatternMatch with matching JSONPath scopes; empty list if no matches.
        """
        if configuration is None:
            return []

        if self.search_scopes:
            all_matches: list[PatternMatch] = []
            # Deduplicate hits when scopes overlap (e.g. "parameters" + "parameters.query")
            # or the same logical scope is provided multiple times.
            seen: set[str | None] = set()
            for _scope, self_expr, desc_expr in self._scope_exprs:
                # Search in self expression node for scalar matches first
                self_matches = self._find_matches_for_expr(configuration, self_expr, scalar_only=True)
                # If no scalar matches, search in descendants nodes
                desc_matches: list[PatternMatch] = []
                if not self_matches:
                    desc_matches = self._find_matches_for_expr(configuration, desc_expr)
                for match in self_matches or desc_matches:
                    if match.scope in seen:
                        continue
                    seen.add(match.scope)
                    all_matches.append(match)
                    if not self.return_all_matched_patterns:
                        return all_matches
            return all_matches
        else:
            # No scope provided – search all descendants and return exact match paths.
            return self._find_matches_for_expr(configuration, self._all_nodes_expr)

    def match_texts(self, texts: Iterable[str]) -> list[PatternMatch]:
        """
        Matches a sequence of strings against the patterns.

        :param texts: The sequence of strings to match against the patterns.
        :return: A list of PatternMatch objects.
        """
        matches: list[PatternMatch] = []
        for text in texts:
            if matched := self.match_patterns(text):
                matches.append(PatternMatch(scope=None, patterns=matched))
                if not self.return_all_matched_patterns:
                    break
        return matches


def _clean_jsonpath_path_str(path_str: str) -> str:
    """Normalize a jsonpath_ng full_path string across library versions.

    jsonpath_ng >= 1.8.0 wraps Child nodes in parentheses and single-quotes field names
    with special characters, e.g. "(authorization.'#apiKey')" instead of "authorization.#apiKey".
    """
    # Strip parentheses added by jsonpath_ng >= 1.8.0
    result = path_str.replace('(', '').replace(')', '')
    # Remove surrounding quotes from field name segments, e.g. "'#apiKey'" -> "#apiKey"
    result = re.sub(r"['\"]([^'\"]+)['\"]", r'\1', result)
    # Normalize .[N] -> [N]
    return re.sub(r'\.\[', '[', result)
