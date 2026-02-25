from typing import Any

from keboola_mcp_server.tools.components.model import (
    Component,
    ComponentConfigurationResponseBase,
    ComponentSummary,
    ComponentType,
    ComponentWithConfigs,
)

__all__ = [
    'Component',
    'ComponentConfigurationResponseBase',
    'ComponentSummary',
    'ComponentType',
    'ComponentWithConfigs',
    'add_component_tools',
    'add_config_row',
    'create_config',
    'create_sql_transformation',
    'get_components',
    'get_config_examples',
    'get_configs',
    'update_config',
    'update_config_row',
    'update_sql_transformation',
]

_TOOLS_EXPORTS = {
    'add_component_tools',
    'add_config_row',
    'create_config',
    'create_sql_transformation',
    'get_components',
    'get_config_examples',
    'get_configs',
    'update_config',
    'update_config_row',
    'update_sql_transformation',
}


def __getattr__(name: str) -> Any:
    """Lazily expose component tools to avoid importing tools.py during package import."""
    if name in _TOOLS_EXPORTS:
        from keboola_mcp_server.tools.components import tools as _tools

        return getattr(_tools, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
