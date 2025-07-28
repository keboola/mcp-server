
"""Tests to ensure all MCP tool functions have the required justification parameter."""

import inspect
from typing import get_type_hints

import pytest

from keboola_mcp_server.tools import (
    components,
    doc,
    jobs,
    oauth,
    project,
    search,
    sql,
    storage,
)
from keboola_mcp_server.tools.flow import tools as flow_tools


def get_all_tool_functions():
    """
    Discover all tool functions across all tool modules.

    Returns a list of tuples: (module_name, function_name, function_object)
    """
    tool_modules = [
        ('doc', doc),
        ('jobs', jobs),
        ('oauth', oauth),
        ('project', project),
        ('search', search),
        ('sql', sql),
        ('storage', storage),
        ('components', components),
        ('flow.tools', flow_tools),
    ]

    tool_functions = []

    for module_name, module in tool_modules:
        for attr_name in dir(module):
            attr = getattr(module, attr_name)

            # Check if it's a function and not a private/internal function
            if (
                callable(attr)
                and not attr_name.startswith('_')
                and hasattr(attr, '__module__')
                and attr.__module__ and module_name in attr.__module__
            ):
                # Get the function signature to check for ctx parameter
                try:
                    sig = inspect.signature(attr)
                    # Only include functions that have a 'ctx' or '_ctx' parameter (these are tool functions)
                    if 'ctx' in sig.parameters or '_ctx' in sig.parameters:
                        tool_functions.append((module_name, attr_name, attr))
                except (ValueError, TypeError):
                    # Skip functions we can't inspect
                    continue

    return tool_functions


class TestJustificationParameterEnforcement:
    """Test suite to ensure all tool functions have the required justification parameter."""

    @pytest.mark.parametrize(('module_name', 'func_name', 'func'), get_all_tool_functions())
    def test_tool_has_justification_parameter(self, module_name, func_name, func):
        """Test that each tool function has a justification parameter with correct type and description."""
        sig = inspect.signature(func)

        # Check that justification parameter exists
        assert 'justification' in sig.parameters, (
            f"Tool function {module_name}.{func_name} is missing the required 'justification' parameter"
        )

        justification_param = sig.parameters['justification']

        # Check that justification parameter comes after ctx parameter (or _ctx)
        param_names = list(sig.parameters.keys())
        if 'ctx' in param_names:
            ctx_index = param_names.index('ctx')
        elif '_ctx' in param_names:
            ctx_index = param_names.index('_ctx')
        else:
            raise AssertionError(f"Tool function {module_name}.{func_name} must have 'ctx' or '_ctx' parameter")

        justification_index = param_names.index('justification')

        assert justification_index == ctx_index + 1, (
            f"Tool function {module_name}.{func_name} must have 'justification' parameter "
            f"immediately after 'ctx'/'_ctx' parameter. Current order: {param_names}"
        )

        # Check that justification parameter has no default value (is required)
        assert justification_param.default == inspect.Parameter.empty, (
            f'{module_name}.{func_name} justification parameter must be required (no default value)'
        )

        # Check type annotation
        type_hints = get_type_hints(func, include_extras=True)
        assert 'justification' in type_hints, (
            f'Tool function {module_name}.{func_name} justification parameter must have type annotation'
        )

        justification_type = type_hints['justification']

        # Check that it's Annotated[str, Field(...)]
        assert hasattr(justification_type, '__origin__') or hasattr(justification_type, '__metadata__'), (
            f'Tool function {module_name}.{func_name} justification parameter must use Annotated type'
        )

        # Check if it's an Annotated type (works across different Python/typing versions)
        is_annotated = (
            (hasattr(justification_type, '__origin__') and justification_type.__origin__ is not None) or
            hasattr(justification_type, '__metadata__')
        )
        assert is_annotated, (
            f'Tool function {module_name}.{func_name} justification parameter must use Annotated[str, Field(...)]'
        )

        # Check that the first type argument is str
        if hasattr(justification_type, '__args__') and justification_type.__args__:
            assert justification_type.__args__[0] is str, (
                f'Tool function {module_name}.{func_name} justification parameter must be Annotated[str, ...]'
            )

        # Check that it has a Field annotation with description
        field_found = False
        field_description = None

        # Check args (normal case)
        if hasattr(justification_type, '__args__') and len(justification_type.__args__) > 1:
            for arg in justification_type.__args__[1:]:
                if hasattr(arg, 'description') and hasattr(arg, '__class__') and 'Field' in str(arg.__class__):
                    field_found = True
                    field_description = arg.description
                    break

        # Check metadata (alternative case)
        if not field_found and hasattr(justification_type, '__metadata__'):
            for arg in justification_type.__metadata__:
                if hasattr(arg, 'description') and hasattr(arg, '__class__') and 'Field' in str(arg.__class__):
                    field_found = True
                    field_description = arg.description
                    break

        assert field_found, (
            f'Tool function {module_name}.{func_name} justification parameter must have Field '
            f'annotation with description'
        )

        # Check that Field has description
        assert field_description, (
            f'Tool function {module_name}.{func_name} justification parameter Field must have description'
        )

        # Check that description mentions the expected content
        description = field_description.lower()
        expected_phrases = ['brief explanation', 'tool call']
        for phrase in expected_phrases:
            assert phrase in description, (
                f'{module_name}.{func_name} justification parameter description should mention '
                f'{phrase!r}. Got: {field_description}'
            )

    def test_all_expected_tools_found(self):
        """Test that we found all expected tool functions."""
        tool_functions = get_all_tool_functions()

        # Expected minimum number of tool functions (as of current implementation)
        # This will help catch if tools are accidentally removed or not discovered
        expected_minimum = 33

        actual_count = len(tool_functions)
        assert actual_count >= expected_minimum, (
            f'Expected at least {expected_minimum} tool functions, but found only {actual_count}. '
            f'Found functions: {[(m, f) for m, f, _ in tool_functions]}'
        )

        # Check that all expected modules have at least one tool function
        expected_modules = {
            'doc', 'jobs', 'oauth', 'project', 'search', 'sql', 'storage', 'components', 'flow.tools'
        }
        found_modules = {module_name for module_name, _, _ in tool_functions}

        missing_modules = expected_modules - found_modules
        assert not missing_modules, f'No tool functions found in expected modules: {missing_modules}'

    def test_function_discovery_consistency(self):
        """Test that our function discovery is working consistently."""
        tool_functions = get_all_tool_functions()

        # Check that we found some specific known functions
        known_functions = [
            ('doc', 'docs_query'),
            ('jobs', 'list_jobs'),
            ('oauth', 'create_oauth_url'),
            ('storage', 'get_bucket'),
            ('components', 'create_config'),
        ]

        found_functions = [(module_name, func_name) for module_name, func_name, _ in tool_functions]

        for expected_module, expected_func in known_functions:
            assert (expected_module, expected_func) in found_functions, (
                f'Expected to find {expected_module}.{expected_func} but it was not discovered. '
                f'This might indicate an issue with the function discovery logic.'
            )
