from importlib import resources

PACKAGE = __name__


def load_prompt(name: str) -> str:
    """Load a prompt markdown file bundled in this package.

    Args:
        name: File name inside this package (e.g., "project_system_prompt.md").

    Returns:
        The file content as text.

    Raises:
        FileNotFoundError: If the resource does not exist.
    """
    with resources.files(PACKAGE).joinpath(name).open('r', encoding='utf-8') as f:
        return f.read()


def get_project_system_prompt() -> str:
    """Convenience accessor for the project system prompt text."""
    return load_prompt('project_system_prompt.md')
