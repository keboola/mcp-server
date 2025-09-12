from importlib import resources


def load_prompt(name: str) -> str:
    return resources.read_text(__package__, name)


def get_project_system_prompt() -> str:
    return load_prompt('project_system_prompt.md')
