"""Component configuration persistence for local-backend mode."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ComponentConfig(BaseModel):
    config_id: str = Field(description='Unique identifier used as the filename (no spaces).')
    component_id: str = Field(description='Keboola component ID (e.g. "keboola.ex-http").')
    name: str = Field(description='Human-readable configuration name.')
    parameters: dict = Field(default_factory=dict, description='Component parameters written to config.json.')
    component_image: str | None = Field(
        default=None,
        description='Docker image tag for registry-based execution (alternative to git_url).',
    )
    git_url: str | None = Field(
        default=None,
        description='Git URL for source-based execution (alternative to component_image).',
    )
    created_at: str = Field(default='', description='ISO 8601 creation timestamp.')
    updated_at: str = Field(default='', description='ISO 8601 last-update timestamp.')


class ConfigsOutput(BaseModel):
    configs: list[ComponentConfig] = Field(description='Saved component configurations.')
    total: int = Field(description='Total number of saved configurations.')


# ---------------------------------------------------------------------------
# Config file CRUD
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def list_configs(configs_dir: Path) -> list[ComponentConfig]:
    """Return all saved configs, sorted by config_id. Skips unreadable files."""
    configs_dir.mkdir(parents=True, exist_ok=True)
    results: list[ComponentConfig] = []
    for path in sorted(configs_dir.glob('*.json')):
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            results.append(ComponentConfig(**data))
        except Exception as exc:
            LOG.warning(f'Could not parse config {path.name}: {exc}')
    return results


def save_config(configs_dir: Path, config: ComponentConfig) -> ComponentConfig:
    """Write a config to disk. Sets created_at on first save; always updates updated_at."""
    configs_dir.mkdir(parents=True, exist_ok=True)
    now = _now_iso()
    updates: dict = {'updated_at': now}
    if not config.created_at:
        updates['created_at'] = now
    config = config.model_copy(update=updates)
    path = configs_dir / f'{config.config_id}.json'
    path.write_text(config.model_dump_json(indent=2), encoding='utf-8')
    return config


def load_config(configs_dir: Path, config_id: str) -> ComponentConfig:
    """Load a saved config by ID. Raises FileNotFoundError if not found."""
    path = configs_dir / f'{config_id}.json'
    if not path.exists():
        raise FileNotFoundError(f'Config not found: {config_id!r}')
    data = json.loads(path.read_text(encoding='utf-8'))
    return ComponentConfig(**data)


def delete_config(configs_dir: Path, config_id: str) -> bool:
    """Delete a config file. Returns True if deleted, False if it did not exist."""
    path = configs_dir / f'{config_id}.json'
    if path.exists():
        path.unlink()
        return True
    return False
