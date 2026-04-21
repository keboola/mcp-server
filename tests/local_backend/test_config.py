"""Tests for config.py — ComponentConfig CRUD."""

import json
from pathlib import Path

import pytest

from keboola_mcp_server.local_backend.config import (
    ComponentConfig,
    delete_config,
    list_configs,
    load_config,
    save_config,
)


@pytest.fixture
def configs_dir(tmp_path: Path) -> Path:
    d = tmp_path / 'configs'
    d.mkdir()
    return d


def _make_config(**kwargs) -> ComponentConfig:
    defaults = dict(
        config_id='test-001',
        component_id='keboola.ex-http',
        name='Test Config',
        parameters={'url': 'https://example.com'},
    )
    defaults.update(kwargs)
    return ComponentConfig(**defaults)


# ---------------------------------------------------------------------------
# save_config
# ---------------------------------------------------------------------------


def test_save_config_creates_file(configs_dir: Path) -> None:
    cfg = _make_config()
    saved = save_config(configs_dir, cfg)

    path = configs_dir / 'test-001.json'
    assert path.exists()
    assert saved.config_id == 'test-001'


def test_save_config_sets_timestamps(configs_dir: Path) -> None:
    cfg = _make_config()
    saved = save_config(configs_dir, cfg)

    assert saved.created_at != ''
    assert saved.updated_at != ''
    assert saved.created_at == saved.updated_at  # first save


def test_save_config_preserves_created_at_on_update(configs_dir: Path) -> None:
    cfg = _make_config()
    first = save_config(configs_dir, cfg)
    # Save again — should keep original created_at
    second = save_config(configs_dir, first)

    assert second.created_at == first.created_at
    assert second.updated_at >= first.updated_at


def test_save_config_roundtrip(configs_dir: Path) -> None:
    cfg = _make_config(
        parameters={'url': 'https://example.com', 'method': 'GET'},
        component_image='keboola/ex-http:1.0',
    )
    save_config(configs_dir, cfg)

    data = json.loads((configs_dir / 'test-001.json').read_text(encoding='utf-8'))
    assert data['config_id'] == 'test-001'
    assert data['parameters']['url'] == 'https://example.com'
    assert data['component_image'] == 'keboola/ex-http:1.0'


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------


def test_load_config_returns_saved(configs_dir: Path) -> None:
    cfg = _make_config()
    save_config(configs_dir, cfg)

    loaded = load_config(configs_dir, 'test-001')
    assert loaded.config_id == 'test-001'
    assert loaded.component_id == 'keboola.ex-http'
    assert loaded.parameters == {'url': 'https://example.com'}


def test_load_config_not_found_raises(configs_dir: Path) -> None:
    with pytest.raises(FileNotFoundError, match='nonexistent'):
        load_config(configs_dir, 'nonexistent')


# ---------------------------------------------------------------------------
# list_configs
# ---------------------------------------------------------------------------


def test_list_configs_empty(configs_dir: Path) -> None:
    assert list_configs(configs_dir) == []


def test_list_configs_returns_all(configs_dir: Path) -> None:
    save_config(configs_dir, _make_config(config_id='cfg-a', name='A'))
    save_config(configs_dir, _make_config(config_id='cfg-b', name='B'))

    configs = list_configs(configs_dir)
    assert len(configs) == 2
    ids = {c.config_id for c in configs}
    assert ids == {'cfg-a', 'cfg-b'}


def test_list_configs_sorted_by_id(configs_dir: Path) -> None:
    for cid in ('zzz', 'aaa', 'mmm'):
        save_config(configs_dir, _make_config(config_id=cid, name=cid))

    ids = [c.config_id for c in list_configs(configs_dir)]
    assert ids == sorted(ids)


def test_list_configs_skips_corrupt_file(configs_dir: Path, caplog) -> None:
    (configs_dir / 'bad.json').write_text('not json', encoding='utf-8')
    save_config(configs_dir, _make_config())

    import logging

    with caplog.at_level(logging.WARNING):
        configs = list_configs(configs_dir)

    assert len(configs) == 1
    assert 'bad.json' in caplog.text


# ---------------------------------------------------------------------------
# delete_config
# ---------------------------------------------------------------------------


def test_delete_config_removes_file(configs_dir: Path) -> None:
    save_config(configs_dir, _make_config())
    assert (configs_dir / 'test-001.json').exists()

    result = delete_config(configs_dir, 'test-001')

    assert result is True
    assert not (configs_dir / 'test-001.json').exists()


def test_delete_config_not_found_returns_false(configs_dir: Path) -> None:
    assert delete_config(configs_dir, 'does-not-exist') is False


# ---------------------------------------------------------------------------
# creates dir if missing
# ---------------------------------------------------------------------------


def test_list_configs_creates_dir_if_missing(tmp_path: Path) -> None:
    missing_dir = tmp_path / 'new-configs'
    assert not missing_dir.exists()

    result = list_configs(missing_dir)

    assert missing_dir.exists()
    assert result == []
