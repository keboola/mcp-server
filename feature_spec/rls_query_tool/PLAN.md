# RLS Query Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `query_data_rls` MCP tool that rewrites a user's SELECT so every table is replaced by a filtered subquery taken from a YAML rules file, and make the server register *only* that tool (never the unrestricted `query_data`) when the rules file is configured.

**Architecture:** A new pure module `keboola_mcp_server/rls.py` owns rules loading/validation and the sqlglot AST rewrite (no I/O beyond reading the YAML at startup). `tools/sql.py` gains a thin tool that calls the rewrite and reuses the existing query-execution path. `Config`/CLI gain one deployment-level field for the rules path; `create_server()` loads the rules into `ServerState` and picks which SQL tool to register.

**Tech Stack:** Python 3.10, FastMCP, `sqlglot ~= 30.0` (already a dependency), `pyyaml ~= 6.0` (already a dependency), pytest + pytest-asyncio + pytest-mock.

**Spec:** `feature_spec/rls_query_tool/RFC.md`

## Global Constraints

- Work on branch `feat/rls-query-tool`; open a **draft** PR; do **not** merge.
- No new dependencies. No hardcoded paths/defaults: the rules path comes only from `KBC_RLS_RULES_PATH` env or `--rls-rules-path` CLI, never from an HTTP header.
- Fail-closed everywhere: any load/parse/lookup failure raises; no SQL reaches the workspace unrewritten.
- `query_data` behaviour is unchanged when no rules path is configured (`tests/test_server.py::TestServer::test_list_tools` must keep passing untouched).
- Code, comments, docstrings, commit messages in English. No emoji.
- Style: ruff (`tox -m cs-fix` auto-fixes). Single quotes, 120-column lines (see existing code). Parametrized tests declare names as a tuple `('a', 'b')`.
- Tests run with `.venv/bin/python -m pytest <path> -q -p no:cacheprovider`. Pre-existing failure on clean `main`: `tests/test_server.py::test_json_logging` (`FileNotFoundError`, environment-specific) — ignore it.
- Commit messages: `RLS: <what>` (no Linear issue assigned yet; no Co-Authored-By trailer).
- Version bump: `1.79.2` → `1.80.0` (new tool = minor) in Task 5 only, followed by `uv lock`.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `src/keboola_mcp_server/rls.py` (new) | `RlsRules` (load + validate YAML, case-insensitive lookup), `RlsError`, `rewrite_query()` (sqlglot AST rewrite, fail-closed) |
| `tests/test_rls.py` (new) | Unit tests for loading, lookup and rewrite |
| `src/keboola_mcp_server/config.py` | New field `rls_rules_path` (deployment-level, not header-eligible) |
| `src/keboola_mcp_server/cli.py` | `--rls-rules-path` flag wired into `Config(...)` |
| `tests/test_config.py` | Field reachable from env/CLI, unreachable from headers |
| `src/keboola_mcp_server/tools/sql.py` | Shared `_execute_and_serialize()` helper, `RlsQueryDataOutput`, `query_data_rls` tool, `add_sql_tools(mcp, rls_rules=None)` swap |
| `tests/tools/test_sql.py` | Tool tests for `query_data_rls` |
| `src/keboola_mcp_server/mcp.py` | `ServerState.rls_rules: RlsRules \| None` |
| `src/keboola_mcp_server/server.py` | Load rules in `create_server()` when configured; pass to `add_sql_tools` |
| `tests/test_server.py` | Swap test: exactly one of the two SQL tools is registered |
| `README.md` | "Row-Level Security (pilot)" section |
| `pyproject.toml`, `uv.lock` | Version bump |

**Parallelism:** Tasks 1 and 2 are independent — run them in parallel. Tasks 3 and 4 both depend on Task 1's interface (and Task 4 on Task 2's field); they can run in parallel with each other once 1 and 2 are merged into the branch. Task 5 is sequential at the end.

---

### Task 1: `rls.py` — rules loading and SQL rewrite

**Files:**
- Create: `src/keboola_mcp_server/rls.py`
- Create: `tests/test_rls.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces (used by Tasks 3 and 4):

```python
class RlsError(ValueError):
    """Any RLS failure: bad rules file, unsupported SQL, missing rule. Always means 'no data'."""

@dataclasses.dataclass(frozen=True)
class RlsRules:
    tables: Mapping[str, Mapping[str, str]]   # lower-cased table key -> lower-cased user -> predicate

    @classmethod
    def load(cls, path: str) -> 'RlsRules': ...          # raises RlsError
    def predicate_for(self, *, table_name: str, schema: str | None, user: str) -> tuple[str, str]: ...
        # returns (matched_key, predicate); raises RlsError

@dataclasses.dataclass(frozen=True)
class RewrittenQuery:
    sql: str
    applied_rules: list[str]                             # e.g. ["invoices: country = 'CZ'"]

def rewrite_query(sql: str, *, user: str, dialect: str, rules: RlsRules) -> RewrittenQuery: ...
    # dialect: 'snowflake' | 'bigquery' (lower-case sqlglot dialect name); raises RlsError
```

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_rls.py
import textwrap
from pathlib import Path

import pytest

from keboola_mcp_server.rls import RewrittenQuery, RlsError, RlsRules, rewrite_query

VALID_YAML = textwrap.dedent(
    """
    tables:
      invoices:
        petr: "country = 'CZ'"
        Monika: "country = 'DE'"
        admin: "TRUE"
      in.c-crm.orders:
        petr: "country = 'CZ' AND status <> 'draft'"
      orders:
        petr: "FALSE"
    """
)


@pytest.fixture
def rules(tmp_path: Path) -> RlsRules:
    path = tmp_path / 'rls.yaml'
    path.write_text(VALID_YAML)
    return RlsRules.load(str(path))


class TestLoad:
    def test_load_normalizes_keys(self, rules: RlsRules) -> None:
        assert rules.tables['invoices']['monika'] == "country = 'DE'"
        assert rules.tables['in.c-crm.orders']['petr'] == "country = 'CZ' AND status <> 'draft'"

    @pytest.mark.parametrize(
        ('content', 'match'),
        [
            ('', 'empty'),
            ('tables: {}', 'no tables'),
            ('foo: bar', "'tables'"),
            ('tables:\n  invoices: "not a mapping"', "'invoices'"),
            ('tables:\n  invoices:\n    petr: 42', "'petr'"),
            ('tables:\n  invoices:\n    petr: "country = = 1"', 'petr'),
            ('tables:\n  invoices:\n    petr: ""', 'petr'),
            ('tables: [\n', 'YAML'),
        ],
    )
    def test_load_rejects_invalid_file(self, tmp_path: Path, content: str, match: str) -> None:
        path = tmp_path / 'rls.yaml'
        path.write_text(content)
        with pytest.raises(RlsError, match=match):
            RlsRules.load(str(path))

    def test_load_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(RlsError, match='not found'):
            RlsRules.load(str(tmp_path / 'nope.yaml'))


class TestPredicateFor:
    @pytest.mark.parametrize(
        ('table_name', 'schema', 'user', 'expected'),
        [
            ('invoices', None, 'petr', ('invoices', "country = 'CZ'")),
            ('INVOICES', None, 'PETR', ('invoices', "country = 'CZ'")),
            ('invoices', 'in.c-main', 'monika', ('invoices', "country = 'DE'")),  # falls back to bare name
            ('orders', 'in.c-crm', 'petr', ('in.c-crm.orders', "country = 'CZ' AND status <> 'draft'")),
            ('orders', None, 'petr', ('orders', 'FALSE')),
        ],
    )
    def test_lookup(self, rules: RlsRules, table_name, schema, user, expected) -> None:
        assert rules.predicate_for(table_name=table_name, schema=schema, user=user) == expected

    @pytest.mark.parametrize(
        ('table_name', 'schema', 'user', 'match'),
        [
            ('customers', None, 'petr', "table 'customers'"),
            ('invoices', None, 'nobody', "user 'nobody'"),
            ('orders', 'in.c-crm', 'monika', "user 'monika'"),  # qualified key wins even if bare key has no user
        ],
    )
    def test_lookup_denied(self, rules: RlsRules, table_name, schema, user, match) -> None:
        with pytest.raises(RlsError, match=match):
            rules.predicate_for(table_name=table_name, schema=schema, user=user)


class TestRewriteQuery:
    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected_sql', 'expected_rules'),
        [
            (
                'SELECT COUNT(*) FROM invoices',
                'snowflake',
                "SELECT COUNT(*) FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices",
                ["invoices: country = 'CZ'"],
            ),
            (
                'SELECT i.id FROM invoices i JOIN "in.c-crm"."orders" AS o ON o.id = i.id',
                'snowflake',
                "SELECT i.id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS i "
                "JOIN (SELECT * FROM \"in.c-crm\".\"orders\" WHERE country = 'CZ' AND status <> 'draft') AS \"o\" "
                'ON o.id = i.id',
                ["invoices: country = 'CZ'", "in.c-crm.orders: country = 'CZ' AND status <> 'draft'"],
            ),
            (
                'WITH x AS (SELECT * FROM invoices) SELECT * FROM x',
                'snowflake',
                "WITH x AS (SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices) SELECT * FROM x",
                ["invoices: country = 'CZ'"],
            ),
            (
                'SELECT * FROM (SELECT id FROM invoices) sub',
                'snowflake',
                "SELECT * FROM (SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices) AS sub",
                ["invoices: country = 'CZ'"],
            ),
            (
                'SELECT id FROM invoices UNION ALL SELECT id FROM orders',
                'snowflake',
                "SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                'UNION ALL SELECT id FROM (SELECT * FROM orders WHERE FALSE) AS orders',
                ["invoices: country = 'CZ'", 'orders: FALSE'],
            ),
            (
                'SELECT COUNT(*) FROM `proj.ds.invoices`',
                'bigquery',
                "SELECT COUNT(*) FROM (SELECT * FROM `proj`.`ds`.`invoices` WHERE country = 'CZ') AS `invoices`",
                ["invoices: country = 'CZ'"],
            ),
            (
                'SELECT COUNT(*) FROM `ds`.`invoices` LIMIT 10',
                'bigquery',
                "SELECT COUNT(*) FROM (SELECT * FROM `ds`.`invoices` WHERE country = 'CZ') AS `invoices` LIMIT 10",
                ["invoices: country = 'CZ'"],
            ),
        ],
    )
    def test_rewrite(self, rules: RlsRules, sql, dialect, expected_sql, expected_rules) -> None:
        out = rewrite_query(sql, user='petr', dialect=dialect, rules=rules)
        assert out == RewrittenQuery(sql=expected_sql, applied_rules=expected_rules)

    @pytest.mark.parametrize(
        ('sql', 'user', 'match'),
        [
            ('DELETE FROM invoices', 'petr', 'SELECT'),
            ('INSERT INTO invoices SELECT * FROM orders', 'petr', 'SELECT'),
            ('SELECT 1; SELECT 2', 'petr', 'one statement'),
            ('SELCT nonsense', 'petr', 'SELECT'),
            ('SELECT * FROM customers', 'petr', "table 'customers'"),
            ('SELECT * FROM invoices', 'nobody', "user 'nobody'"),
            # A CTE named like a protected table would shadow it inside its own body -- refuse.
            ('WITH invoices AS (SELECT * FROM invoices) SELECT * FROM invoices', 'petr', 'CTE'),
            ('', 'petr', 'one statement'),
        ],
    )
    def test_rewrite_fails_closed(self, rules: RlsRules, sql, user, match) -> None:
        with pytest.raises(RlsError, match=match):
            rewrite_query(sql, user=user, dialect='snowflake', rules=rules)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_rls.py -q -p no:cacheprovider`
Expected: FAIL with `ModuleNotFoundError: No module named 'keboola_mcp_server.rls'`

- [ ] **Step 3: Implement `rls.py`**

```python
# src/keboola_mcp_server/rls.py
"""Row-level security (RLS) for the `query_data_rls` tool.

Rules are data, not code: a YAML file maps `table -> user -> SQL predicate`. `rewrite_query()`
replaces every table referenced by a SELECT with `(SELECT * FROM <table> WHERE <predicate>)`, so
the caller can only ever see the slice the admin wrote down for them. Everything here is
fail-closed: a missing rule, an unsupported statement or an unparseable query raises `RlsError`
and no SQL is executed. See `feature_spec/rls_query_tool/RFC.md`.
"""

import dataclasses
import logging
from collections.abc import Mapping
from pathlib import Path

import sqlglot
import yaml
from sqlglot import exp

LOG = logging.getLogger(__name__)


class RlsError(ValueError):
    """Any RLS failure: bad rules file, unsupported SQL, missing rule. Always means "no data"."""


@dataclasses.dataclass(frozen=True)
class RewrittenQuery:
    sql: str
    applied_rules: list[str]
    """Human-readable disclosure, one entry per rewritten table: `"<table key>: <predicate>"`."""


@dataclasses.dataclass(frozen=True)
class RlsRules:
    """RLS rules keyed by lower-cased table key, then lower-cased user name.

    A table key is either a bare table name (`invoices`) or `<schema>.<name>` (`in.c-crm.invoices`);
    the qualified form takes precedence during lookup.
    """

    tables: Mapping[str, Mapping[str, str]]

    @classmethod
    def load(cls, path: str) -> 'RlsRules':
        """Read and validate the YAML rules file. Raises `RlsError` on any problem."""
        file = Path(path)
        if not file.is_file():
            raise RlsError(f'RLS rules file not found: {path}')
        try:
            raw = yaml.safe_load(file.read_text())
        except yaml.YAMLError as e:
            raise RlsError(f'RLS rules file {path} is not valid YAML: {e}') from e
        if raw is None:
            raise RlsError(f'RLS rules file {path} is empty')
        if not isinstance(raw, Mapping) or 'tables' not in raw:
            raise RlsError(f"RLS rules file {path} must have a top-level 'tables' mapping")
        tables_raw = raw['tables']
        if not isinstance(tables_raw, Mapping) or not tables_raw:
            raise RlsError(f'RLS rules file {path} has no tables defined')

        tables: dict[str, dict[str, str]] = {}
        for table_key, users_raw in tables_raw.items():
            if not isinstance(users_raw, Mapping) or not users_raw:
                raise RlsError(f"RLS rules for table '{table_key}' must be a non-empty mapping of user -> predicate")
            users: dict[str, str] = {}
            for user, predicate in users_raw.items():
                if not isinstance(predicate, str) or not predicate.strip():
                    raise RlsError(f"RLS predicate for table '{table_key}', user '{user}' must be a non-empty string")
                try:
                    # Dialect-agnostic parse: we only check the predicate is a well-formed condition.
                    sqlglot.parse_one(predicate, into=exp.Condition)
                except sqlglot.errors.ParseError as e:
                    raise RlsError(f"RLS predicate for table '{table_key}', user '{user}' is not valid SQL: {e}") from e
                users[str(user).lower()] = predicate
            tables[str(table_key).lower()] = users

        LOG.info(f'Loaded RLS rules for {len(tables)} table(s) from {path}')
        return cls(tables=tables)

    def predicate_for(self, *, table_name: str, schema: str | None, user: str) -> tuple[str, str]:
        """Return `(matched_key, predicate)` for the table/user, or raise `RlsError`.

        The `<schema>.<name>` key is tried first, then the bare name. When a key matches but has
        no entry for `user`, that is a denial -- we do not fall through to a less specific key.
        """
        candidates: list[str] = []
        if schema:
            candidates.append(f'{schema}.{table_name}'.lower())
        candidates.append(table_name.lower())
        for key in candidates:
            users = self.tables.get(key)
            if users is None:
                continue
            predicate = users.get(user.lower())
            if predicate is None:
                raise RlsError(f"RLS: no rule for user '{user.lower()}' on table '{key}'")
            return key, predicate
        raise RlsError(f"RLS: no rule for table '{table_name}'")


def rewrite_query(sql: str, *, user: str, dialect: str, rules: RlsRules) -> RewrittenQuery:
    """Rewrite a single SELECT so every referenced table becomes a filtered subquery.

    :param sql: the caller's SQL, in the workspace dialect
    :param user: identity used to select rules; case-insensitive
    :param dialect: sqlglot dialect name (`'snowflake'` / `'bigquery'`)
    :param rules: loaded rules
    :raises RlsError: on anything other than one SELECT statement whose every table has a rule
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        raise RlsError(f'RLS: cannot parse SQL: {e}') from e
    if len(statements) != 1 or statements[0] is None:
        raise RlsError('RLS: exactly one statement is allowed')
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union)):
        raise RlsError(f'RLS: only SELECT statements are allowed, got {type(tree).__name__}')

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    # A CTE named like a protected table would shadow the real table inside its own body and let
    # the reference through unfiltered. Refuse instead of trying to be clever (fail-closed).
    if collisions := sorted(cte_names & set(rules.tables)):
        raise RlsError(f'RLS: CTE name(s) collide with protected table(s): {", ".join(collisions)}')

    applied: list[str] = []

    def _transform(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node
        if not node.db and node.name.lower() in cte_names:
            return node  # reference to a CTE defined in this statement, not a real table
        key, predicate = rules.predicate_for(table_name=node.name, schema=node.db or None, user=user)
        applied.append(f'{key}: {predicate}')
        alias = node.alias or node.name
        inner = exp.Table(this=node.this, db=node.args.get('db'), catalog=node.args.get('catalog'))
        filtered = exp.select('*').from_(inner).where(sqlglot.parse_one(predicate, dialect=dialect, into=exp.Condition))
        # Returning a new node stops `transform` from descending into it, so the inner table is
        # not wrapped a second time.
        return exp.Subquery(
            this=filtered, alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=node.this.quoted))
        )

    rewritten = tree.transform(_transform, copy=True)
    return RewrittenQuery(sql=rewritten.sql(dialect=dialect), applied_rules=applied)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_rls.py -q -p no:cacheprovider`
Expected: all PASS. If an `expected_sql` string differs only in whitespace/quoting emitted by sqlglot, fix the **test expectation** to the actual output *only if* the output is semantically the same filtered subquery; never loosen the fail-closed assertions.

- [ ] **Step 5: Lint and commit**

```bash
.venv/bin/ruff format src/keboola_mcp_server/rls.py tests/test_rls.py
.venv/bin/ruff check --fix src/keboola_mcp_server/rls.py tests/test_rls.py
git add src/keboola_mcp_server/rls.py tests/test_rls.py
git commit -m "RLS: add rules loading and sqlglot-based SELECT rewrite"
```

---

### Task 2: `Config.rls_rules_path` + `--rls-rules-path`

**Files:**
- Modify: `src/keboola_mcp_server/config.py:76-103`
- Modify: `src/keboola_mcp_server/cli.py:58-63` (argparse) and `cli.py:559-564` (`Config(...)`)
- Modify: `tests/test_config.py:238-247`

**Interfaces:**
- Consumes: nothing.
- Produces: `Config.rls_rules_path: str | None` (used by Task 4). Env var `KBC_RLS_RULES_PATH` works automatically through `Config._read_options`.

- [ ] **Step 1: Write the failing tests**

Add `'rls_rules_path'` to the parametrize list of `TestConfigHeaderAllowlist::test_deployment_level_fields_are_unreachable` (the list at `tests/test_config.py:~230-237`; keep the existing entries). Then add a new test to the same class:

```python
    def test_rls_rules_path_from_env_and_cli(self) -> None:
        # Deployment-level: reachable from env / CLI (trusted), never from a header (Task 2 above).
        assert Config().replace_by({'KBC_RLS_RULES_PATH': '/etc/rls.yaml'}).rls_rules_path == '/etc/rls.yaml'
        assert Config(rls_rules_path='/opt/rls.yaml').rls_rules_path == '/opt/rls.yaml'
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_config.py -q -p no:cacheprovider -k "rls_rules_path or deployment_level"`
Expected: FAIL — `TypeError: Config.__init__() got an unexpected keyword argument 'rls_rules_path'` and the header test fails for the `rls_rules_path` parameter.

- [ ] **Step 3: Add the field**

In `config.py`, after the `project_id` field (line ~81), add:

```python
    rls_rules_path: str | None = None
    """Path to the YAML row-level-security rules file (feature_spec/rls_query_tool/RFC.md).

    When set, the server registers the `query_data_rls` tool instead of `query_data`. Deployment-level:
    maps `KBC_RLS_RULES_PATH` / `--rls-rules-path` only and is deliberately absent from
    `_HEADER_ELIGIBLE_FIELDS` so a caller can never point the server at a different rules file."""
```

Do **not** touch `_HEADER_ELIGIBLE_FIELDS`.

- [ ] **Step 4: Add the CLI flag**

In `cli.py` after the `--workspace-id` argument (line ~60):

```python
    parser.add_argument(
        '--rls-rules-path',
        metavar='PATH',
        help='YAML file with row-level-security rules. When set, only the query_data_rls tool is registered.',
    )
```

and in the `Config(...)` constructor at `cli.py:~559`:

```python
        config = Config(
            storage_api_url=parsed_args.api_url,
            storage_token=parsed_args.storage_token,
            workspace_schema=parsed_args.workspace_schema,
            workspace_id=parsed_args.workspace_id,
            rls_rules_path=parsed_args.rls_rules_path,
        ).replace_by(os.environ)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_config.py tests/test_cli.py -q -p no:cacheprovider`
Expected: PASS (if `tests/test_cli.py` does not exist, run only `tests/test_config.py`).

- [ ] **Step 6: Lint and commit**

```bash
.venv/bin/ruff format src/keboola_mcp_server/config.py src/keboola_mcp_server/cli.py tests/test_config.py
.venv/bin/ruff check --fix src/keboola_mcp_server/config.py src/keboola_mcp_server/cli.py tests/test_config.py
git add src/keboola_mcp_server/config.py src/keboola_mcp_server/cli.py tests/test_config.py
git commit -m "RLS: add rls_rules_path config field and --rls-rules-path flag"
```

---

### Task 3: `query_data_rls` tool in `tools/sql.py`

**Files:**
- Modify: `src/keboola_mcp_server/tools/sql.py:215-357`
- Modify: `tests/tools/test_sql.py` (append after `test_query_data`, line ~88)

**Interfaces:**
- Consumes (Task 1): `from keboola_mcp_server.rls import RlsError, RlsRules, rewrite_query`; `ServerState.rls_rules` (Task 4 adds the field — until then the tests below set it via `dataclasses.replace`, so add the field in `mcp.py` here if Task 4 has not landed yet: `rls_rules: 'RlsRules | None' = None` right after `kai_scope_store` in `ServerState`, with `from keboola_mcp_server.rls import RlsRules` at the top of `mcp.py`).
- Produces (used by Task 4):

```python
class RlsQueryDataOutput(QueryDataOutput):
    applied_rules: list[str]

def add_sql_tools(mcp: FastMCP, *, rls_rules: RlsRules | None = None) -> None
async def query_data_rls(sql_query: str, query_name: str, user: str, ctx: Context) -> RlsQueryDataOutput
```

- [ ] **Step 1: Write the failing tests**

Append to `tests/tools/test_sql.py` (add the imports at the top of the file: `import dataclasses`, `from keboola_mcp_server.mcp import ServerState, ServerRuntimeInfo` if not present, `from keboola_mcp_server.config import Config` if not present, `from keboola_mcp_server.rls import RlsRules`, and extend the existing `from keboola_mcp_server.tools.sql import ...` with `RlsQueryDataOutput, add_sql_tools, query_data_rls`; also `from fastmcp import FastMCP`):

```python
@pytest.fixture
def rls_context(mcp_context_client: Context, mocker) -> tuple[Context, WorkspaceManager]:
    """`mcp_context_client` with RLS rules in the server state and a Snowflake workspace mock."""
    rules = RlsRules(tables={'invoices': {'petr': "country = 'CZ'"}})
    state = mcp_context_client.request_context.lifespan_context
    mcp_context_client.request_context.lifespan_context = dataclasses.replace(state, rls_rules=rules)
    manager = mocker.AsyncMock(WorkspaceManager)
    manager.get_sql_dialect.return_value = 'Snowflake'
    manager.execute_query.return_value = QueryResult(
        status='ok', data=SqlSelectData(columns=['n'], rows=[{'n': 3}]), message=None
    )
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = manager
    return mcp_context_client, manager


@pytest.mark.asyncio
async def test_query_data_rls_rewrites_and_discloses(rls_context) -> None:
    ctx, manager = rls_context

    result = await query_data_rls('SELECT COUNT(*) AS n FROM invoices', 'Invoice Count', 'Petr', ctx)

    assert isinstance(result, RlsQueryDataOutput)
    assert result.csv_data == 'n\r\n3\r\n'
    assert result.applied_rules == ["invoices: country = 'CZ'"]
    sent_sql = manager.execute_query.call_args.args[0]
    assert sent_sql == "SELECT COUNT(*) AS n FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('sql', 'user', 'match'),
    [
        ('SELECT * FROM invoices', 'nobody', "user 'nobody'"),
        ('SELECT * FROM customers', 'petr', "table 'customers'"),
        ('DELETE FROM invoices', 'petr', 'SELECT'),
    ],
)
async def test_query_data_rls_fails_closed(rls_context, sql: str, user: str, match: str) -> None:
    ctx, manager = rls_context

    with pytest.raises(ValueError, match=match):
        await query_data_rls(sql, 'Bad Query', user, ctx)

    manager.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_query_data_rls_requires_rules_in_state(mcp_context_client: Context) -> None:
    # Defensive: the tool is only registered when rules exist, but never run unfiltered if they are missing.
    with pytest.raises(ValueError, match='RLS rules'):
        await query_data_rls('SELECT 1', 'No Rules', 'petr', mcp_context_client)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('rls_rules', 'expected_tools'),
    [
        (None, ['query_data']),
        (RlsRules(tables={'invoices': {'petr': 'TRUE'}}), ['query_data_rls']),
    ],
)
async def test_add_sql_tools_registers_exactly_one_query_tool(rls_rules, expected_tools) -> None:
    mcp = FastMCP('test')
    add_sql_tools(mcp, rls_rules=rls_rules)
    tools = await mcp.list_tools(run_middleware=False)
    assert sorted(t.name for t in tools) == expected_tools
    assert all(t.annotations is not None and t.annotations.readOnlyHint is True for t in tools)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/tools/test_sql.py -q -p no:cacheprovider -k rls`
Expected: FAIL with `ImportError: cannot import name 'RlsQueryDataOutput'`.

- [ ] **Step 3: Refactor `query_data` body into a shared helper and add the RLS tool**

In `tools/sql.py`:

1. Add imports: `from keboola_mcp_server.mcp import ServerState, get_http_request_or_none` (replace the existing `get_http_request_or_none` import line) and `from keboola_mcp_server.rls import RlsRules, rewrite_query`.
2. After `QueryDataOutput` add:

```python
class RlsQueryDataOutput(QueryDataOutput):
    """Output of `query_data_rls`: the data plus a disclosure of which RLS rules shaped it."""

    applied_rules: list[str] = Field(
        description='RLS rules applied to the query, one per table, as "<table>: <predicate>". '
        'The result is a filtered slice of the data, never the whole table.'
    )
```

3. Replace `add_sql_tools` with:

```python
def add_sql_tools(mcp: FastMCP, *, rls_rules: RlsRules | None = None) -> None:
    """Add SQL tools to the MCP server.

    With `rls_rules` the server exposes only `query_data_rls`; the unrestricted `query_data` is not
    registered at all, so no per-request header (`X-Allowed-Tools`, ...) can bring it back.
    """
    if rls_rules is None:
        tool = query_data
    else:
        tool = query_data_rls
    mcp.add_tool(
        FunctionTool.from_function(
            tool,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SQL_TOOLS_TAG},
        )
    )
    LOG.info(f'SQL tools added to the MCP server: {tool.__name__}.')
```

4. Move the body of `query_data` (everything from `workspace_manager = ...` to the end, lines 310-356) into a module-level helper, and make `query_data` call it:

```python
async def _execute_and_serialize(sql_query: str, query_name: str, ctx: Context) -> tuple[SqlSelectData, str | None]:
    """Run `sql_query` in the workspace and return `(data, message)`; raises `ValueError` on failure.

    Shared by `query_data` and `query_data_rls` so progress notifications, disconnect watching and
    error mapping live in exactly one place.
    """
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)

    progress_token = _client_progress_token(ctx)

    async def _on_job_submitted(info: JobSubmittedInfo) -> None:
        await _emit_job_submitted_progress(ctx, progress_token, info)

    query_coro = workspace_manager.execute_query(
        sql_query,
        max_rows=MAX_ROWS,
        max_chars=MAX_CHARS,
        on_job_submitted=_on_job_submitted if progress_token is not None else None,
    )
    # (keep the existing comment about the disconnect race here)
    request = get_http_request_or_none()
    if request is None:
        result = await query_coro
    else:
        result = await _execute_watching_disconnect(query_coro, request, query_name)
    if result.is_ok:
        LOG.info(' '.join(filter(None, [f'Query "{query_name}" executed successfully.', result.message])))
        if result.data:
            data = result.data
        else:
            # non-SELECT query, this should not really happen, because this tool is for running SELECT queries
            data = SqlSelectData(columns=['message'], rows=[{'message': result.message}])
        return data, result.message

    # (keep the existing comment about cancellation here)
    if result.message == 'Query was cancelled':
        LOG.info(f'Query "{query_name}" was cancelled.')
        raise ValueError('Query was cancelled')
    LOG.warning(' '.join(filter(None, [f'Query "{query_name}" failed.', result.message])))
    raise ValueError(f'Failed to run SQL query, error: {result.message}')


def _to_csv(data: SqlSelectData) -> str:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=data.columns)
    writer.writeheader()
    writer.writerows(data.rows)
    return output.getvalue()
```

`query_data`'s body becomes:

```python
    data, message = await _execute_and_serialize(sql_query, query_name, ctx)
    return QueryDataOutput(query_name=query_name, csv_data=_to_csv(data), message=message)
```

5. Add the new tool right after `query_data`:

```python
@tool_errors()
async def query_data_rls(
    sql_query: Annotated[str, Field(description='SQL SELECT query to run.')],
    query_name: Annotated[
        str,
        Field(
            description=(
                'A concise, human-readable name for this query based on its purpose and what data it retrieves. '
                'Use normal words with spaces (e.g., "Customer Orders Last Month", "Top Selling Products", '
                '"User Activity Summary").'
            )
        ),
    ],
    user: Annotated[
        str,
        Field(
            description=(
                'Name of the user on whose behalf the query runs. Row-level-security rules are selected by this '
                'name; every table in the query must have a rule for it, otherwise the query is refused.'
            )
        ),
    ],
    ctx: Context,
) -> RlsQueryDataOutput:
    """
    Executes an SQL SELECT query with row-level security applied for the given user.

    Every table referenced by the query is replaced by a filtered view defined by the server-side RLS
    rules for `user`. The result is therefore a SLICE of the data, never the whole table; the
    `applied_rules` field of the output says exactly which filters were applied — always tell the user.
    Tables that have no rule for `user`, non-SELECT statements and multi-statement input are refused.

    The SQL requirements below are identical to the `query_data` tool.

    BEFORE QUERYING:
    * Always verify the table has a non-null fullyQualifiedName from get_tables tool.
      If it does not, the table is not SQL-accessible from this workspace — do not attempt the query and inform user.

    CRITICAL SQL REQUIREMENTS:

    * ALWAYS check the SQL dialect before constructing queries.
    * Do not include any comments in the SQL code
    * Use delimited identifiers and FQN format for the current SQL dialect.
    * Always use the LIMIT clause in your SELECT statements when fetching data; the tool truncates
      results beyond its row/character limits and a truncated result is a contiguous prefix, not a sample.
    * Compute aggregates (COUNT, GROUP BY, SUM, AVG, etc.) in SQL rather than pulling raw rows.
    """
    rules = ServerState.from_context(ctx).rls_rules
    if rules is None:
        raise ValueError('RLS rules are not configured on this server.')
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    dialect = (await workspace_manager.get_sql_dialect()).lower()
    # Raises RlsError (a ValueError) before anything is sent to the workspace -- fail-closed.
    rewritten = rewrite_query(sql_query, user=user, dialect=dialect, rules=rules)
    LOG.info(f'RLS applied for user "{user}" in query "{query_name}": {rewritten.applied_rules}')

    data, message = await _execute_and_serialize(rewritten.sql, query_name, ctx)
    return RlsQueryDataOutput(
        query_name=query_name, csv_data=_to_csv(data), message=message, applied_rules=rewritten.applied_rules
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/tools/test_sql.py -q -p no:cacheprovider`
Expected: all PASS, including the pre-existing `test_query_data*` and progress/disconnect tests (the refactor must not change their behaviour).

- [ ] **Step 5: Lint and commit**

```bash
.venv/bin/ruff format src/keboola_mcp_server/tools/sql.py tests/tools/test_sql.py src/keboola_mcp_server/mcp.py
.venv/bin/ruff check --fix src/keboola_mcp_server/tools/sql.py tests/tools/test_sql.py src/keboola_mcp_server/mcp.py
git add src/keboola_mcp_server/tools/sql.py tests/tools/test_sql.py src/keboola_mcp_server/mcp.py
git commit -m "RLS: add query_data_rls tool and swap it for query_data when rules are set"
```

---

### Task 4: Server wiring, swap test and README

**Files:**
- Modify: `src/keboola_mcp_server/mcp.py:121-128` (`ServerState`) — skip if Task 3 already added the field
- Modify: `src/keboola_mcp_server/server.py:255-260` and `:314`
- Modify: `tests/test_server.py` (append)
- Modify: `README.md` (after the "Option D: Using Docker" section, before "Do I Need to Start the Server Myself?")

**Interfaces:**
- Consumes: `Config.rls_rules_path` (Task 2), `RlsRules.load`, `RlsError` (Task 1), `add_sql_tools(mcp, rls_rules=...)` (Task 3).
- Produces: `ServerState.rls_rules`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_server.py` (imports needed: `from keboola_mcp_server.rls import RlsError` plus `pytest`, `Config`, `ServerRuntimeInfo`, `create_server` which are already imported):

```python
@pytest.mark.asyncio
async def test_rls_swaps_query_tool(tmp_path) -> None:
    rules_file = tmp_path / 'rls.yaml'
    rules_file.write_text("tables:\n  invoices:\n    petr: \"country = 'CZ'\"\n")

    server = create_server(Config(rls_rules_path=str(rules_file)), runtime_info=ServerRuntimeInfo(transport='stdio'))
    tool_names = {tool.name for tool in await server.list_tools(run_middleware=False)}

    assert 'query_data_rls' in tool_names
    assert 'query_data' not in tool_names


def test_rls_invalid_rules_file_fails_startup(tmp_path) -> None:
    rules_file = tmp_path / 'rls.yaml'
    rules_file.write_text('tables:\n  invoices:\n    petr: ""\n')

    with pytest.raises(RlsError, match='petr'):
        create_server(Config(rls_rules_path=str(rules_file)), runtime_info=ServerRuntimeInfo(transport='stdio'))


def test_rls_missing_rules_file_fails_startup(tmp_path) -> None:
    with pytest.raises(RlsError, match='not found'):
        create_server(
            Config(rls_rules_path=str(tmp_path / 'missing.yaml')), runtime_info=ServerRuntimeInfo(transport='stdio')
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_server.py -q -p no:cacheprovider -k rls`
Expected: FAIL — `query_data_rls` not in tool names / no `RlsError` raised.

- [ ] **Step 3: Add `rls_rules` to `ServerState`** (only if Task 3 did not)

In `mcp.py`, add `from keboola_mcp_server.rls import RlsRules` to the imports and, in `ServerState` after `kai_scope_store`:

```python
    rls_rules: RlsRules | None = None
    """Row-level-security rules loaded at startup from `Config.rls_rules_path`; None = RLS disabled."""
```

- [ ] **Step 4: Load rules in `create_server()`**

In `server.py`, add `from keboola_mcp_server.rls import RlsRules` and, just before `LOG.info(f'Creating server with config: {config}')` (line ~256):

```python
    # Row-level security (feature_spec/rls_query_tool/RFC.md): load and validate the rules once, at
    # startup, so a broken file refuses to start the server instead of failing the first query.
    rls_rules = RlsRules.load(config.rls_rules_path) if config.rls_rules_path else None
```

Pass it into `ServerState(...)`:

```python
    server_state = ServerState(
        config=config,
        runtime_info=runtime_info,
        session_store=session_store,
        kai_scope_store=kai_scope_store,
        rls_rules=rls_rules,
    )
```

and change line 314 to `add_sql_tools(mcp, rls_rules=rls_rules)`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_server.py -q -p no:cacheprovider`
Expected: the three new tests PASS and `TestServer::test_list_tools` still PASSES unchanged (no RLS ⇒ `query_data` is still there). `test_json_logging` may fail for the pre-existing environment reason — ignore.

- [ ] **Step 6: README section**

Insert into `README.md` before the `### Do I Need to Start the Server Myself?` heading:

````markdown
### Row-Level Security (pilot)

Restrict what each user can read from a shared table by giving the server a YAML rules file and
starting it with `--rls-rules-path <file>` (or `KBC_RLS_RULES_PATH=<file>`). When set, the server
exposes `query_data_rls` **instead of** `query_data`; the unrestricted tool is not registered at all.

```yaml
# rls.yaml -- table -> user -> SQL predicate (workspace dialect, inserted into WHERE verbatim)
tables:
  invoices:
    petr: "country = 'CZ'"
    monika: "country = 'DE'"
    admin: "TRUE"              # unrestricted access must be written explicitly
  in.c-crm.orders:             # <bucket>.<table> key takes precedence over a bare table name
    petr: "country = 'CZ' AND status <> 'draft'"
```

`query_data_rls(sql_query, query_name, user)` rewrites every table in the SELECT to
`(SELECT * FROM <table> WHERE <predicate>)` for that user, runs it, and reports the applied rules in
`applied_rules`. It is fail-closed: a table without a rule for the user, a non-SELECT statement, or an
unparseable query is refused and nothing is executed. The file is validated at startup; a broken file
stops the server.

Limitation: the `user` argument is supplied by the MCP client / model and is not verified by the server
(same trust level as the `X-*` request headers). Suitable for a pilot behind a trusted client.
````

- [ ] **Step 7: Lint and commit**

```bash
.venv/bin/ruff format src/keboola_mcp_server/server.py src/keboola_mcp_server/mcp.py tests/test_server.py
.venv/bin/ruff check --fix src/keboola_mcp_server/server.py src/keboola_mcp_server/mcp.py tests/test_server.py
git add src/keboola_mcp_server/server.py src/keboola_mcp_server/mcp.py tests/test_server.py README.md
git commit -m "RLS: load rules at startup, wire tool swap into create_server, document pilot"
```

---

### Task 5: Version bump, full checks, draft PR

**Files:**
- Modify: `pyproject.toml:7`, `uv.lock`

- [ ] **Step 1: Bump version and lock**

Change `version = "1.79.2"` to `version = "1.80.0"` in `pyproject.toml`, then:

```bash
VIRTUAL_ENV=.venv uv lock
```

- [ ] **Step 2: Run the full gate**

```bash
.venv/bin/ruff format --check src tests && .venv/bin/ruff check src tests
.venv/bin/python -m pytest tests -q -p no:cacheprovider
.venv/bin/python -m keboola_mcp_server.generate_tool_docs && git diff --exit-code TOOLS.md
```

Expected: ruff clean; pytest all green except the pre-existing `test_json_logging`; `TOOLS.md` unchanged (the docs generator runs with a default `Config`, so it still documents `query_data` — `query_data_rls` is documented in the README only, by design for the pilot). If `TOOLS.md` did change, inspect the diff: only a *removal* of `query_data` would be a bug (it means the swap fired without rules).

- [ ] **Step 3: Commit and push**

```bash
git add pyproject.toml uv.lock
git commit -m "RLS: bump version to 1.80.0"
git push -u origin feat/rls-query-tool
```

- [ ] **Step 4: Open a draft PR**

```bash
gh pr create --draft --title "RLS: row-level security query tool (pilot)" --body-file - <<'EOF'
## Description

**Linear**: AI-XXX (pilot, no issue yet)

### Change Type

- [ ] Major (breaking changes, significant new features)
- [x] Minor (new features, enhancements, backward compatible)
- [ ] Patch (bug fixes, small improvements, no new features)

### Summary

Pilot of row-level security for SQL queries. RFC: `feature_spec/rls_query_tool/RFC.md`.

- New `query_data_rls(sql_query, query_name, user)` tool: rewrites every table in a SELECT to
  `(SELECT * FROM <table> WHERE <predicate>)` using rules from a YAML file (`--rls-rules-path` /
  `KBC_RLS_RULES_PATH`), fail-closed, and discloses `applied_rules` in the output.
- When the rules path is set the server registers **only** `query_data_rls`; `query_data` is not
  registered, so it cannot be re-enabled via headers. Without the path nothing changes.
- Rules are validated at startup; a broken file refuses to start the server.

Not for merge as-is: `user` is client-supplied and unverified (documented limitation), no groups,
no column masking, no hot reload.

## Testing

- [ ] Tested with Cursor AI desktop (`Streamable-HTTP` transports)

## Checklist

- [x] Self-review completed
- [x] Unit tests added/updated (if applicable)
- [ ] Integration tests added/updated (if applicable)
- [x] Project version bumped according to the change type
- [x] Documentation updated (if applicable)
EOF
```

Report the PR URL.
