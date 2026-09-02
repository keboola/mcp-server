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
        return exp.Subquery(this=filtered, alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=node.this.quoted)))

    rewritten = tree.transform(_transform, copy=True)
    return RewrittenQuery(sql=rewritten.sql(dialect=dialect), applied_rules=applied)
