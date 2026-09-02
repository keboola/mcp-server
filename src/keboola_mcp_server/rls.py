"""Row-level security (RLS) for the `query_data_rls` tool.

Rules are data, not code: a YAML file maps `table -> user -> SQL predicate`. `rewrite_query()`
replaces every table referenced by a SELECT with `(SELECT * FROM <table> WHERE <predicate>)`, so
the caller can only ever see the slice the admin wrote down for them. Everything here is
fail-closed: a missing rule, an unsupported statement or an unparseable query raises `RlsError`
and no SQL is executed. See `feature_spec/rls_query_tool/RFC.md`.

Predicates are authored in the workspace's own SQL dialect and are never transpiled: a rules file
written for Snowflake is not portable to a BigQuery workspace (e.g. a double-quoted identifier is
a column reference on Snowflake but a string literal on BigQuery). One rules file serves one
workspace backend. `RlsRules.load()` only performs a dialect-agnostic syntax check; the real,
dialect-specific parse happens in `rewrite_query()` at rewrite time and is fail-closed too.
"""

import dataclasses
import itertools
import logging
from collections.abc import Mapping
from pathlib import Path

import sqlglot
import yaml
from sqlglot import exp

LOG = logging.getLogger(__name__)

# FROM/JOIN sources the rewriter knows how to secure. This is an allowlist on purpose: sqlglot has
# many node types that read like a table but carry no `exp.Table` to wrap -- `FROM TABLE(x)` parses
# as `exp.TableFromRows`, table functions as `exp.Anonymous` -- and those would otherwise reach the
# workspace unfiltered. `exp.Lateral` is allowed only as a container; its own source is checked too.
_ALLOWED_FROM_SOURCES = (exp.Table, exp.Subquery, exp.Unnest, exp.Values, exp.Lateral)

# `exp.Table` args the rewrite can faithfully reproduce. Anything else -- PIVOT/UNPIVOT, SAMPLE,
# Snowflake AT()/BEFORE()/CHANGES(), BigQuery FOR SYSTEM_TIME AS OF, an alias column list -- would be
# silently dropped when the table is rebuilt inside the wrapper, changing what the query means.
_ALLOWED_TABLE_ARGS = frozenset({'this', 'db', 'catalog', 'alias'})


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
    the qualified form takes precedence during lookup. A bare key such as `invoices` matches a table
    of that name in every schema/bucket; use the `<bucket>.<table>` form to scope a rule.
    """

    tables: Mapping[str, Mapping[str, str]]

    @classmethod
    def load(cls, path: str) -> 'RlsRules':
        """Read and validate the YAML rules file. Raises `RlsError` on any problem.

        This only checks that each predicate is well-formed, dialect-agnostic SQL -- it does not
        know which workspace dialect the predicate will eventually run under. A double-quoted
        identifier, for instance, is a column reference on Snowflake but a string literal on
        BigQuery; that mismatch is not, and cannot be, caught here. Predicates must be written for
        the dialect of the workspace they are used against; `rewrite_query()` does the real,
        dialect-specific parse (and fails closed if that parse fails too).
        """
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


def _check_from_sources(tree: exp.Expression) -> None:
    """Refuse any FROM/JOIN/LATERAL source that is not on `_ALLOWED_FROM_SOURCES`.

    Allowlist, not denylist: the rewrite can only protect what it recognises, so an unknown source
    type means "no data", never "pass it through".
    """
    for clause in itertools.chain(tree.find_all(exp.From), tree.find_all(exp.Join), tree.find_all(exp.Lateral)):
        source = clause.this
        if not isinstance(source, _ALLOWED_FROM_SOURCES):
            raise RlsError(f'RLS: unsupported FROM source: {type(source).__name__}')


def _check_output(sql: str, *, dialect: str) -> None:
    """Assert the generated SQL is still a plain SELECT over wrapped tables; raise `RlsError` if not.

    This is the safety net: it re-parses the rewriter's own output and checks it from scratch, so a
    bug or an unforeseen node type upstream cannot smuggle DDL, a second statement or an unfiltered
    table past it. The only table shape the rewrite ever produces is
    `(SELECT * FROM <table> WHERE <predicate>) AS <alias>`; anything else is a defect, not data.

    Consequence worth knowing when authoring rules: a predicate that itself references another table
    (`id IN (SELECT id FROM other)`) leaves a table outside a wrapper and is refused. Predicates must
    be plain conditions over the protected table's own columns.
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError as e:
        raise RlsError(f'RLS: rewrite produced SQL that cannot be re-parsed: {e}') from e
    if len(statements) != 1 or not isinstance(statements[0], (exp.Select, exp.SetOperation)):
        raise RlsError('RLS: rewrite produced a non-SELECT statement')
    tree = statements[0]

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if not table.db and table.name.lower() in cte_names:
            continue  # reference to a CTE defined in this statement, not a real table
        select = table.parent.parent if isinstance(table.parent, exp.From) else None
        wrapped = (
            isinstance(select, exp.Select)
            and [type(e) for e in select.expressions] == [exp.Star]
            and isinstance(select.parent, exp.Subquery)
        )
        if not wrapped:
            raise RlsError(f'RLS: rewrite left an unwrapped table reference: {table.name}')


def rewrite_query(sql: str, *, user: str, dialect: str, rules: RlsRules) -> RewrittenQuery:
    """Rewrite a single SELECT so every referenced table becomes a filtered subquery.

    :param sql: the caller's SQL, in the workspace dialect
    :param user: identity used to select rules; case-insensitive
    :param dialect: sqlglot dialect name (`'snowflake'` / `'bigquery'`)
    :param rules: loaded rules
    :raises RlsError: on anything other than one SELECT statement whose every table has a rule
    """
    try:
        sqlglot.Dialect.get_or_raise(dialect)
    except Exception as e:
        raise RlsError(f'RLS: unsupported SQL dialect {dialect!r}') from e
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError as e:
        raise RlsError(f'RLS: cannot parse SQL: {e}') from e
    if len(statements) != 1 or statements[0] is None:
        raise RlsError('RLS: exactly one statement is allowed')
    tree = statements[0]
    # `exp.SetOperation` covers UNION, EXCEPT and INTERSECT alike.
    if not isinstance(tree, (exp.Select, exp.SetOperation)):
        raise RlsError(f'RLS: only SELECT statements are allowed, got {type(tree).__name__}')
    # `SELECT ... INTO t` is generated back as `CREATE TABLE t AS ...` -- DDL from a read-only tool.
    # Every SELECT is checked, not just the outermost one: set operations nest them.
    if any(select.args.get('into') is not None for select in tree.find_all(exp.Select)):
        raise RlsError('RLS: SELECT INTO is not allowed')

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    # A CTE named like a protected table would shadow the real table inside its own body and let
    # the reference through unfiltered. Refuse instead of trying to be clever (fail-closed).
    if collisions := sorted(cte_names & set(rules.tables)):
        raise RlsError(f'RLS: CTE name(s) collide with protected table(s): {", ".join(collisions)}')

    _check_from_sources(tree)

    applied: list[str] = []

    def _transform(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node
        if not node.db and node.name.lower() in cte_names:
            return node  # reference to a CTE defined in this statement, not a real table
        # The wrapper below rebuilds the table from name/db/catalog only, so any other modifier the
        # node carries would vanish and change the query's meaning. Refuse rather than drop it.
        extra_args = [
            key
            for key, value in node.args.items()
            if key not in _ALLOWED_TABLE_ARGS and value is not None and value != []
        ]
        if (alias := node.args.get('alias')) is not None and alias.args.get('columns'):
            extra_args.append('alias columns')
        if extra_args:
            raise RlsError(f'RLS: table modifiers are not supported on {node.name!r}: {sorted(extra_args)}')
        key, predicate = rules.predicate_for(table_name=node.name, schema=node.db or None, user=user)
        applied.append(f'{key}: {predicate}')
        # Reuse the original alias identifier as-is (preserving its own quoting) so references to
        # it elsewhere in the query (e.g. an unquoted `o.id` in an ON clause) still resolve. Only
        # fall back to the table's own name/quoting when the table was not aliased at all -- using
        # the table identifier's quoting for an *existing* alias would silently change whether the
        # alias is case-sensitive, breaking those other references.
        alias_node = node.args.get('alias')
        alias_identifier = (
            alias_node.this.copy() if alias_node is not None else exp.to_identifier(node.name, quoted=node.this.quoted)
        )
        inner = exp.Table(this=node.this, db=node.args.get('db'), catalog=node.args.get('catalog'))
        try:
            predicate_expr = sqlglot.parse_one(predicate, dialect=dialect, into=exp.Condition)
        except sqlglot.errors.SqlglotError as e:
            raise RlsError(f"RLS: predicate for table '{key}' is not valid SQL for dialect {dialect!r}: {e}") from e
        filtered = exp.select('*').from_(inner).where(predicate_expr)
        # Returning a new node stops `transform` from descending into it, so the inner table is
        # not wrapped a second time.
        return exp.Subquery(this=filtered, alias=exp.TableAlias(this=alias_identifier))

    rewritten_sql = tree.transform(_transform, copy=True).sql(dialect=dialect)
    _check_output(rewritten_sql, dialect=dialect)
    # `dict.fromkeys` deduplicates while preserving first-seen order: a table joined or unioned with
    # itself is disclosed once.
    return RewrittenQuery(sql=rewritten_sql, applied_rules=list(dict.fromkeys(applied)))
