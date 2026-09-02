"""Row-level security (RLS) for the `query_data_rls` tool.

Rules are data, not code: a YAML file maps `table -> user -> SQL predicate`. `rewrite_query()`
replaces every table referenced by a SELECT with `(SELECT * FROM <table> WHERE <predicate>)`, so
the caller can only ever see the slice the admin wrote down for them. Everything here is
fail-closed: a missing rule, an unsupported statement or an unparseable query raises `RlsError`
and no SQL is executed. See `feature_spec/rls_query_tool/RFC.md`.

Predicates are authored in the workspace's own SQL dialect and are never transpiled: a rules file
written for Snowflake is not portable to a BigQuery workspace (e.g. a double-quoted identifier is
a column reference on Snowflake but a string literal on BigQuery). One rules file serves one
workspace backend, and says so: a required top-level `dialect:` key pins it. Every predicate is
parsed in that dialect at load time, and `rewrite_query()` refuses outright when the workspace it
is asked to rewrite for is not the dialect the rules were written for.
"""

import dataclasses
import itertools
import logging
import re
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

# The workspace backends the RLS pilot supports. A rules file must pin exactly one of them.
_SUPPORTED_DIALECTS = ('bigquery', 'snowflake')

# Table and user keys in the rules file. Deliberately narrow: it is the set of characters a Keboola
# bucket/table name or a user name actually uses, and it rejects the shapes that would make a key
# mean something other than it looks like -- an empty string, embedded quotes, whitespace, a `*`
# that reads like a wildcard but is not one, and (via the `str` check) YAML 1.1 scalars such as
# `yes:`/`on:`/`42:` that never were strings.
_RULE_KEY_RE = re.compile(r'^[A-Za-z0-9_.\-]+$')
_RULE_KEY_HINT = 'keys must be non-empty strings of letters, digits, underscore, dot or hyphen'

# Expression roots a predicate may have. `exp.Predicate` covers the comparison and membership
# operators (`=`, `IN`, `LIKE`, `IS`, `BETWEEN`, ...); the rest are the boolean connectives and
# literals sqlglot does not classify as predicates. Anything else -- a bare column, a literal, a
# function call, a CASE expression -- is not a filter a rules author can reason about.
_BOOLEAN_ROOTS = (exp.Predicate, exp.And, exp.Or, exp.Not, exp.Boolean)

# What a query with no real table may still call. Everything here either reads the clock or is a
# pure scalar expression over its own arguments -- nothing that reaches the catalog, the query
# history or a model. `exp.Localtime`/`exp.Localtimestamp` are what Snowflake's `CURRENT_TIME` and
# BigQuery's `CURRENT_DATETIME` parse into; `exp.If` is a `CASE` branch.
_FROMLESS_ALLOWED_FUNC_TYPES = (
    exp.CurrentDate,
    exp.CurrentTime,
    exp.CurrentTimestamp,
    exp.Localtime,
    exp.Localtimestamp,
    exp.Cast,
    exp.TryCast,
    exp.Concat,
    exp.Coalesce,
    exp.Case,
    exp.If,
)
# `NOW()` has no dedicated node -- it parses as an `exp.Anonymous`, so it is allowed by name.
_FROMLESS_ALLOWED_FUNC_NAMES = frozenset({'NOW', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP'})

# sqlglot underlines the offending token in a parse error with ANSI escapes.
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

# An unquoted Keboola bucket path (`in.c-crm.orders`), i.e. the single most common reason a query
# fails to parse here. Matched on the SQL, not on the error text, because the parser gives up at a
# different token -- and with a different message -- depending on where the dots and hyphens fall.
_BUCKET_PATH_RE = re.compile(r'(?<![\w."`])(?:in|out)\.[A-Za-z0-9_-]+\.', re.IGNORECASE)


class RlsError(ValueError):
    """Any RLS failure: bad rules file, unsupported SQL, missing rule. Always means "no data"."""


def _clean_error(error: Exception) -> str:
    """A sqlglot error message fit to put in front of a user (or a model).

    sqlglot underlines the offending token with ANSI escapes. They render as mojibake in a JSON tool
    result, an MCP client transcript or a log file, so they come out here.
    """
    return _ANSI_RE.sub('', str(error))


def _parse_error_hint(sql: str) -> str:
    """An extra sentence for a parse failure, when the SQL shows a known, fixable mistake.

    Keboola bucket names contain dots, so `in.c-crm.orders` written bare is four name parts to the
    parser and it gives up somewhere in the middle. The fix is quoting, and saying so turns an
    opaque token error into something actionable.
    """
    if _BUCKET_PATH_RE.search(sql):
        return ' -- quote the bucket, e.g. "in.c-crm"."orders"'
    return ''


def _unwrap_parenthesised(tree: exp.Expression) -> exp.Expression:
    """Strip parentheses that merely wrap a whole statement, so `(SELECT ...)` is checked as SELECT.

    A top-level `(SELECT ...)` is legal SQL and means exactly the SELECT inside it, but it parses as
    an `exp.Subquery` and would be refused as "not a SELECT" -- a false refusal, and one that invites
    the caller to go looking for a formulation that slips through. Only a bare wrapper is unwrapped:
    anything hanging off it (an alias, an ORDER BY, a LIMIT) means the node is more than parentheses
    and is left alone for the gate below to judge.
    """
    while isinstance(tree, (exp.Paren, exp.Subquery)):
        if any(key != 'this' and value is not None and value != [] for key, value in tree.args.items()):
            break
        inner = tree.this
        if not isinstance(inner, (exp.Select, exp.SetOperation, exp.Paren, exp.Subquery)):
            break
        tree = inner
    # The scope-chain walks below climb `parent` pointers; the discarded wrapper must not be on them.
    tree.parent = None
    return tree


def _is_boolean_condition(node: exp.Expression) -> bool:
    """Whether `node` is a boolean-valued condition, looking through any wrapping parentheses."""
    while isinstance(node, exp.Paren):
        node = node.this
    return isinstance(node, _BOOLEAN_ROOTS)


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

    `dialect` is the workspace backend the predicates were written for; it is not a default but a
    pin, and `rewrite_query()` refuses to run these rules against any other backend.
    """

    tables: Mapping[str, Mapping[str, str]]
    dialect: str

    @classmethod
    def load(cls, path: str) -> 'RlsRules':
        """Read and validate the YAML rules file. Raises `RlsError` on any problem.

        The file's required top-level `dialect:` key says which workspace backend the predicates are
        written for, and every predicate is parsed in exactly that dialect -- so the load-time check
        is the real one, not an approximation. (A double-quoted identifier, for instance, is a column
        reference on Snowflake but a string literal on BigQuery; without the pin that mismatch could
        not be caught here at all.)
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
        dialect_raw = raw.get('dialect')
        # `isinstance(..., str)` also rejects the YAML scalars that are not strings at all -- a bare
        # `dialect:` (None), a number, or `on`/`yes` (booleans).
        if not isinstance(dialect_raw, str) or dialect_raw.strip().lower() not in _SUPPORTED_DIALECTS:
            raise RlsError(
                f"RLS rules file {path} must have a top-level 'dialect' of "
                f'{" or ".join(_SUPPORTED_DIALECTS)}, got {dialect_raw!r}'
            )
        dialect = dialect_raw.strip().lower()
        tables_raw = raw['tables']
        if not isinstance(tables_raw, Mapping) or not tables_raw:
            raise RlsError(f'RLS rules file {path} has no tables defined')

        tables: dict[str, dict[str, str]] = {}
        for table_key, users_raw in tables_raw.items():
            if not isinstance(table_key, str) or not _RULE_KEY_RE.match(table_key):
                raise RlsError(f'RLS rules file {path} has an invalid table key {table_key!r}: {_RULE_KEY_HINT}')
            key = table_key.lower()
            if key in tables:
                # Only reachable when two YAML keys differ solely in case: the second would silently
                # replace the first, so the admin would be reading a rule that is not in force.
                raise RlsError(f"RLS rules file {path} has a duplicate table key '{key}'")
            if not isinstance(users_raw, Mapping) or not users_raw:
                raise RlsError(f"RLS rules for table '{table_key}' must be a non-empty mapping of user -> predicate")
            users: dict[str, str] = {}
            for user, predicate in users_raw.items():
                if not isinstance(user, str) or not _RULE_KEY_RE.match(user):
                    raise RlsError(
                        f"RLS rules for table '{table_key}' have an invalid user key {user!r}: {_RULE_KEY_HINT}"
                    )
                user_key = user.lower()
                if user_key in users:
                    raise RlsError(f"RLS rules for table '{table_key}' have a duplicate user key '{user_key}'")
                if not isinstance(predicate, str) or not predicate.strip():
                    raise RlsError(f"RLS predicate for table '{table_key}', user '{user}' must be a non-empty string")
                try:
                    # Parsed in the file's own pinned dialect, so this check is the real one.
                    parsed = sqlglot.parse_one(predicate, dialect=dialect, into=exp.Condition)
                except sqlglot.errors.ParseError as e:
                    raise RlsError(
                        f"RLS predicate for table '{table_key}', user '{user}' "
                        f'is not valid {dialect} SQL: {_clean_error(e)}'
                    ) from e
                if not _is_boolean_condition(parsed):
                    raise RlsError(
                        f"RLS predicate for table '{table_key}', user '{user}' must be a boolean condition, "
                        f'got {type(parsed).__name__}'
                    )
                if next(parsed.find_all(exp.Table, exp.Subquery, exp.Select), None) is not None:
                    # Such a predicate leaves a table outside a wrapper; `_check_output` would refuse
                    # it at query time anyway, but the admin should hear about it at startup.
                    raise RlsError(
                        f"RLS predicate for table '{table_key}', user '{user}' must not reference a table or subquery"
                    )
                users[user_key] = predicate
            tables[key] = users

        LOG.info(f'Loaded RLS rules for {len(tables)} table(s) from {path} (dialect {dialect})')
        return cls(tables=tables, dialect=dialect)

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
        if isinstance(source, exp.Table) and not isinstance(source.this, exp.Identifier):
            # A table function (`FROM my_udtf(1)`) parses as an `exp.Table` wrapping an
            # `exp.Anonymous`. It has no table name to look a rule up by, so it must be refused
            # here rather than reach the workspace as an unrewritten source.
            raise RlsError(f'RLS: unsupported table reference: {source.sql()}')


def _cte_names(tree: exp.Expression) -> set[str]:
    """Every CTE alias in `tree`, as raw identifier text lower-cased (quoting ignored).

    Case and quoting are deliberately ignored here because this set only ever *widens* a check: it
    is the cheap "could this name mean a CTE at all?" pre-filter for `_is_cte_reference` (which then
    resolves the name precisely) and the collision guard against rule keys, which must fire on the
    merest resemblance to a protected table's name.
    """
    return {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}


def _identifier_key(text: str, quoted: bool) -> tuple[str, bool]:
    """Reduce an identifier to a comparison key that matches engine name resolution.

    Snowflake and BigQuery fold unquoted identifiers to a canonical case but treat quoted ones as
    literal, so `"secret"` and `"SECRET"` are two different names while `secret` and `SECRET` are
    one. Comparing everything lower-cased would bind a quoted reference to a differently-cased
    quoted CTE that the engine resolves to the *base table* -- and pass that table through
    unfiltered. Quoted and unquoted forms of the same text stay distinct too (Ruling 14): the
    caller decides whether that mismatch is a refusal or merely a non-match.
    """
    return text if quoted else text.lower(), quoted


def _cte_key(cte: exp.CTE) -> tuple[str, bool]:
    """A CTE declaration as an `_identifier_key` -- the same shape a table reference is reduced to
    in `_is_cte_reference`."""
    alias = cte.args.get('alias')
    identifier = alias.this if isinstance(alias, exp.TableAlias) else None
    quoted = isinstance(identifier, exp.Identifier) and identifier.quoted
    return _identifier_key(cte.alias_or_name, quoted)


def _with_clause(node: exp.Expression) -> exp.With | None:
    """The WITH clause `node` carries, if any.

    Found by scanning the node's args rather than by key: sqlglot has renamed the argument
    (`with` -> `with_`) between versions, and a silently missed WITH clause here would mean a
    missed shadowing check.
    """
    for value in node.args.values():
        if isinstance(value, exp.With):
            return value
    return None


def _cte_names_in_scope(node: exp.Expression) -> set[tuple[str, bool]]:
    """CTE aliases visible from `node`, as `_identifier_key` pairs.

    Walks `node`'s own ancestor chain outwards: a statement's WITH clause is visible to that
    statement's body, and inside a CTE body only that CTE's earlier siblings are visible -- plus the
    CTE itself, but *only* under `WITH RECURSIVE`, which is what makes a recursive CTE work. Without
    `RECURSIVE` the engine resolves a CTE's own name inside its body to the base table, so counting
    it as visible here would wave a real, unfiltered table through. A CTE declared in a nested or
    sibling scope is not reachable either -- which is the whole point, see `_is_cte_reference`.

    This is a scope *chain* walk, not full SQL name resolution; it never has to be more precise
    than that because every name it fails to resolve is refused, not passed through.
    """
    names: set[tuple[str, bool]] = set()
    child, parent = node, node.parent
    while parent is not None:
        if isinstance(parent, exp.With):
            # `child` is the CTE whose body we are in: stop at it, later siblings are not visible.
            for cte in parent.expressions:
                if cte is child and not parent.args.get('recursive'):
                    break
                names.add(_cte_key(cte))
                if cte is child:
                    break
        elif (with_clause := _with_clause(parent)) is not None and with_clause is not child:
            names.update(_cte_key(cte) for cte in with_clause.expressions)
        child, parent = parent, parent.parent
    return names


def _is_non_recursive_self_reference(node: exp.Table, key: tuple[str, bool]) -> bool:
    """Whether `node` sits inside the body of a CTE named `key` whose WITH lacks `RECURSIVE`.

    Only used to explain a refusal: `_cte_names_in_scope` has already decided such a name is not a
    CTE reference. It exists so the caller gets "this needs RECURSIVE" rather than the misleading
    "declared in another scope" -- the declaration is right here, it is just not in scope yet.
    """
    child, parent = node, node.parent
    while parent is not None:
        if (
            isinstance(parent, exp.With)
            and not parent.args.get('recursive')
            and any(cte is child and _cte_key(cte) == key for cte in parent.expressions)
        ):
            return True
        child, parent = parent, parent.parent
    return False


def _is_cte_reference(node: exp.Table, cte_names: set[str]) -> bool:
    """Whether `node` names a CTE declared in its own enclosing scope chain (and so is not a table).

    `cte_names` is `_cte_names()` for the whole statement. Matching the whole-tree set is not
    enough on its own: a CTE declared in a nested subquery or in the other branch of a UNION used
    to make a top-level *real* table look like a CTE reference and sail through unfiltered.

    Fail-closed: when the name matches a CTE that is out of scope, or matches one in scope but with
    different quoting (so the engine and this rewriter could disagree about what it resolves to),
    raise rather than guess.
    """
    if node.db or node.catalog or not isinstance(node.this, exp.Identifier):
        return False  # a qualified or non-identifier source is never a CTE reference
    if node.name.lower() not in cte_names:
        return False
    key = _identifier_key(node.name, node.this.quoted)
    in_scope = _cte_names_in_scope(node)
    if key in in_scope:
        return True
    if any(scoped_name == key[0] for scoped_name, _ in in_scope):
        raise RlsError(f'RLS: ambiguous CTE reference, quoting differs from the declaration: {node.name}')
    if _is_non_recursive_self_reference(node, key):
        raise RlsError(f'RLS: a CTE cannot reference itself without RECURSIVE: {node.name}')
    raise RlsError(f'RLS: table reference shadowed by a CTE declared in another scope: {node.name}')


def _function_name(node: exp.Expression) -> str:
    """The name a function call goes by, including any `db.schema.` prefix it was written with.

    sqlglot parses `SNOWFLAKE.CORTEX.COMPLETE(...)` as a `Dot` chain whose rightmost element is an
    `exp.Anonymous` named only `COMPLETE`, so the qualification -- the part that says which function
    family this is -- lives in the ancestors and has to be walked back in.
    """
    if isinstance(node, exp.Anonymous):
        name = node.name
        current: exp.Expression = node
        parent = current.parent
        while isinstance(parent, exp.Dot) and parent.expression is current:
            # The left side of such a `Dot` is identifiers only, so rendering it is safe.
            name = f'{parent.this.sql()}.{name}'
            current, parent = parent, parent.parent
        return name
    return node.sql_name() if isinstance(node, exp.Func) else type(node).__name__


def _check_functions(tree: exp.Expression, cte_names: set[str]) -> None:
    """Refuse function calls that RLS cannot reason about; raise `RlsError` if any is present.

    Two bans, both allowlist-shaped where it matters:

    * `SYSTEM$...` and anything under `CORTEX` are refused wherever they appear. They read metadata,
      cancel queries or hand text to an LLM -- none of which the row filter constrains, however
      thoroughly the FROM clause is rewritten.
    * A query with no real table to filter (`SELECT GET_DDL(...)`, or the same thing dressed up with
      a dummy CTE) is not a data query at all: whatever it returns, no predicate shaped it. Only a
      small set of clock functions and pure scalar expressions is allowed there.
    """
    for node in tree.find_all(exp.Anonymous):
        name = _function_name(node)
        if any(part.upper().startswith('SYSTEM$') for part in name.split('.')) or 'CORTEX' in name.upper():
            raise RlsError(f'RLS: function call is not allowed: {name}')

    # An `exp.Table` naming a CTE in scope is not a real table. Resolution goes through
    # `_is_cte_reference` rather than the cheap name set so that a name which only *looks* like a
    # CTE is refused with the reason it deserves ("declared in another scope") instead of being
    # counted as a non-table here and reported as a stray function call.
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier) or not _is_cte_reference(table, cte_names):
            return  # there is a real table here; the rewrite will filter it

    for node in tree.find_all(exp.Func):
        if isinstance(node, _FROMLESS_ALLOWED_FUNC_TYPES):
            continue
        name = _function_name(node)
        if name.upper() in _FROMLESS_ALLOWED_FUNC_NAMES:
            continue
        raise RlsError(f'RLS: function calls are not allowed in a query without FROM: {name}')


def _matching_key(table: exp.Table, keys: Mapping[str, str]) -> str | None:
    """The rules key `table` was wrapped under, in the same order `predicate_for` tries them."""
    candidates = [f'{table.db}.{table.name}'.lower()] if table.db else []
    candidates.append(table.name.lower())
    return next((key for key in candidates if key in keys), None)


def _check_output(sql: str, *, dialect: str, predicates: Mapping[str, str]) -> None:
    """Assert the generated SQL is still a plain SELECT over wrapped tables; raise `RlsError` if not.

    This is the safety net: it re-parses the rewriter's own output and checks it from scratch, so a
    bug or an unforeseen node type upstream cannot smuggle DDL, a second statement or an unfiltered
    table past it. The only table shape the rewrite ever produces is
    `(SELECT * FROM <table> WHERE <predicate>) AS <alias>`; anything else is a defect, not data.

    `predicates` maps each rules key the rewrite matched to the predicate text it inserted for it.
    A wrapper is only accepted when its WHERE is present AND generates back to exactly that
    predicate: "wrapped in something" is not the invariant, "wrapped in the filter the admin wrote"
    is. Without the comparison a wrapper carrying a weakened or empty condition would pass.

    Consequence worth knowing when authoring rules: a predicate that itself references another table
    (`id IN (SELECT id FROM other)`) leaves a table outside a wrapper and is refused. Predicates must
    be plain conditions over the protected table's own columns.
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError as e:
        raise RlsError(f'RLS: rewrite produced SQL that cannot be re-parsed: {_clean_error(e)}') from e
    if len(statements) != 1 or not isinstance(statements[0], (exp.Select, exp.SetOperation)):
        raise RlsError('RLS: rewrite produced a non-SELECT statement')
    tree = statements[0]

    cte_names = _cte_names(tree)
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            # A table function (`FROM my_udtf(1)`) parses as an `exp.Table` with no identifier to
            # name it. The same guard `_check_from_sources` and `_transform` apply on the way in --
            # here it also stops such a node reaching the "is it wrapped?" test, which would report
            # it under an empty name.
            raise RlsError('RLS: rewrite left an unsupported table reference')
        if _is_cte_reference(table, cte_names):
            continue  # reference to a CTE in scope here, not a real table
        select = table.parent.parent if isinstance(table.parent, exp.From) else None
        wrapped = (
            isinstance(select, exp.Select)
            and [type(e) for e in select.expressions] == [exp.Star]
            and isinstance(select.parent, exp.Subquery)
        )
        if not wrapped:
            raise RlsError(f'RLS: rewrite left an unwrapped table reference: {table.name}')
        assert isinstance(select, exp.Select)  # narrowed by `wrapped`
        key = _matching_key(table, predicates)
        if key is None:
            raise RlsError(f'RLS: rewrite wrapped a table no rule was looked up for: {table.name}')
        where = select.args.get('where')
        if where is None:
            raise RlsError(f'RLS: rewrite left a wrapper without a WHERE clause: {table.name}')
        try:
            expected = sqlglot.parse_one(predicates[key], dialect=dialect, into=exp.Condition)
        except sqlglot.errors.SqlglotError as e:
            raise RlsError(
                f"RLS: predicate for table '{key}' is not valid SQL for dialect {dialect!r}: {_clean_error(e)}"
            ) from e
        # Compared as generated text in the same dialect, so the two sides are normalised the same
        # way and only a real difference in the condition can fail this.
        if where.this.sql(dialect=dialect) != expected.sql(dialect=dialect):
            raise RlsError(f"RLS: rewrite produced a WHERE that is not the rule for table '{key}'")


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
    # Predicates are never transpiled, so rules written for one backend must not be applied to
    # another: the same text can mean different things (or silently nothing) under a different
    # dialect. Fail closed rather than rewrite with a filter whose meaning we cannot vouch for.
    if rules.dialect != dialect.lower():
        raise RlsError(f'RLS: rules are for dialect {rules.dialect} but the workspace is {dialect.lower()}')
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError as e:
        raise RlsError(f'RLS: cannot parse SQL: {_clean_error(e)}{_parse_error_hint(sql)}') from e
    if len(statements) != 1 or statements[0] is None:
        raise RlsError('RLS: exactly one statement is allowed')
    tree = _unwrap_parenthesised(statements[0])
    # `exp.SetOperation` covers UNION, EXCEPT and INTERSECT alike.
    if not isinstance(tree, (exp.Select, exp.SetOperation)):
        raise RlsError(f'RLS: only SELECT statements are allowed, got {type(tree).__name__}')
    # `SELECT ... INTO t` is generated back as `CREATE TABLE t AS ...` -- DDL from a read-only tool.
    # Every SELECT is checked, not just the outermost one: set operations nest them.
    if any(select.args.get('into') is not None for select in tree.find_all(exp.Select)):
        raise RlsError('RLS: SELECT INTO is not allowed')

    cte_names = _cte_names(tree)
    # A CTE named like a protected table would shadow the real table inside its own body and let
    # the reference through unfiltered. Refuse instead of trying to be clever (fail-closed).
    # A CTE alias is always bare, so it is a rule key's bare table name it shadows: `in.c-crm.orders`
    # is guarded by `orders`. The full keys are compared too, for a bare key like `invoices`.
    # Unlike `_identifier_key`, this comparison stays case- and quoting-insensitive on purpose: rule
    # keys are stored lower-cased and have no quoting of their own, so anything that merely *looks*
    # like a protected table's name has to collide here rather than be resolved later.
    rule_keys = {key.lower() for key in rules.tables}
    if collisions := sorted(cte_names & (rule_keys | {key.rsplit('.', 1)[-1] for key in rule_keys})):
        raise RlsError(f'RLS: CTE name(s) collide with protected table(s): {", ".join(collisions)}')

    _check_from_sources(tree)
    _check_functions(tree, cte_names)

    applied: list[str] = []
    # The predicate the rewrite actually inserted for each matched key, handed to `_check_output` so
    # the safety net can verify the WHERE it finds is the rule, not merely some WHERE.
    inserted: dict[str, str] = {}

    def _transform(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node
        if not isinstance(node.this, exp.Identifier):
            # A table function has no name to look a rule up by -- `_check_from_sources` already
            # refuses these; this is the same guard on the rewrite path itself.
            raise RlsError(f'RLS: unsupported table reference: {node.sql()}')
        if _is_cte_reference(node, cte_names):
            return node  # reference to a CTE in scope here, not a real table
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
        inserted[key] = predicate
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
            raise RlsError(
                f"RLS: predicate for table '{key}' is not valid SQL for dialect {dialect!r}: {_clean_error(e)}"
            ) from e
        filtered = exp.select('*').from_(inner).where(predicate_expr)
        # Returning a new node stops `transform` from descending into it, so the inner table is
        # not wrapped a second time.
        return exp.Subquery(this=filtered, alias=exp.TableAlias(this=alias_identifier))

    rewritten_sql = tree.transform(_transform, copy=True).sql(dialect=dialect)
    _check_output(rewritten_sql, dialect=dialect, predicates=inserted)
    # `dict.fromkeys` deduplicates while preserving first-seen order: a table joined or unioned with
    # itself is disclosed once.
    return RewrittenQuery(sql=rewritten_sql, applied_rules=list(dict.fromkeys(applied)))
