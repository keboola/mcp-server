"""Row-level security (RLS) for `query_data`.

Rules are data, not code: an org admin authors an `rls-policy` metastore object per protected
table (`RlsRules.from_metastore()`), mapping `principal -> declarative condition primitive`.
`rewrite_query()` replaces every table referenced by a SELECT that has a policy applicable to the
current project with `(SELECT * FROM <table> WHERE <predicate>)`, so the caller can only ever see
the slice the admin wrote down for them. Everything here is fail-closed *for a table a policy
names*: a missing rule for the resolved principal, an unsupported statement, or an unparseable
query raises `RlsError` and no SQL is executed. A table with no applicable policy at all is left
untouched -- RLS is opt-in per table, not a deployment-wide switch. See
`feature_spec/rls_query_tool/RFC.md`.

Predicates are compiled from primitives (see `_compile_primitive`), never hand-written SQL text --
closing the injection surface for a rule author who isn't an engineer. They are still tied to one
SQL dialect: a policy authored against a Snowflake-workspace's column names is not portable to a
BigQuery workspace. `RlsRules.dialect` pins the workspace backend the compiled predicates are for,
and `rewrite_query()` refuses outright when the workspace it is asked to rewrite for is not that
dialect. A policy's own `dialect` is optional (schema 1.1.0); one that names the other backend refuses
reads of its table only.

Schema 1.1.0 (read alongside 1.0.0): a rule selects by `principal`, `principals` or IdP `groups`; every
rule selecting the reading identity applies and their conditions combine with OR (CLS: the visible
columns are the union); `{"$identity": "email"}` / `{"$identity": "groups"}` resolve to bound literals
per identity; a policy's `default` condition applies to an identity no rule selects, and the
`{"false": true}` sentinel matches no row.

Column-level security (CLS) is a sibling mechanism, `ClsRules`, backed by `cls-policy` metastore
objects: an allowlist of visible columns per principal instead of a row predicate. `rewrite_query()`
optionally takes both an `RlsRules` and a `ClsRules` and composes them into the *same* wrapper
subquery per table -- never two separate rewrite passes. See
`feature_spec/rls_query_tool/RFC.md`'s "v3 Amendment" for the design.
"""

import dataclasses
import itertools
import logging
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

import sqlglot
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

# A principal is the caller's login identity -- an email (`@`, `+`, ...) -- not an identifier. It is
# only ever compared to the session's identity (a dict key), never put into SQL, so it needs a looser
# check than `_RULE_KEY_RE`: no whitespace or control characters, nothing that reads as empty.
_PRINCIPAL_RE = re.compile(r'^[^\s\x00-\x1f\x7f]+$')
# Case folding for principal matching lower-cases ASCII letters only. `str.lower()` also folds some non-ASCII
# characters onto ASCII ones (the Kelvin sign U+212A becomes `k`), which would make two different addresses
# one principal. A non-ASCII character is compared exactly instead.
_ASCII_LOWER = {ord(c): ord(c) + 32 for c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'}


def _fold_principal(value: str) -> str:
    return value.translate(_ASCII_LOWER)


# An IdP group name or id, compared exactly as delivered (schema 1.1.0 `groups`). Names may contain spaces
# ("Sales EU"); control characters and the empty string are refused.
_GROUP_RE = re.compile(r'^[^\x00-\x1f\x7f]+$')

# Identity the load-time validation compiles placeholder rules with, so a malformed rule is refused when the
# policy loads, not only when a matching user first queries the table.
_VALIDATION_IDENTITY: tuple[str, tuple[str, ...]] = ('validation@example.invalid', ('validation-group',))


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
# Functions that read catalog, stage or query-history metadata. A row filter does not constrain them, so
# they are refused wherever they appear -- a governed table added as a dummy FROM must not unlock them.
_METADATA_FUNC_NAMES = frozenset(
    {
        'GET_DDL',
        'GET_OBJECT_REFERENCES',
        'GET_QUERY_OPERATOR_STATS',
        'INFER_SCHEMA',
        'EXTRACT_SEMANTIC_CATEGORIES',
        'GENERATE_COLUMN_DESCRIPTION',
        'GET_PRESIGNED_URL',
        'GET_STAGE_LOCATION',
        'BUILD_STAGE_FILE_URL',
        'BUILD_SCOPED_FILE_URL',
        'GET_ABSOLUTE_PATH',
        'GET_RELATIVE_PATH',
    }
)

# sqlglot underlines the offending token in a parse error with ANSI escapes.
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

# An unquoted Keboola bucket path (`in.c-crm.orders`), i.e. the single most common reason a query
# fails to parse here. Matched on the SQL, not on the error text, because the parser gives up at a
# different token -- and with a different message -- depending on where the dots and hyphens fall.
_BUCKET_PATH_RE = re.compile(r'(?<![\w."`])(?:in|out)\.[A-Za-z0-9_-]+\.', re.IGNORECASE)


# What the caller is told when a rule exists but could not be applied. Deliberately says nothing
# about the predicate: the caller is a model relaying to a user, and the predicate is the policy.
_RULE_NOT_APPLIED = 'RLS: rule for table {key} could not be applied'


class RlsError(ValueError):
    """Any RLS failure: bad rules file, unsupported SQL, missing rule. Always means "no data"."""


# What a caller with no rule for a governed table is told. Deliberately says nothing about policies:
# row-/column-level security is applied silently, so the refusal reads like any other missing grant.
ACCESS_DENIED_MESSAGE = 'Access denied: you do not have permission to query one of the requested tables.'


class RlsAccessDenied(RlsError):
    """The caller has no rule for a governed table. The detail goes to the log, never to the caller."""


def _applies_to_project(obj: Any, *, label: str, obj_id: str) -> bool:
    """Whether a listed policy object is enforced for the calling project.

    Every object the SA-verified, project-scoped metastore list returns applies: the metastore already
    limits that list to what the project owns, was granted, or inherits at organization scope. The
    source/target fields cannot be used to filter it again -- for a project that sees a policy only
    through a grant the metastore omits `targetProjectIds` (the project has no need to know who else was
    granted access) and `projectId` is the AUTHORING project, so such a policy would match neither and its
    table would be silently treated as ungoverned.

    Only a `project`-scoped object is refused (see below).
    """
    meta = getattr(obj, 'meta', None)
    if getattr(meta, 'scope', None) == 'project':
        # These policy types are organization/targeted only (schema `x-metastore.scope.supported`). A
        # project-scoped one means the backend's boundary regressed; enforcing it would quietly accept
        # policy authored outside that boundary, so refuse.
        raise RlsError(f"{label}: metastore object '{obj_id}' has the unsupported scope 'project'")
    return True


def _rule_selector(rule: Mapping[str, Any], *, label: str, obj_id: str) -> tuple[list[str], list[str]]:
    """`(principals, groups)` a rule applies to: exactly one of `principal` / `principals` / `groups` (the
    schema's `oneOf`; `groups` is new in 1.1.0). Two or none is rejected -- silently preferring one would apply
    a different access rule than the author wrote. Principals come back case-folded."""
    present = [field for field in ('principal', 'principals', 'groups') if rule.get(field) is not None]
    if len(present) != 1:
        raise RlsError(
            f"{label}: metastore object '{obj_id}' must have exactly one of 'principal', 'principals', 'groups' "
            f'in a rule, got {present or "none"}'
        )
    field = present[0]
    value = rule[field]
    names = [value] if field == 'principal' else value
    if not isinstance(names, Sequence) or isinstance(names, (str, bytes)) or not names:
        raise RlsError(f"{label}: metastore object '{obj_id}' has an invalid '{field}': {rule!r}")
    if field == 'groups':
        for group in names:
            if not isinstance(group, str) or not _GROUP_RE.fullmatch(group):
                raise RlsError(f"{label}: metastore object '{obj_id}' has an invalid group {group!r}")
        return [], list(names)
    for name in names:
        if not isinstance(name, str) or not _PRINCIPAL_RE.fullmatch(name):
            raise RlsError(f"{label}: metastore object '{obj_id}' has an invalid principal {name!r}")
    return [_fold_principal(name) for name in names], []


def _selects(
    principals: frozenset[str], groups: frozenset[str], *, folded_user: str, user_groups: Sequence[str] | None
) -> bool:
    """Whether a rule selects the identity: its (folded) email is a listed principal, or -- only when the caller's
    groups are KNOWN (`user_groups` is not None) -- it is in a listed group."""
    return folded_user in principals or (user_groups is not None and not groups.isdisjoint(user_groups))


def _uses_identity(condition: Any) -> bool:
    """Whether a condition contains an `$identity` placeholder anywhere (so it must be compiled per identity)."""
    if not isinstance(condition, Mapping):
        return False
    if any(isinstance(condition.get(field), Mapping) for field in ('value', 'values')):
        return True
    return any(
        _uses_identity(branch)
        for combinator in ('and', 'or')
        if isinstance(condition.get(combinator), Sequence)
        for branch in condition[combinator]
    )


def _combine(conditions: Sequence[exp.Condition | str], *, op: str, dialect: str) -> exp.Condition | str:
    """Combine conditions with `op` ('or' / 'and') in one step, from trees -- never by re-parsing an accumulated
    predicate (that is quadratic and recursion-bound in the number of rules). Duplicates (same generated text)
    collapse; a single condition comes back unchanged. A `str` is a predicate given as SQL text (direct
    `RlsRules(tables=...)` construction); it is parsed only when it has to be combined with another."""
    unique: dict[str, exp.Condition | str] = {}
    for condition in conditions:
        unique.setdefault(condition if isinstance(condition, str) else condition.sql(dialect=dialect), condition)
    if len(unique) == 1:
        return next(iter(unique.values()))
    trees: list[exp.Condition] = []
    for text, condition in unique.items():
        if isinstance(condition, str):
            try:
                condition = sqlglot.parse_one(text, dialect=dialect, into=exp.Condition)
            except sqlglot.errors.SqlglotError as e:
                raise RlsError(f'RLS: a rule predicate is not valid SQL for dialect {dialect!r}') from e
        trees.append(condition)
    return exp.or_(*trees, copy=True) if op == 'or' else exp.and_(*trees, copy=True)


def _as_sql(condition: exp.Condition | str, *, dialect: str) -> str:
    return condition if isinstance(condition, str) else condition.sql(dialect=dialect)


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


def _normalize_schema(schema: str, dialect: str) -> str:
    """The bucket part of a rules key, as the workspace actually spells it.

    Snowflake keeps a Keboola bucket name verbatim as the schema (`"in.c-crm"`). BigQuery cannot:
    dataset names allow neither dots nor hyphens, so the server maps bucket `in.c-crm` to dataset
    `in_c_crm`. Normalising the key at load time means the rules file is written the same way for
    both backends -- in Keboola's own bucket names -- and still matches what the query says.
    """
    return schema.replace('.', '_').replace('-', '_') if dialect == 'bigquery' else schema


# `<branchId>_` in front of a Keboola stage (`in`/`out`) in the physical schema of a branch workspace.
_BRANCH_PREFIX_RE = re.compile(r'^\d+_(?=(?:in|out)[._])')


def _rule_key(schema: str, table: str, dialect: str) -> str:
    """The rules-file key a `<schema>.<table>` reference is looked up under.

    Case handling follows the backend's own object-name resolution, so a rule matches exactly the
    table the engine would read:

    * Snowflake folds unquoted names and the workspace hands out fully-qualified names quoted in the
      storage case, so keys are compared case-insensitively -- `"IN.C-CRM"."INVOICES"` and
      `"in.c-crm"."invoices"` name the same table there.
    * BigQuery dataset and table names are case-SENSITIVE: `in_c_crm.Invoices` is a different table
      from `in_c_crm.invoices`, so a rule for one must not cover the other. Keys keep their case and
      a mismatch simply finds no rule -- which is a refusal.

    A development-branch workspace spells the schema with the branch id in front (`35403_out.c-model`,
    `35403_out_c_model` on BigQuery) while the policy and the Storage table id stay `out.c-model`. The
    prefix is dropped on both sides, so a branch query is governed by the same policy as production
    instead of silently reading the table unfiltered.
    """
    key = f'{_BRANCH_PREFIX_RE.sub("", schema)}.{table}'
    return key if dialect == 'bigquery' else key.lower()


# Comparison ops a `condition` primitive's `column`/`op`/`value` shape may use -- each maps to the
# `exp.Condition` builder (a method for `eq`/`ne`; sqlglot has no `.gt()`/`.gte()`/`.lt()`/`.lte()`
# methods, only the operator overloads, hence the two shapes) that produces the equivalent SQL,
# never a formatted string. Deliberately closed: an op not in this set is refused, not passed
# through as a no-op.
_COMPARISON_OPS: Mapping[str, Callable[[exp.Column, exp.Expression], exp.Condition]] = {
    'eq': lambda col, val: col.eq(val),
    'ne': lambda col, val: col.neq(val),
    'gt': lambda col, val: col > val,
    'gte': lambda col, val: col >= val,
    'lt': lambda col, val: col < val,
    'lte': lambda col, val: col <= val,
}


def _compile_primitive(
    condition: Any, *, dialect: str, identity: tuple[str, Sequence[str]] | None = None
) -> exp.Condition:
    """Compile one declarative `condition` primitive (the shape in the RFC's JSON schema for the
    `rls-policy` metastore object type) into a `sqlglot.exp.Condition` tree.

    Never parses or formats a SQL string: every branch below builds the expression tree directly
    from sqlglot's own builder methods (`exp.column(...).eq(...)`, `.isin(...)`, `exp.And`/`Or`,
    `exp.true()`), so a primitive can only ever produce a well-formed boolean condition over one
    named column and literal value(s) -- there is no string-formatting step for an admin's (or a
    guided CLI's) input to inject through. Raises `RlsError` for any shape this function doesn't
    recognise; an unrecognised primitive is refused, never silently treated as `TRUE`.

    `identity` is `(email, groups)` of the reading identity. Schema 1.1.0 placeholders resolve against it to
    bound literals through the same builders: `value: {"$identity": "email"}` and
    `values: {"$identity": "groups"}`. An identity with no email or no groups makes that comparison match
    nothing (`FALSE`) -- never `= ''` or `IN ()`. A placeholder with no identity, or in any other place, is
    refused.
    """
    if not isinstance(condition, Mapping):
        raise RlsError(f'RLS: condition must be an object, got {type(condition).__name__}')

    if 'true' in condition:
        if condition.get('true') is not True or len(condition) != 1:
            raise RlsError(f"RLS: a 'true' condition must be exactly {{'true': true}}, got {condition!r}")
        return exp.true()

    if 'false' in condition:
        if condition.get('false') is not True or len(condition) != 1:
            raise RlsError(f"RLS: a 'false' condition must be exactly {{'false': true}}, got {condition!r}")
        return exp.false()

    for combinator in ('and', 'or'):
        if combinator not in condition:
            continue
        if len(condition) != 1:
            raise RlsError(f"RLS: a {combinator!r} condition must not have other keys, got {condition!r}")
        branches = condition[combinator]
        if not isinstance(branches, Sequence) or isinstance(branches, (str, bytes)) or len(branches) < 2:
            raise RlsError(f"RLS: {combinator!r} must be a list of at least 2 conditions, got {branches!r}")
        compiled = [_compile_primitive(branch, dialect=dialect, identity=identity) for branch in branches]
        result = compiled[0]
        for branch_expr in compiled[1:]:
            result = result.and_(branch_expr) if combinator == 'and' else result.or_(branch_expr)
        return result

    column_name = condition.get('column')
    op = condition.get('op')
    if not isinstance(column_name, str) or not column_name:
        raise RlsError(f"RLS: condition is missing a valid 'column': {condition!r}")
    if not isinstance(op, str):
        raise RlsError(f"RLS: condition is missing a valid 'op': {condition!r}")
    # Quoted so the name is taken exactly as the policy spells it: Keboola creates Snowflake columns
    # with their exact (usually lower-case) names, and an unquoted `id` would fold to `ID` and not exist.
    column = exp.column(column_name, quoted=True)

    if op in _COMPARISON_OPS:
        if 'value' not in condition:
            raise RlsError(f"RLS: op {op!r} requires a 'value': {condition!r}")
        raw_value = condition['value']
        if isinstance(raw_value, Mapping):
            email = _identity_field(raw_value, 'email', identity, condition)
            if not email:
                return exp.false()
            raw_value = email
        value = exp.convert(raw_value)
        return _COMPARISON_OPS[op](column, value)

    if op in ('in', 'not_in'):
        values = condition.get('values')
        if isinstance(values, Mapping):
            values = list(_identity_field(values, 'groups', identity, condition))
            if not values:
                return exp.false()
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or not values:
            raise RlsError(f"RLS: op {op!r} requires a non-empty 'values' list: {condition!r}")
        membership = column.isin(*(exp.convert(v) for v in values))
        return membership.not_() if op == 'not_in' else membership

    if op in ('is_null', 'is_not_null'):
        is_null = column.is_(exp.Null())
        return is_null.not_() if op == 'is_not_null' else is_null

    raise RlsError(f'RLS: unknown condition op {op!r}: {condition!r}')


def _identity_field(
    placeholder: Mapping[str, Any], field: str, identity: tuple[str, Sequence[str]] | None, condition: Any
) -> Any:
    """Resolve `{"$identity": field}` against `identity`; anything else in that position is refused."""
    if dict(placeholder) != {'$identity': field}:
        raise RlsError(f'RLS: unsupported placeholder (expected {{"$identity": "{field}"}}): {condition!r}')
    if identity is None:
        raise RlsError(f'RLS: an $identity placeholder needs a reading identity: {condition!r}')
    email, groups = identity
    return email if field == 'email' else tuple(groups)


@dataclasses.dataclass(frozen=True)
class _Rule:
    """A rule matched at read time rather than by a literal principal lookup: it selects by `groups`, or its
    condition has an `$identity` placeholder. `compiled` is the condition compiled once at load when it has no
    placeholder; otherwise None and the condition compiles per identity."""

    principals: frozenset[str]
    groups: frozenset[str]
    condition: Mapping[str, Any]
    compiled: exp.Condition | None = None


@dataclasses.dataclass(frozen=True)
class _RlsPolicy:
    """One `rls-policy` object, evaluated on its own: the conditions of its rules selecting an identity combine
    with OR; when none does, its own `default` applies (identified reader with known groups only), else the
    policy refuses. Several policies on one table combine with AND, so a second policy can only narrow."""

    principals: Mapping[str, exp.Condition | str] = dataclasses.field(default_factory=dict)
    """Folded principal -> the OR of its literal-principal rules without placeholders (compiled at load)."""
    rules: tuple[_Rule, ...] = ()
    default: Mapping[str, Any] | None = None
    default_compiled: exp.Condition | None = None
    """`default` compiled at load when it has no `$identity` placeholder."""


@dataclasses.dataclass(frozen=True)
class _ClsPolicy:
    """One `cls-policy` object: the union of the visible columns of its rules selecting an identity, or a
    refusal when none does. Several policies on one table intersect, so a second policy can only narrow."""

    principals: Mapping[str, tuple[str, ...]] = dataclasses.field(default_factory=dict)
    group_rules: tuple[tuple[frozenset[str], tuple[str, ...]], ...] = ()


def _policy_table_key(data: Mapping[str, Any], *, label: str, obj_id: str, dialect: str) -> tuple[str, str]:
    """`(rules key, table id as authored)` of a policy object, or `RlsError` for an invalid `table`."""
    table_key_raw = data.get('table')
    if not isinstance(table_key_raw, str) or not _RULE_KEY_RE.fullmatch(table_key_raw):
        raise RlsError(f"{label}: metastore object '{obj_id}' has an invalid 'table': {table_key_raw!r}")
    bucket, _, table = table_key_raw.rpartition('.')
    if not bucket or not table:
        raise RlsError(
            f"{label}: metastore object '{obj_id}' has an unqualified table '{table_key_raw}': must be <bucket>.<table>"
        )
    return _rule_key(_normalize_schema(bucket, dialect), table, dialect), table_key_raw


def _dialect_refusal(data: Mapping[str, Any], *, label: str, obj_id: str, key: str, dialect: str) -> str | None:
    """Why a policy cannot be applied in a `dialect` workspace, or None. `dialect` is optional since 1.1.0."""
    obj_dialect = data.get('dialect')
    if obj_dialect is None or obj_dialect == dialect:
        return None
    return f"{label}: metastore object '{obj_id}' for table '{key}' is for dialect {obj_dialect!r}, the workspace is {dialect!r}"


@dataclasses.dataclass(frozen=True)
class RewrittenQuery:
    sql: str
    applied_rules: list[str]
    """Disclosure for the caller: the rules key of every table that was filtered, de-duplicated.

    Keys only, never the predicate. The point of the disclosure is "this result is a slice, and here
    is which tables were sliced" -- the predicate text is the admin's policy, and handing it to a
    model (which relays it to the user, and which may be trying to work out what it is not allowed
    to see) discloses the shape of the data that was withheld. The server log has the detail.
    """


@dataclasses.dataclass(frozen=True)
class RlsRules:
    """RLS rules keyed by table key, then lower-cased principal name.

    A table key is always `<bucket>.<table>` (`in.c-crm.invoices`) -- the table part is the text
    after the LAST dot, because a Keboola bucket name contains dots of its own. There is no bare-name
    form: a policy must say which bucket it protects, and a query must say which bucket it reads, or
    neither side can be sure they mean the same table.

    Only the bucket and table parts are matched. A reference may also carry a database/project name
    (`OTHER_DB."in.c-crm"."orders"`, `` `proj`.`in_c_crm`.`invoices` ``) and that part is ignored: the
    workspace credentials reach exactly one project database, so a policy keyed by bucket and table
    cannot be side-stepped by naming a different database in front of it.

    `dialect` is the workspace backend the predicates were compiled for; it is not a default but a
    pin, and `rewrite_query()` refuses to run these rules against any other backend. It also decides
    two things about the keys, each following that backend's own name resolution: how the bucket part
    is spelled (BigQuery datasets take neither dots nor hyphens, so bucket `in.c-crm` is dataset
    `in_c_crm` -- normalised at load, see `_normalize_schema`) and whether case matters (it does on
    BigQuery, where table and dataset names are case-sensitive; it does not on Snowflake -- see
    `_rule_key`).
    """

    tables: Mapping[str, Mapping[str, str]]
    dialect: str
    table_ids: Mapping[str, str] = dataclasses.field(default_factory=dict)
    """Rules key -> the table's original Keboola id (`<bucket>.<table>`, as authored -- e.g. still
    `in.c-crm.invoices` even on BigQuery, where the rules key itself is normalised to
    `in_c_crm.invoices`, see `_normalize_schema`). `rewrite_query()` never reads this: it exists
    only so a caller doing a schema-drift check (`referenced_columns()`, `tools/sql.py`) can turn a
    matched rules key back into an id `StorageClient.table_detail()` accepts. Defaulted so every
    existing direct `RlsRules(tables=..., dialect=...)` construction (tests, mainly) keeps working
    unchanged -- an empty mapping here just means "no drift check possible for this instance",
    never a functional difference to `rewrite_query()` itself.
    """
    policies: Mapping[str, tuple[_RlsPolicy, ...]] = dataclasses.field(default_factory=dict)
    """Rules key -> every policy object on the table, evaluated one by one and combined with AND (see
    `_RlsPolicy`). This is what `predicate_for` enforces. A key without an entry (a direct
    `RlsRules(tables=...)` construction) is one implicit policy made of `tables[key]`. `tables` itself stays the
    governed-table index and a per-principal summary (the AND of each policy's literal-principal predicate)."""
    refused: Mapping[str, str] = dataclasses.field(default_factory=dict)
    """Rules key -> why every read of that table is refused, e.g. a policy authored for the other dialect.
    Refusing the one table keeps every other governed table of the project readable."""

    @classmethod
    def from_metastore(cls, objects: Sequence[Any], *, dialect: str, project_id: int) -> 'RlsRules':
        """Build `RlsRules` from `rls-policy` metastore objects (`clients.metastore.MetastoreObject`)
        listed for `project_id`. Raises `RlsError` on any problem with an object's own shape.

        Every object in `objects` applies: the SA-verified, project-scoped metastore list already contains
        exactly what this project owns, was granted, or inherits at organization scope (see
        `_applies_to_project` for why the source/target fields are not filtered on again).

        Every rule's `condition` is compiled via `_compile_primitive` -- never parsed from a
        hand-written predicate string, unlike the file-based pilot this superseded.
        """
        if dialect not in _SUPPORTED_DIALECTS:
            raise RlsError(f'RLS: unsupported workspace dialect {dialect!r}')
        table_ids: dict[str, str] = {}
        policies: dict[str, list[_RlsPolicy]] = {}
        refused: dict[str, str] = {}
        for obj in objects:
            obj_id = getattr(obj, 'id', None) or '<unknown>'
            if not _applies_to_project(obj, label='RLS', obj_id=obj_id):
                continue
            data = getattr(obj, 'attributes', None)
            if not isinstance(data, Mapping):
                raise RlsError(f"RLS: metastore object '{obj_id}' has no attributes")
            key, table_key_raw = _policy_table_key(data, label='RLS', obj_id=obj_id, dialect=dialect)
            table_ids[key] = table_key_raw
            if (reason := _dialect_refusal(data, label='RLS', obj_id=obj_id, key=key, dialect=dialect)) is not None:
                # Optional since schema 1.1.0 (absent = the workspace backend). Predicates are never transpiled,
                # so a policy written for the other backend cannot be applied -- but only ITS table is refused;
                # the rest of the project's policies keep working.
                refused[key] = reason
            default = data.get('default')
            default_compiled: exp.Condition | None = None
            if default is not None:
                if _uses_identity(default):
                    _compile_primitive(default, dialect=dialect, identity=_VALIDATION_IDENTITY)
                else:
                    default_compiled = _compile_primitive(default, dialect=dialect)
            rules_raw = data.get('rules')
            if not isinstance(rules_raw, Sequence) or isinstance(rules_raw, (str, bytes)) or not rules_raw:
                raise RlsError(f"RLS: metastore object '{obj_id}' has no rules")
            principal_conditions: dict[str, list[exp.Condition]] = {}
            rules: list[_Rule] = []
            for rule in rules_raw:
                if not isinstance(rule, Mapping):
                    raise RlsError(f"RLS: metastore object '{obj_id}' has an invalid rule entry: {rule!r}")
                principals, groups = _rule_selector(rule, label='RLS', obj_id=obj_id)
                condition = rule.get('condition')
                if _uses_identity(condition):
                    # Compiled per identity; validated here so a malformed rule is refused at load.
                    _compile_primitive(condition, dialect=dialect, identity=_VALIDATION_IDENTITY)
                    rules.append(_Rule(frozenset(principals), frozenset(groups), condition))
                    continue
                compiled = _compile_primitive(condition, dialect=dialect)
                if groups:
                    rules.append(_Rule(frozenset(), frozenset(groups), condition, compiled))
                    continue
                for user_key in principals:
                    # Schema 1.1.0: every rule of the policy matching one identity applies, combined with OR.
                    principal_conditions.setdefault(user_key, []).append(compiled)
            policies.setdefault(key, []).append(
                _RlsPolicy(
                    principals={
                        user_key: _combine(conditions, op='or', dialect=dialect)
                        for user_key, conditions in principal_conditions.items()
                    },
                    rules=tuple(rules),
                    default=default,
                    default_compiled=default_compiled,
                )
            )

        tables: dict[str, dict[str, str]] = {}
        for key, key_policies in policies.items():
            per_principal: dict[str, list[exp.Condition | str]] = {}
            for policy in key_policies:
                for user_key, condition in policy.principals.items():
                    per_principal.setdefault(user_key, []).append(condition)
            tables[key] = {
                user_key: _as_sql(_combine(conditions, op='and', dialect=dialect), dialect=dialect)
                for user_key, conditions in per_principal.items()
            }

        LOG.info(
            f'Loaded RLS rules for {len(tables)} table(s) from the metastore (dialect {dialect}, project {project_id})'
        )
        return cls(
            tables=tables,
            dialect=dialect,
            table_ids=table_ids,
            policies={key: tuple(key_policies) for key, key_policies in policies.items()},
            refused=refused,
        )

    def _policies(self, key: str) -> tuple[_RlsPolicy, ...]:
        """The policies on `key`; a direct `RlsRules(tables=...)` construction is one implicit policy."""
        return self.policies.get(key) or (_RlsPolicy(principals=self.tables.get(key, {})),)

    def referenced_columns(self, keys: Iterable[str] | None = None) -> dict[str, set[str]]:
        """Every column name a rule's condition (or a policy `default`) mentions, per rules key.

        Diagnostic input only, for the schema-drift check in `tools/sql.py` (see
        `feature_spec/rls_query_tool/RFC.md` "Data-contract maturity") -- this says what a policy
        *claims* to reference, nothing about whether that column still exists; this module has no
        network access and stays that way. `keys` limits the work to those rules keys (the tables one
        query touched); None means every governed table. Compiled trees are read directly; only a
        predicate given as SQL text (direct construction) is parsed.
        """
        result: dict[str, set[str]] = {}
        for key in self.tables if keys is None else [k for k in keys if k in self.tables]:
            columns: set[str] = set()
            for policy in self._policies(key):
                trees: list[exp.Expression] = []
                for condition in policy.principals.values():
                    if isinstance(condition, str):
                        try:
                            condition = sqlglot.parse_one(condition, dialect=self.dialect, into=exp.Condition)
                        except sqlglot.errors.SqlglotError:
                            continue  # never fatal for a diagnostic
                    trees.append(condition)
                for rule in policy.rules:
                    trees.append(
                        rule.compiled
                        if rule.compiled is not None
                        else _compile_primitive(rule.condition, dialect=self.dialect, identity=_VALIDATION_IDENTITY)
                    )
                if policy.default is not None:
                    trees.append(
                        policy.default_compiled
                        if policy.default_compiled is not None
                        else _compile_primitive(policy.default, dialect=self.dialect, identity=_VALIDATION_IDENTITY)
                    )
                for tree in trees:
                    columns.update(col.name for col in tree.find_all(exp.Column))
            result[key] = columns
        return result

    def is_governed(self, *, table_name: str, schema: str | None) -> bool:
        """Whether any policy at all applies to this table (regardless of user).

        `rewrite_query()` calls this before `predicate_for()` for every table it sees: a table
        this returns `False` for has *no* applicable `rls-policy` object and is left completely
        untouched (RLS is opt-in per table, not a blanket switch -- see the RFC). A missing
        `schema` also means "not governed": there is no key to look up, and a bare table name is
        ordinary `query_data` territory, not an RLS concern.
        """
        return bool(schema) and _rule_key(schema, table_name, self.dialect) in self.tables

    def governs_table_id(self, table_id: str) -> bool:
        """Whether a row-level policy applies to the table with Storage id `<bucket>.<table>`, for
        metadata views (a governed table's true row count / size must not be shown)."""
        bucket, _, name = table_id.rpartition('.')
        return bool(bucket) and _rule_key(_normalize_schema(bucket, self.dialect), name, self.dialect) in self.tables

    def governs_bucket_id(self, bucket_id: str) -> bool:
        """Whether a row-level policy applies to any table of the bucket, for metadata views (a bucket's
        aggregate size would reveal the rows hidden from a table inside it)."""
        prefix = _rule_key(_normalize_schema(bucket_id, self.dialect), 't', self.dialect).rpartition('.')[0]
        return any(key.rpartition('.')[0] == prefix for key in self.tables)

    def predicate_for(
        self, *, table_name: str, schema: str | None, user: str, groups: Sequence[str] | None = None
    ) -> tuple[str, str]:
        """Return `(matched_key, predicate)` for the table/identity, or raise `RlsError`.

        Each policy on the table is evaluated on its own: the conditions of its rules selecting the identity
        (its email, or one of its `groups`) combine with OR; when none does, the policy's `default` applies,
        else the read is refused. The policies' predicates then combine with AND, so a second policy can only
        narrow what another allows.

        `user` empty = no identity: refused before any rule or default is looked at. `groups` None = the
        caller's group source is unknown (an MCP session today): group rules never match and no `default`
        applies, because a default may be meant only for readers outside some group. `()` = known to have none.

        Only call this once `is_governed()` is true for the same table -- a table with no policy
        at all is not this method's job to reject or admit, see `is_governed()`. `schema` is the
        bucket/dataset the reference names; without it there is no key to look up and the
        reference is refused. There is deliberately no fall-back to a bare table name: a rule that
        matched `invoices` in every bucket would silently cover tables its author never saw.
        """
        if not schema:
            raise RlsError(f"RLS: table reference must be qualified as <bucket>.<table>: '{table_name}'")
        key = _rule_key(schema, table_name, self.dialect)
        if key not in self.tables:
            raise RlsError(f"RLS: no rule for table '{key}'")
        if (reason := self.refused.get(key)) is not None:
            LOG.warning(reason)
            raise RlsError(_RULE_NOT_APPLIED.format(key=key))
        if not user:
            # No identity degrades to no protected data, never to what a group rule or a default hands out.
            LOG.info(f"RLS: no identity for table '{key}'")
            raise RlsAccessDenied(ACCESS_DENIED_MESSAGE)
        folded = _fold_principal(user)
        identity = (user, tuple(groups or ()))
        per_policy: list[exp.Condition | str] = []
        for policy in self._policies(key):
            conditions: list[exp.Condition | str] = []
            if folded in policy.principals:
                conditions.append(policy.principals[folded])
            for rule in policy.rules:
                if _selects(rule.principals, rule.groups, folded_user=folded, user_groups=groups):
                    conditions.append(
                        rule.compiled
                        if rule.compiled is not None
                        else _compile_primitive(rule.condition, dialect=self.dialect, identity=identity)
                    )
            if not conditions and policy.default is not None and groups is not None:
                conditions.append(
                    policy.default_compiled
                    if policy.default_compiled is not None
                    else _compile_primitive(policy.default, dialect=self.dialect, identity=identity)
                )
            if not conditions:
                LOG.info(f"RLS: no rule for user '{folded}' on table '{key}'")
                raise RlsAccessDenied(ACCESS_DENIED_MESSAGE)
            per_policy.append(_combine(conditions, op='or', dialect=self.dialect))
        return key, _as_sql(_combine(per_policy, op='and', dialect=self.dialect), dialect=self.dialect)


@dataclasses.dataclass(frozen=True)
class ClsRules:
    """Column-level security rules keyed by table key, then lower-cased principal name.

    Same key derivation, dialect-pinning, and org-authored/metastore-backed model as `RlsRules` --
    see its docstring for the shared parts (table-key shape, BigQuery schema normalisation, case
    handling). The one structural difference: a rule here is an allowlist of column names
    (`visible_columns`), not a compiled predicate -- there is no boolean logic to combine, only "is
    this column in the list." Deliberately an allowlist, not a denylist of hidden columns: a column
    added to a protected table later defaults to hidden until a rule names it, never silently
    exposed -- the same posture `_ALLOWED_FROM_SOURCES`/`_COMPARISON_OPS` already take elsewhere in
    this module. `rewrite_query()` composes an `RlsRules` and a `ClsRules` into the *same* wrapper
    subquery per table, never two separate rewrite passes -- see
    `feature_spec/rls_query_tool/RFC.md` "Column-Level Security".

    A sibling dataclass to `RlsRules`, not a field on it: the two object types have different authorship
    triggers and lifecycles (see the RFC's "Schema stability" section). The loaders share the table-key,
    dialect and selector checks (`_policy_table_key`, `_dialect_refusal`, `_rule_selector`).
    """

    tables: Mapping[str, Mapping[str, tuple[str, ...]]]
    dialect: str
    table_ids: Mapping[str, str] = dataclasses.field(default_factory=dict)
    """Same purpose as `RlsRules.table_ids` -- see there."""
    policies: Mapping[str, tuple[_ClsPolicy, ...]] = dataclasses.field(default_factory=dict)
    """Rules key -> every policy object on the table; their column sets intersect (see `_ClsPolicy`). A key
    without an entry (a direct `ClsRules(tables=...)` construction) is one implicit policy made of
    `tables[key]`. `tables` stays the governed-table index and a per-principal summary (union of columns)."""
    refused: Mapping[str, str] = dataclasses.field(default_factory=dict)
    """Same purpose as `RlsRules.refused` -- see there."""

    @classmethod
    def from_metastore(cls, objects: Sequence[Any], *, dialect: str, project_id: int) -> 'ClsRules':
        """Build `ClsRules` from `cls-policy` metastore objects applicable to `project_id`.

        Mirrors `RlsRules.from_metastore` (same applicability check, `<bucket>.<table>` key derivation,
        selectors and per-table dialect refusal) -- the only difference is validating `visible_columns`
        (a non-empty list of column-name strings) instead of compiling a `condition`. Within one policy
        several rules for one identity union their columns; several policies intersect.
        """
        if dialect not in _SUPPORTED_DIALECTS:
            raise RlsError(f'CLS: unsupported workspace dialect {dialect!r}')
        tables: dict[str, dict[str, tuple[str, ...]]] = {}
        table_ids: dict[str, str] = {}
        policies: dict[str, list[_ClsPolicy]] = {}
        refused: dict[str, str] = {}
        for obj in objects:
            obj_id = getattr(obj, 'id', None) or '<unknown>'
            if not _applies_to_project(obj, label='CLS', obj_id=obj_id):
                continue
            data = getattr(obj, 'attributes', None)
            if not isinstance(data, Mapping):
                raise RlsError(f"CLS: metastore object '{obj_id}' has no attributes")
            key, table_key_raw = _policy_table_key(data, label='CLS', obj_id=obj_id, dialect=dialect)
            table_ids[key] = table_key_raw
            users = tables.setdefault(key, {})
            if (reason := _dialect_refusal(data, label='CLS', obj_id=obj_id, key=key, dialect=dialect)) is not None:
                refused[key] = reason
            rules_raw = data.get('rules')
            if not isinstance(rules_raw, Sequence) or isinstance(rules_raw, (str, bytes)) or not rules_raw:
                raise RlsError(f"CLS: metastore object '{obj_id}' has no rules")

            policy_principals: dict[str, tuple[str, ...]] = {}
            policy_group_rules: list[tuple[frozenset[str], tuple[str, ...]]] = []
            for rule in rules_raw:
                if not isinstance(rule, Mapping):
                    raise RlsError(f"CLS: metastore object '{obj_id}' has an invalid rule entry: {rule!r}")
                principals, groups = _rule_selector(rule, label='CLS', obj_id=obj_id)
                columns_raw = rule.get('visible_columns')
                if not isinstance(columns_raw, Sequence) or isinstance(columns_raw, (str, bytes)) or not columns_raw:
                    raise RlsError(f"CLS: metastore object '{obj_id}' has an invalid 'visible_columns': {rule!r}")
                columns: list[str] = []
                for col in columns_raw:
                    if not isinstance(col, str) or not _RULE_KEY_RE.fullmatch(col):
                        raise RlsError(f"CLS: metastore object '{obj_id}' has an invalid column name {col!r}")
                    columns.append(col)
                if groups:
                    policy_group_rules.append((frozenset(groups), tuple(columns)))
                for user_key in principals:
                    # Schema 1.1.0: several rules of one policy for one identity union their visible columns.
                    policy_principals[user_key] = tuple(dict.fromkeys((*policy_principals.get(user_key, ()), *columns)))
                    users[user_key] = tuple(dict.fromkeys((*users.get(user_key, ()), *columns)))
            policies.setdefault(key, []).append(
                _ClsPolicy(principals=policy_principals, group_rules=tuple(policy_group_rules))
            )

        LOG.info(
            f'Loaded CLS rules for {len(tables)} table(s) from the metastore (dialect {dialect}, project {project_id})'
        )
        return cls(
            tables=tables,
            dialect=dialect,
            table_ids=table_ids,
            policies={key: tuple(key_policies) for key, key_policies in policies.items()},
            refused=refused,
        )

    def _policies(self, key: str) -> tuple[_ClsPolicy, ...]:
        """The policies on `key`; a direct `ClsRules(tables=...)` construction is one implicit policy."""
        return self.policies.get(key) or (_ClsPolicy(principals=self.tables.get(key, {})),)

    def referenced_columns(self, keys: Iterable[str] | None = None) -> dict[str, set[str]]:
        """Every column name any rule allowlists, per rules key -- the CLS analogue of
        `RlsRules.referenced_columns()` (same `keys` narrowing), for the same schema-drift check in
        `tools/sql.py`. No parsing needed here: the allowlist already is the column list.
        """
        return {
            key: {
                col
                for policy in self._policies(key)
                for columns in (*policy.principals.values(), *(cols for _, cols in policy.group_rules))
                for col in columns
            }
            for key in (self.tables if keys is None else [k for k in keys if k in self.tables])
        }

    def is_governed(self, *, table_name: str, schema: str | None) -> bool:
        """Whether any policy at all applies to this table (regardless of user) -- see
        `RlsRules.is_governed`, identical semantics."""
        return bool(schema) and _rule_key(schema, table_name, self.dialect) in self.tables

    def _columns(self, key: str, *, user: str | None, groups: Sequence[str] | None) -> tuple[str, ...] | None:
        """The columns the identity may see, or None (refuse).

        Per policy: the union of the visible columns of its rules selecting the identity; a policy with no
        such rule refuses. Across policies: the intersection, in the first policy's column order -- an empty
        intersection refuses too (there is no column to show). `user` empty = no identity, refused before any
        rule is looked at; `groups` None = unknown, so group rules never match (see `RlsRules.predicate_for`).
        """
        if not user:
            return None
        folded = _fold_principal(user)
        result: tuple[str, ...] | None = None
        for policy in self._policies(key):
            matched: list[tuple[str, ...]] = [policy.principals[folded]] if folded in policy.principals else []
            if groups is not None:
                matched += [
                    columns for rule_groups, columns in policy.group_rules if not rule_groups.isdisjoint(groups)
                ]
            if not matched:
                return None
            union = tuple(dict.fromkeys(col for columns in matched for col in columns))
            result = union if result is None else tuple(col for col in result if col in union)
        return result or None

    def visible_columns(
        self, *, table_id: str, user: str | None, groups: Sequence[str] | None = None
    ) -> tuple[str, ...] | None:
        """The columns `user` may see of the table with Storage id `<bucket>.<table>`, for metadata views.

        `None` = no policy governs the table (every column is visible). Fail closed otherwise: a
        governed table with no rule for `user` (or no resolvable identity) shows no columns at all.
        """
        bucket, _, name = table_id.rpartition('.')
        key = _rule_key(_normalize_schema(bucket, self.dialect), name, self.dialect) if bucket else None
        if key is None or key not in self.tables:
            return None
        if key in self.refused:
            return ()
        return self._columns(key, user=user, groups=groups) or ()

    def columns_for(
        self, *, table_name: str, schema: str | None, user: str, groups: Sequence[str] | None = None
    ) -> tuple[str, tuple[str, ...]]:
        """Return `(matched_key, visible_columns)` for the table/user, or raise `RlsError` -- see
        `RlsRules.predicate_for`, identical fail-closed semantics (governed table, no rule for this
        principal -> refuse, never fall back to every column)."""
        if not schema:
            raise RlsError(f"CLS: table reference must be qualified as <bucket>.<table>: '{table_name}'")
        key = _rule_key(schema, table_name, self.dialect)
        if key not in self.tables:
            raise RlsError(f"CLS: no rule for table '{key}'")
        if (reason := self.refused.get(key)) is not None:
            LOG.warning(reason)
            raise RlsError(_RULE_NOT_APPLIED.format(key=key))
        columns = self._columns(key, user=user, groups=groups)
        if columns is None:
            LOG.info(f"CLS: no rule for user '{_fold_principal(user)}' on table '{key}'")
            raise RlsAccessDenied(ACCESS_DENIED_MESSAGE)
        return key, columns


# BigQuery's legacy per-dataset meta tables (`__TABLES__`, `__TABLES_SUMMARY__`, `__PARTITIONS_SUMMARY__`): readable with
# ordinary dataset access, and they report true row counts and sizes for every table of the dataset.
_BIGQUERY_META_TABLE_RE = re.compile(r'__[A-Z_]+__')


def _reads_system_metadata(tree: exp.Expression) -> bool:
    """Whether a statement reads a source that describes tables or earlier queries rather than data.

    * Query history (`QUERY_HISTORY*`, BigQuery `INFORMATION_SCHEMA.JOBS*`) returns the SQL text of earlier queries,
      which is the REWRITTEN text -- the predicate a policy injected -- so it would disclose what the rewrite is
      meant to keep silent.
    * `INFORMATION_SCHEMA` views, Snowflake's `SNOWFLAKE` system database (`ACCOUNT_USAGE`, ...) and BigQuery's
      `__TABLES__`-style meta tables describe every table regardless of any policy: real row counts and sizes
      (which the table metadata hides) and the names of columns a column policy withholds.
    """
    for table in tree.find_all(exp.Table):
        parts = [part.upper() for part in (table.catalog, table.db, table.name) if part]
        qualified = '.'.join(parts)
        if any(
            marker in qualified
            for marker in ('QUERY_HISTORY', 'INFORMATION_SCHEMA', 'ACCOUNT_USAGE', 'ORGANIZATION_USAGE')
        ):
            return True
        if 'SNOWFLAKE' in parts[:-1] or (parts and _BIGQUERY_META_TABLE_RE.fullmatch(parts[-1])):
            return True
    return False


def _is_wildcard_table(table: exp.Table, *, dialect: str) -> bool:
    """A BigQuery wildcard table (`dataset.prefix*`) expands to every matching table, governed ones included,
    but names none of them -- so no policy key can be derived from it."""
    return dialect == 'bigquery' and any(
        '*' in part or '?' in part for part in (table.catalog, table.db, table.name) if part
    )


def _check_from_sources(tree: exp.Expression) -> None:
    """Refuse any FROM/JOIN/LATERAL source that is not on `_ALLOWED_FROM_SOURCES`.

    Allowlist, not denylist: the rewrite can only protect what it recognises, so an unknown source
    type means "no data", never "pass it through".
    """
    for clause in itertools.chain(tree.find_all(exp.From), tree.find_all(exp.Join), tree.find_all(exp.Lateral)):
        # Every source of the clause, not just the first: depending on the sqlglot version the extra
        # comma-separated sources of a `FROM a, b` live in `From.expressions` rather than in `Join` nodes,
        # and one that is not checked would reach the warehouse untouched next to a filtered table.
        sources = (
            [clause.this, *(clause.args.get('expressions') or [])] if isinstance(clause, exp.From) else [clause.this]
        )
        for source in sources:
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


def _identifier_key(text: str, quoted: bool, *, dialect: str) -> tuple[str, bool]:
    """Reduce a CTE name to a comparison key that matches how the engine resolves it.

    The two backends do not agree, and the difference decides whether a reference binds to the CTE
    or falls through to the base table -- so it is keyed on the dialect rather than guessed:

    * Snowflake folds unquoted identifiers to a canonical case but treats quoted ones literally, so
      `"secret"` and `"SECRET"` are two different names while `secret` and `SECRET` are one.
      Comparing everything lower-cased would bind a quoted reference to a differently-cased quoted
      CTE that the engine resolves to the *base table* -- and pass that table through unfiltered.
      Quoted and unquoted forms of the same text stay distinct too (Ruling 14); the caller decides
      whether that mismatch is a refusal or merely a non-match.
    * BigQuery resolves CTE names case-insensitively and backticks around one carry no meaning at
      all (verified against a live workspace): `WITH secret AS (...) SELECT * FROM \\`SECRET\\`` reads the
      CTE. Keeping Snowflake's rule there would refuse ordinary queries as "ambiguous" while
      protecting nothing. (BigQuery *table* and *dataset* names are case-sensitive -- that is a
      separate question, settled by the rules-key lookup, not here.)
    """
    if dialect == 'bigquery':
        return text.lower(), False
    return text if quoted else text.lower(), quoted


def _cte_key(cte: exp.CTE, *, dialect: str) -> tuple[str, bool]:
    """A CTE declaration as an `_identifier_key` -- the same shape a table reference is reduced to
    in `_is_cte_reference`."""
    alias = cte.args.get('alias')
    identifier = alias.this if isinstance(alias, exp.TableAlias) else None
    quoted = isinstance(identifier, exp.Identifier) and identifier.quoted
    return _identifier_key(cte.alias_or_name, quoted, dialect=dialect)


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


def _cte_names_in_scope(node: exp.Expression, *, dialect: str) -> set[tuple[str, bool]]:
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
                names.add(_cte_key(cte, dialect=dialect))
                if cte is child:
                    break
        elif (with_clause := _with_clause(parent)) is not None and with_clause is not child:
            names.update(_cte_key(cte, dialect=dialect) for cte in with_clause.expressions)
        child, parent = parent, parent.parent
    return names


def _is_non_recursive_self_reference(node: exp.Table, key: tuple[str, bool], *, dialect: str) -> bool:
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
            and any(cte is child and _cte_key(cte, dialect=dialect) == key for cte in parent.expressions)
        ):
            return True
        child, parent = parent, parent.parent
    return False


def _is_cte_reference(node: exp.Table, cte_names: set[str], *, dialect: str) -> bool:
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
    key = _identifier_key(node.name, node.this.quoted, dialect=dialect)
    in_scope = _cte_names_in_scope(node, dialect=dialect)
    if key in in_scope:
        return True
    if any(scoped_name == key[0] for scoped_name, _ in in_scope):
        raise RlsError(f'RLS: ambiguous CTE reference, quoting differs from the declaration: {node.name}')
    if _is_non_recursive_self_reference(node, key, dialect=dialect):
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


def _check_functions(tree: exp.Expression, cte_names: set[str], *, dialect: str) -> None:
    """Refuse function calls that RLS cannot reason about; raise `RlsError` if any is present.

    Two bans, both allowlist-shaped where it matters:

    * `SYSTEM$...`, anything under `CORTEX` and the catalog/stage metadata functions
      (`_METADATA_FUNC_NAMES`) are refused wherever they appear. They read metadata,
      cancel queries or hand text to an LLM -- none of which the row filter constrains, however
      thoroughly the FROM clause is rewritten.
    * A query with no real table to filter (`SELECT GET_DDL(...)`, or the same thing dressed up with
      a dummy CTE) is not a data query at all: whatever it returns, no predicate shaped it. Only a
      small set of clock functions and pure scalar expressions is allowed there.
    """
    for node in tree.find_all(exp.Anonymous):
        name = _function_name(node)
        parts = [part.upper() for part in name.split('.')]
        if (
            any(part.startswith('SYSTEM$') for part in parts)
            or 'CORTEX' in name.upper()
            or parts[-1] in _METADATA_FUNC_NAMES
        ):
            raise RlsError(f'RLS: function call is not allowed: {name}')

    # An `exp.Table` naming a CTE in scope is not a real table. Resolution goes through
    # `_is_cte_reference` rather than the cheap name set so that a name which only *looks* like a
    # CTE is refused with the reason it deserves ("declared in another scope") instead of being
    # counted as a non-table here and reported as a stray function call.
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier) or not _is_cte_reference(table, cte_names, dialect=dialect):
            return  # there is a real table here; the rewrite will filter it

    for node in tree.find_all(exp.Func):
        if isinstance(node, _FROMLESS_ALLOWED_FUNC_TYPES):
            continue
        name = _function_name(node)
        if name.upper() in _FROMLESS_ALLOWED_FUNC_NAMES:
            continue
        raise RlsError(f'RLS: function calls are not allowed in a query without FROM: {name}')


def _matching_key(table: exp.Table, keys: Mapping[str, str], *, dialect: str) -> str | None:
    """The rules key `table` was wrapped under -- built exactly as `predicate_for` builds it."""
    if not table.db:
        return None
    key = _rule_key(table.db, table.name, dialect)
    return key if key in keys else None


def _check_output(
    sql: str,
    *,
    dialect: str,
    predicates: Mapping[str, str],
    columns: Mapping[str, tuple[str, ...] | None] | None = None,
) -> None:
    """Assert the generated SQL is still a plain SELECT over wrapped tables; raise `RlsError` if not.

    This is the safety net: it re-parses the rewriter's own output and checks it from scratch, so a
    bug or an unforeseen node type upstream cannot smuggle DDL, a second statement or an unfiltered
    *governed* table past it. The only shape the rewrite ever produces for a table a policy names
    is `(SELECT <cols> FROM <table> WHERE <predicate>) AS <alias>`; anything else there is a defect,
    not data. An ungoverned table (no policy names it at all) is untouched by design and may appear
    in any shape -- this check only ever looks at tables matching a key in `predicates`.

    `predicates` maps each rules key the rewrite matched to the predicate text it inserted for it --
    `'TRUE'` for a key only CLS governs, see `rewrite_query`. A wrapper is only accepted when its
    WHERE is present AND generates back to exactly that predicate: "wrapped in something" is not the
    invariant, "wrapped in the filter the admin wrote" is. Without the comparison a wrapper carrying
    a weakened or empty condition would pass.

    `columns` maps each matched key to the exact `visible_columns` a CLS rule allowlisted for it, or
    `None` (the default, via `.get()`, when a key isn't present at all -- true for every call site
    that never dealt with CLS) meaning "expect a plain `SELECT *`". Same all-or-nothing comparison
    as the predicate: the wrapper's SELECT list must generate back to exactly the expected columns,
    in the same order, never a superset/subset fuzz match.

    Consequence worth knowing when authoring rules: a predicate that itself references another table
    (`id IN (SELECT id FROM other)`) leaves a table outside a wrapper and is refused. Predicates must
    be plain conditions over the protected table's own columns.
    """
    columns = columns or {}
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
        if _is_cte_reference(table, cte_names, dialect=dialect):
            continue  # reference to a CTE in scope here, not a real table
        key = _matching_key(table, predicates, dialect=dialect)
        if key is None:
            # No policy governs this table -- it was deliberately left untouched by `_transform`
            # (RLS is opt-in per table, see `RlsRules.is_governed`), so there is nothing here to
            # verify: an ungoverned table may appear in any shape, wrapped or not.
            continue
        select = table.parent.parent if isinstance(table.parent, exp.From) else None
        expected_cols = columns.get(key)
        if expected_cols is None:
            # No CLS rule matched this key -- the wrapper must carry a plain, untouched `SELECT *`.
            projection_ok = isinstance(select, exp.Select) and [type(e) for e in select.expressions] == [exp.Star]
        else:
            expected_col_sql = [exp.column(c, quoted=True).sql(dialect=dialect) for c in expected_cols]
            projection_ok = (
                isinstance(select, exp.Select)
                and [e.sql(dialect=dialect) for e in select.expressions] == expected_col_sql
            )
        wrapped = projection_ok and isinstance(select, exp.Select) and isinstance(select.parent, exp.Subquery)
        if not wrapped:
            LOG.warning(f'RLS: rewrite left an unwrapped or wrong-projection table reference for governed key {key!r}')
            raise RlsError('RLS: rewrite left an unwrapped table reference')
        assert isinstance(select, exp.Select)  # narrowed by `wrapped`
        where = select.args.get('where')
        if where is None:
            LOG.warning(f'RLS: rewrite left the wrapper around {key!r} without a WHERE clause')
            raise RlsError(_RULE_NOT_APPLIED.format(key=key))
        # A governed table's WHERE must be a plain condition over its own columns -- never a table
        # reference of its own. Checked explicitly here (not left to the generic per-table walk
        # above), because a table embedded in a predicate can itself be bare/unqualified and would
        # otherwise be skipped as "ungoverned" by the `key is None` branch above, the same way any
        # ordinary ungoverned table elsewhere in the query legitimately is.
        if next(where.this.find_all(exp.Table, exp.Subquery, exp.Select), None) is not None:
            LOG.warning(f'RLS: predicate for table {key!r} references another table or subquery')
            raise RlsError(_RULE_NOT_APPLIED.format(key=key))
        try:
            expected = sqlglot.parse_one(predicates[key], dialect=dialect, into=exp.Condition)
        except sqlglot.errors.SqlglotError as e:
            LOG.warning(f'RLS: predicate for table {key!r} is not valid SQL for dialect {dialect!r}: {_clean_error(e)}')
            raise RlsError(_RULE_NOT_APPLIED.format(key=key)) from e
        # Compared as generated text in the same dialect, so the two sides are normalised the same
        # way and only a real difference in the condition can fail this. Neither side is quoted back
        # to the caller: the difference between them IS the predicate.
        if where.this.sql(dialect=dialect) != expected.sql(dialect=dialect):
            LOG.warning(
                f'RLS: rewrite produced a WHERE that is not the rule for table {key!r}: '
                f'{where.this.sql(dialect=dialect)!r} != {expected.sql(dialect=dialect)!r}'
            )
            raise RlsError(_RULE_NOT_APPLIED.format(key=key))


def references_governed_table(sql: str, *, dialect: str, rules: RlsRules, cls_rules: 'ClsRules | None' = None) -> bool:
    """Cheap, lenient pre-check: does `sql` reference any table `rules`/`cls_rules` governs at all?

    Callers (see `tools/sql.py`) use this to decide whether a query needs `rewrite_query()`'s full,
    deliberately paranoid pipeline (single-statement, SELECT-only, allowlisted FROM sources, ...) at
    all: a query that touches zero governed tables is not RLS/CLS's concern and must behave exactly
    like plain, unfiltered `query_data` -- both are opt-in per table, not a blanket restriction on
    every query the moment a project has any policy configured. `cls_rules` is optional so every
    existing RLS-only call site needs no change.

    Lenient about *shape* only where that cannot hide a governed table: it does not require a single
    `SELECT` and ignores ordinary table functions (`LATERAL FLATTEN`, `UNNEST`, ...), so a query that
    touches no governed table behaves exactly like plain `query_data`. It fails CLOSED -- returns
    `True`, routing the query into `rewrite_query()`, which refuses it -- for everything whose
    tables this cannot see: a parse failure (the warehouse may accept syntax sqlglot rejects, or input
    nested too deep to parse), a
    statement that is not a query (`EXECUTE IMMEDIATE`, `CALL`, ... can read any table through
    dynamic SQL), a table named dynamically (`IDENTIFIER(...)`, `RESULT_SCAN(...)`), an unqualified
    table that is not a CTE (no policy key can be derived from it), and a query with no table at all
    (the strict rewrite owns the function allowlist, e.g. against `SYSTEM$CANCEL_ALL_QUERIES()`).
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except (sqlglot.errors.SqlglotError, RecursionError):
        # RecursionError: input nested deeper than the parser can recurse. Same as a parse failure -- the
        # strict rewrite refuses it ("query too deeply nested") instead of this raising past query_data.
        return True
    for statement in statements:
        if statement is None:
            continue
        if (
            not isinstance(statement, exp.Query)
            or _names_table_dynamically(statement)
            or _reads_system_metadata(statement)
        ):
            return True
        tables = list(statement.find_all(exp.Table))
        if any(_is_wildcard_table(t, dialect=dialect) for t in tables):
            return True  # names no table to look a policy up by; the strict rewrite refuses it
        if not tables:
            # No table at all (`SELECT SYSTEM$CANCEL_ALL_QUERIES()`, `GET_DDL(...)`): the strict rewrite owns
            # the function allowlist, so it must see these rather than have them run as ordinary data.
            return True
        cte_names = _cte_names(statement)
        for table in tables:
            if not table.db:
                # Unqualified: it can resolve to a governed table through the workspace's current schema,
                # and no policy key can be derived from it. Only a reference to a CTE declared in its OWN scope
                # chain is harmless (a same-named CTE in a nested or sibling scope must not excuse it); an
                # ambiguous or out-of-scope match, and anything else, goes to the strict rewrite.
                try:
                    if _is_cte_reference(table, cte_names, dialect=dialect):
                        continue
                except RlsError:
                    return True
                return True
            if rules.is_governed(table_name=table.name, schema=table.db) or (
                cls_rules is not None and cls_rules.is_governed(table_name=table.name, schema=table.db)
            ):
                return True
        # The function checks apply to every statement, not only table-less ones: a catalog/system function
        # may name a governed table in its arguments (`SELECT GET_DDL('table', 'in.c-crm.invoices') FROM
        # "in.c-crm"."unrelated"`) and no row filter shapes what it returns. A CTE-only query (no real table)
        # gets the table-less allowlist as well. Plain queries with harmless functions stay untouched.
        try:
            _check_functions(statement, cte_names, dialect=dialect)
        except RlsError:
            return True
    return False


# Table-valued functions that pick the table they read at run time, so its name is never in the SQL.
_DYNAMIC_TABLE_FUNCTIONS = frozenset({'IDENTIFIER', 'RESULT_SCAN'})


def _names_table_dynamically(statement: exp.Expression) -> bool:
    """Whether a statement reads a table whose name is only known at run time."""
    if any(not isinstance(table.this, exp.Identifier) for table in statement.find_all(exp.Table)):
        return True
    if next(statement.find_all(exp.TableFromRows), None) is not None:  # TABLE(...): the table is a function result
        return True
    return any(fn.name.upper() in _DYNAMIC_TABLE_FUNCTIONS for fn in statement.find_all(exp.Anonymous))


def rewrite_query(
    sql: str,
    *,
    user: str,
    dialect: str,
    rules: RlsRules,
    cls_rules: 'ClsRules | None' = None,
    groups: Sequence[str] | None = None,
) -> RewrittenQuery:
    """Rewrite a single SELECT so every table an RLS and/or CLS policy governs becomes a filtered,
    column-restricted subquery; a table no policy of either kind names at all is left completely
    untouched (see `RlsRules.is_governed`/`ClsRules.is_governed`).

    `cls_rules` is optional -- every existing RLS-only caller needs no change. When a table is
    governed by both, RLS's predicate and CLS's column allowlist compose into the *same* wrapper
    (`(SELECT <cols> FROM <table> WHERE <predicate>) AS <alias>`), never two separate rewrite
    passes. A table only CLS governs still gets wrapped, with `WHERE TRUE`; a table only RLS governs
    keeps its `SELECT *`, exactly as before this parameter existed.

    Call this only once `references_governed_table()` has confirmed the query is worth the full,
    paranoid pipeline below -- a query touching zero governed tables should skip this function
    entirely rather than be needlessly restricted to a single SELECT / allowlisted FROM sources.

    :param sql: the caller's SQL, in the workspace dialect
    :param user: identity used to select rules; case-insensitive
    :param groups: the identity's groups (schema 1.1.0 `groups` selectors, `{"$identity": "groups"}`); None when
        the caller's group source is unknown -- group rules then never match and no policy `default` applies
    :param dialect: sqlglot dialect name (`'snowflake'` / `'bigquery'`)
    :param rules: loaded RLS rules
    :param cls_rules: loaded CLS rules, if any
    :raises RlsError: on anything other than one SELECT statement whose every governed table has a
        matching RLS and/or CLS rule for `user`
    """
    try:
        sqlglot.Dialect.get_or_raise(dialect)
    except Exception as e:
        raise RlsError(f'RLS: unsupported SQL dialect {dialect!r}') from e
    # Predicates/column lists are never transpiled, so rules written for one backend must not be
    # applied to another: the same text can mean different things (or silently nothing) under a
    # different dialect. Fail closed rather than rewrite with a filter whose meaning we cannot
    # vouch for.
    if rules.dialect != dialect.lower():
        raise RlsError(f'RLS: rules are for dialect {rules.dialect} but the workspace is {dialect.lower()}')
    if cls_rules is not None and cls_rules.dialect != dialect.lower():
        raise RlsError(f'CLS: rules are for dialect {cls_rules.dialect} but the workspace is {dialect.lower()}')
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

    # A CTE may be named after a protected table -- `WITH orders AS (SELECT * FROM "in.c-crm"."orders"
    # WHERE amount > 0)` is the natural way to write such a query, and refusing it taught callers to
    # go looking for a formulation that slips through instead. It is safe because a rules key names a
    # bucket (see `RlsRules`) and a CTE alias never can: a protected table is always referenced with
    # its bucket, and `_is_cte_reference` never treats a qualified reference as a CTE. What remains
    # is the shadowing that is real -- a bare name resolved against the wrong scope, a quoting
    # mismatch between declaration and reference, a CTE reading its own name without RECURSIVE -- and
    # every one of those is still refused, where it happens, by `_is_cte_reference`.
    cte_names = _cte_names(tree)

    if _reads_system_metadata(tree):
        raise RlsError('RLS: query history and information-schema/system metadata sources are not allowed')
    _check_from_sources(tree)
    _check_functions(tree, cte_names, dialect=dialect)

    applied: list[str] = []
    # The predicate the rewrite actually inserted for each matched key, handed to `_check_output` so
    # the safety net can verify the WHERE it finds is the rule, not merely some WHERE. `'TRUE'` for
    # a key only `cls_rules` governs -- see the docstring above.
    inserted: dict[str, str] = {}
    # The CLS column allowlist actually inserted for each matched key, or `None` when no CLS rule
    # applied to it (meaning "expect a plain SELECT *") -- handed to `_check_output` alongside
    # `inserted` so the safety net can verify the SELECT list too, not only the WHERE.
    inserted_columns: dict[str, tuple[str, ...] | None] = {}
    # (schema, table) of every governed table wrapped WITHOUT a user alias: the wrapper is a derived table
    # aliased with the bare table name, so a column the query qualified with the full name
    # (`"in.c-crm"."invoices"."id"`) must lose that schema qualifier or it no longer resolves.
    unaliased: set[str] = set()
    # Alias a wrapper of an unaliased table takes -> the table it wraps.
    wrapper_alias_owner: dict[str, str] = {}

    def _transform(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node
        if _is_wildcard_table(node, dialect=dialect):
            raise RlsError(f'RLS: wildcard table references are not supported: {node.sql(dialect=dialect)}')
        if not isinstance(node.this, exp.Identifier):
            # A table function has no name to look a rule up by -- `_check_from_sources` already
            # refuses these; this is the same guard on the rewrite path itself.
            raise RlsError(f'RLS: unsupported table reference: {node.sql()}')
        if _is_cte_reference(node, cte_names, dialect=dialect):
            return node  # reference to a CTE in scope here, not a real table
        if not node.db:
            # No schema, so no policy key can be derived -- yet the warehouse may resolve the bare name to a
            # governed table through its current schema. Never pass it through as "ungoverned".
            raise RlsError(f"RLS: table reference must be qualified as <bucket>.<table>: '{node.name}'")
        schema = node.db
        rls_governed = rules.is_governed(table_name=node.name, schema=schema)
        cls_governed = cls_rules is not None and cls_rules.is_governed(table_name=node.name, schema=schema)
        if not rls_governed and not cls_governed:
            return node  # no policy of either kind names this table -- not this rewrite's concern
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
        # `schema` is guaranteed non-empty here: `is_governed()` requires it, and at least one of
        # the two checks above was true. Both lookups below derive the identical key independently
        # (same `_rule_key(schema, node.name, dialect)` either way) -- computed once here rather
        # than trusting two separate returned keys to agree.
        key = _rule_key(schema, node.name, dialect)
        predicate: str | None = None
        columns: tuple[str, ...] | None = None
        if rls_governed:
            _, predicate = rules.predicate_for(table_name=node.name, schema=schema, user=user, groups=groups)
        if cls_governed:
            assert cls_rules is not None  # narrowed by `cls_governed`
            _, columns = cls_rules.columns_for(table_name=node.name, schema=schema, user=user, groups=groups)
        applied.append(key)
        inserted[key] = predicate if predicate is not None else exp.true().sql(dialect=dialect)
        inserted_columns[key] = columns
        # Reuse the original alias identifier as-is (preserving its own quoting) so references to
        # it elsewhere in the query (e.g. an unquoted `o.id` in an ON clause) still resolve. Only
        # fall back to the table's own name/quoting when the table was not aliased at all -- using
        # the table identifier's quoting for an *existing* alias would silently change whether the
        # alias is case-sensitive, breaking those other references.
        alias_node = node.args.get('alias')
        if alias_node is None:
            unaliased.add(_rule_key(node.db, node.name, dialect))
            # Two different governed tables with the same bare name (`in.a.orders`, `in.b.orders`) would
            # both be wrapped as `orders`: duplicate derived-table aliases, and qualifiers that can no
            # longer tell them apart. The same table repeated (self-join, UNION) is fine.
            alias_key = node.name if dialect == 'bigquery' else node.name.lower()
            if wrapper_alias_owner.setdefault(alias_key, key) != key:
                raise RlsError(
                    f'RLS: tables named {node.name!r} in different buckets are both governed; give each an alias'
                )
        alias_identifier = (
            alias_node.this.copy() if alias_node is not None else exp.to_identifier(node.name, quoted=node.this.quoted)
        )
        inner = exp.Table(this=node.this, db=node.args.get('db'), catalog=node.args.get('catalog'))
        predicate_expr: exp.Condition = exp.true()
        if predicate is not None:
            try:
                predicate_expr = sqlglot.parse_one(predicate, dialect=dialect, into=exp.Condition)
            except sqlglot.errors.SqlglotError as e:
                # The caller is told only that the rule could not be applied. Which predicate, and
                # why it failed, is the admin's business and goes to the log -- the message reaches
                # a model.
                LOG.warning(
                    f'RLS: predicate for table {key!r} is not valid SQL for dialect {dialect!r}: {_clean_error(e)}'
                )
                raise RlsError(_RULE_NOT_APPLIED.format(key=key)) from e
        # Quoted for the same reason as in `_compile_primitive`: exact, case-sensitive column names.
        select_columns: list[exp.Expression] = (
            [exp.column(c, quoted=True) for c in columns] if columns is not None else [exp.Star()]
        )
        filtered = exp.select(*select_columns).from_(inner).where(predicate_expr)
        # Returning a new node stops `transform` from descending into it, so the inner table is
        # not wrapped a second time.
        return exp.Subquery(this=filtered, alias=exp.TableAlias(this=alias_identifier))

    rewritten_tree = tree.transform(_transform, copy=True)
    for column in rewritten_tree.find_all(exp.Column):
        qualifier_schema = column.args.get('db')
        if qualifier_schema is not None and _rule_key(qualifier_schema.name, column.table, dialect) in unaliased:
            column.set('db', None)
            column.set('catalog', None)
    rewritten_sql = rewritten_tree.sql(dialect=dialect)
    _check_output(rewritten_sql, dialect=dialect, predicates=inserted, columns=inserted_columns)
    # `dict.fromkeys` deduplicates while preserving first-seen order: a table joined or unioned with
    # itself is disclosed once.
    return RewrittenQuery(sql=rewritten_sql, applied_rules=list(dict.fromkeys(applied)))
