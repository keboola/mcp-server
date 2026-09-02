import textwrap
from pathlib import Path

import pytest

from keboola_mcp_server.rls import RewrittenQuery, RlsError, RlsRules, _check_output, rewrite_query

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
                (
                    "SELECT i.id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS i "
                    "JOIN (SELECT * FROM \"in.c-crm\".\"orders\" WHERE country = 'CZ' AND status <> 'draft') AS o "
                    'ON o.id = i.id'
                ),
                ["invoices: country = 'CZ'", "in.c-crm.orders: country = 'CZ' AND status <> 'draft'"],
            ),
            (
                # The original alias is quoted -- the rewrite must keep it quoted, not borrow the
                # table identifier's own (unquoted) quoting.
                'SELECT "I".id FROM invoices AS "I"',
                'snowflake',
                'SELECT "I".id FROM (SELECT * FROM invoices WHERE country = \'CZ\') AS "I"',
                ["invoices: country = 'CZ'"],
            ),
            (
                'WITH x AS (SELECT * FROM invoices) SELECT * FROM x',
                'snowflake',
                "WITH x AS (SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices) SELECT * FROM x",
                ["invoices: country = 'CZ'"],
            ),
            (
                # A CTE may reference an earlier sibling CTE.
                'WITH a AS (SELECT * FROM invoices), b AS (SELECT * FROM a) SELECT * FROM b',
                'snowflake',
                (
                    "WITH a AS (SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices), "
                    'b AS (SELECT * FROM a) SELECT * FROM b'
                ),
                ["invoices: country = 'CZ'"],
            ),
            (
                # A recursive CTE references itself from inside its own body.
                'WITH RECURSIVE r AS (SELECT id FROM invoices UNION ALL SELECT id FROM r) SELECT * FROM r',
                'snowflake',
                (
                    "WITH RECURSIVE r AS (SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                    'UNION ALL SELECT id FROM r) SELECT * FROM r'
                ),
                ["invoices: country = 'CZ'"],
            ),
            (
                # Quoted CTE alias, quoted reference, same case: the same name on both engines.
                'WITH "X" AS (SELECT * FROM invoices) SELECT * FROM "X"',
                'snowflake',
                (
                    'WITH "X" AS (SELECT * FROM (SELECT * FROM invoices WHERE country = \'CZ\') AS invoices) '
                    'SELECT * FROM "X"'
                ),
                ["invoices: country = 'CZ'"],
            ),
            (
                # Unquoted identifiers fold case on both Snowflake and BigQuery, so `x` and `X` are
                # the same name and this is an ordinary CTE reference -- it must not be refused.
                'WITH x AS (SELECT * FROM invoices) SELECT * FROM X',
                'snowflake',
                "WITH x AS (SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices) SELECT * FROM X",
                ["invoices: country = 'CZ'"],
            ),
            (
                'WITH `X` AS (SELECT * FROM invoices) SELECT * FROM `X`',
                'bigquery',
                (
                    "WITH `X` AS (SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices) "
                    'SELECT * FROM `X`'
                ),
                ["invoices: country = 'CZ'"],
            ),
            (
                # A subquery may declare its own CTE as long as the name shadows nothing outside it.
                'SELECT * FROM invoices WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)',
                'snowflake',
                (
                    "SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                    'WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)'
                ),
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
                (
                    "SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                    'UNION ALL SELECT id FROM (SELECT * FROM orders WHERE FALSE) AS orders'
                ),
                ["invoices: country = 'CZ'", 'orders: FALSE'],
            ),
            (
                'SELECT id FROM invoices EXCEPT SELECT id FROM orders',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                    'EXCEPT SELECT id FROM (SELECT * FROM orders WHERE FALSE) AS orders'
                ),
                ["invoices: country = 'CZ'", 'orders: FALSE'],
            ),
            (
                'SELECT id FROM invoices INTERSECT SELECT id FROM orders',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                    'INTERSECT SELECT id FROM (SELECT * FROM orders WHERE FALSE) AS orders'
                ),
                ["invoices: country = 'CZ'", 'orders: FALSE'],
            ),
            (
                # The same table twice: both references are rewritten, but the disclosure lists the
                # rule once (deduplicated, in first-seen order).
                'SELECT id FROM invoices UNION ALL SELECT id FROM invoices',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                    "UNION ALL SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices"
                ),
                ["invoices: country = 'CZ'"],
            ),
            (
                # No FROM at all: nothing to filter, nothing to disclose -- must still pass.
                'SELECT 1',
                'snowflake',
                'SELECT 1',
                [],
            ),
            (
                # Metadata functions are out of scope for RLS: they take no table source, so there is
                # nothing to rewrite. They are allowed through unchanged, like any other FROM-less SELECT.
                "SELECT GET_DDL('table', 'invoices')",
                'snowflake',
                "SELECT GET_DDL('table', 'invoices')",
                [],
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
        ('sql', 'user', 'dialect', 'match'),
        [
            ('DELETE FROM invoices', 'petr', 'snowflake', 'SELECT'),
            ('INSERT INTO invoices SELECT * FROM orders', 'petr', 'snowflake', 'SELECT'),
            ('SELECT 1; SELECT 2', 'petr', 'snowflake', 'one statement'),
            ('SELCT nonsense', 'petr', 'snowflake', 'SELECT'),
            ('SELECT * FROM customers', 'petr', 'snowflake', "table 'customers'"),
            ('SELECT * FROM invoices', 'nobody', 'snowflake', "user 'nobody'"),
            # A CTE named like a protected table would shadow it inside its own body -- refuse.
            ('WITH invoices AS (SELECT * FROM invoices) SELECT * FROM invoices', 'petr', 'snowflake', 'collide'),
            # ... whatever the quoting of either side: a quoted `"orders"` CTE and the rule key
            # `orders` are the same name.
            ('WITH "orders" AS (SELECT 1) SELECT * FROM ORDERS', 'petr', 'snowflake', 'collide'),
            # A CTE declared in a nested scope must not make a top-level real table look like a CTE
            # reference. Each of these used to pass through verbatim, unfiltered.
            (
                'SELECT * FROM secret WHERE 1 IN (WITH secret AS (SELECT 1) SELECT 1)',
                'petr',
                'snowflake',
                'another scope',
            ),
            (
                'SELECT * FROM secret WHERE 1 IN (WITH secret AS (SELECT 1) SELECT 1)',
                'petr',
                'bigquery',
                'another scope',
            ),
            ('SELECT * FROM secret, (WITH secret AS (SELECT 1) SELECT 1) q', 'petr', 'snowflake', 'another scope'),
            (
                'SELECT id FROM secret UNION ALL SELECT 1 FROM (WITH secret AS (SELECT 1) SELECT 1) q',
                'petr',
                'snowflake',
                'another scope',
            ),
            # Same trick aimed at a table that does have a rule: caught by the collision guard.
            (
                'SELECT * FROM invoices WHERE 1 IN (WITH invoices AS (SELECT 1) SELECT 1)',
                'petr',
                'snowflake',
                'collide',
            ),
            # A CTE reference whose quoting differs from the declaration is ambiguous -- the engine
            # and the rewriter can disagree about whether it resolves to the CTE or a real table.
            ('WITH "secret" AS (SELECT 1) SELECT * FROM SECRET', 'petr', 'snowflake', 'ambiguous'),
            # Quoted identifiers are case-sensitive on Snowflake and BigQuery, so `"SECRET"` binds to
            # the base table, not to the `"secret"` CTE. Treating them as one name would let the real
            # table through unfiltered.
            ('WITH "secret" AS (SELECT 1) SELECT * FROM "SECRET"', 'petr', 'snowflake', 'another scope'),
            ('WITH "SECRET" AS (SELECT 1) SELECT * FROM "secret"', 'petr', 'snowflake', 'another scope'),
            ('WITH `secret` AS (SELECT 1) SELECT * FROM `SECRET`', 'petr', 'bigquery', 'another scope'),
            # Without RECURSIVE a CTE is not visible inside its own body: BigQuery resolves the inner
            # `secret` to the base table. Refuse rather than pass the query through verbatim.
            (
                'WITH secret AS (SELECT * FROM secret) SELECT * FROM secret',
                'petr',
                'snowflake',
                'without RECURSIVE',
            ),
            (
                'WITH secret AS (SELECT * FROM secret) SELECT * FROM secret',
                'petr',
                'bigquery',
                'without RECURSIVE',
            ),
            # Table functions parse as an `exp.Table` whose `this` is not an identifier: there is no
            # table name to look a rule up by, so they must be refused, not silently passed through.
            ('SELECT * FROM my_udtf(1)', 'petr', 'snowflake', 'unsupported table reference'),
            ('SELECT * FROM invoices(1)', 'petr', 'snowflake', 'unsupported table reference'),
            ('', 'petr', 'snowflake', 'one statement'),
            # An unknown/unsupported dialect must not let a raw sqlglot exception escape.
            ('SELECT * FROM invoices', 'petr', 'not-a-real-dialect', 'dialect'),
            # FROM sources that are not tables/subqueries: sqlglot parses `TABLE(...)` as a
            # TableFromRows wrapping a function call, so there is no exp.Table to rewrite and the
            # query would otherwise reach the workspace unfiltered.
            ('SELECT * FROM TABLE(invoices)', 'petr', 'snowflake', 'unsupported FROM source'),
            ('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))', 'petr', 'snowflake', 'unsupported FROM source'),
            ('SELECT * FROM invoices JOIN TABLE(orders) ON TRUE', 'petr', 'snowflake', 'unsupported FROM source'),
            # SELECT ... INTO generates CREATE TABLE DDL out of a read-only tool.
            ('SELECT * INTO t FROM invoices', 'petr', 'snowflake', 'SELECT INTO'),
            ('WITH t AS (SELECT 1) SELECT * INTO t FROM invoices', 'petr', 'snowflake', 'SELECT INTO'),
            # Table modifiers cannot survive the rewrite, so they are refused rather than dropped.
            ('SELECT * FROM invoices SAMPLE (10)', 'petr', 'snowflake', 'table modifiers'),
            ('SELECT * FROM invoices AT(OFFSET => -60)', 'petr', 'snowflake', 'table modifiers'),
            (
                "SELECT * FROM invoices PIVOT(SUM(amount) FOR country IN ('CZ', 'DE'))",
                'petr',
                'snowflake',
                'table modifiers',
            ),
            ('SELECT * FROM invoices AS x(a, b)', 'petr', 'snowflake', 'table modifiers'),
            (
                "SELECT * FROM invoices FOR SYSTEM_TIME AS OF TIMESTAMP('2024-01-01')",
                'petr',
                'bigquery',
                'table modifiers',
            ),
        ],
    )
    def test_rewrite_fails_closed(self, rules: RlsRules, sql, user, dialect, match) -> None:
        with pytest.raises(RlsError, match=match):
            rewrite_query(sql, user=user, dialect=dialect, rules=rules)

    def test_cte_colliding_with_qualified_rule_key_fails_closed(self) -> None:
        """A CTE alias is always bare, so it can never equal a `<schema>.<table>` rule key
        literally -- it is the key's bare table name it shadows. The collision guard must compare
        against that, otherwise `WITH invoices AS (...)` slips past rules keyed
        `in.c-crm.invoices`.
        """
        scoped_rules = RlsRules(tables={'in.c-crm.invoices': {'petr': "country = 'CZ'"}})
        with pytest.raises(RlsError, match='collide'):
            rewrite_query(
                'SELECT * FROM invoices WHERE 1 IN (WITH invoices AS (SELECT 1) SELECT 1)',
                user='petr',
                dialect='snowflake',
                rules=scoped_rules,
            )

    def test_rewrite_predicate_invalid_for_dialect_fails_closed(self) -> None:
        """A predicate can pass `RlsRules.load()`'s dialect-agnostic check yet still fail to parse
        under the workspace dialect used at rewrite time (see the module docstring). Build the
        rules object directly, bypassing `load()`, so the rewrite-time guard is exercised on its
        own; the underlying `sqlglot.parse_one(..., dialect=dialect)` call must not raise a raw
        `sqlglot` exception.
        """
        bad_rules = RlsRules(tables={'invoices': {'petr': 'country = = 1'}})
        with pytest.raises(RlsError):
            rewrite_query('SELECT * FROM invoices', user='petr', dialect='snowflake', rules=bad_rules)

    @pytest.mark.parametrize(
        ('predicate', 'match'),
        [
            # Injection attempt: the predicate must parse as a bare condition, nothing more.
            ('TRUE) AS x, (SELECT * FROM secret WHERE (TRUE', 'not valid SQL'),
            # A predicate referencing another table leaves that table outside a wrapper -- the
            # output invariant refuses it rather than letting an unfiltered reference through.
            ('id IN (SELECT id FROM secret)', 'unwrapped table reference'),
        ],
    )
    def test_rewrite_rejects_predicates_that_are_not_plain_conditions(self, predicate: str, match: str) -> None:
        bad_rules = RlsRules(tables={'invoices': {'petr': predicate}})
        with pytest.raises(RlsError, match=match):
            rewrite_query('SELECT * FROM invoices', user='petr', dialect='snowflake', rules=bad_rules)


class TestOutputInvariant:
    """`_check_output` is the last-resort safety net: whatever the rewrite produced, it must be one
    SELECT (or set operation) in which every real table sits inside a `(SELECT * FROM t WHERE ...)`
    wrapper we generated. It is checked on the re-parsed output, so it does not trust the rewrite.
    """

    @pytest.mark.parametrize(
        ('sql', 'match'),
        [
            ('CREATE TABLE t AS SELECT 1', 'non-SELECT statement'),
            ('DROP TABLE invoices', 'non-SELECT statement'),
            ('SELECT 1; SELECT 2', 'non-SELECT statement'),
            ('SELCT nonsense', 'non-SELECT statement'),
            ('SELECT * FROM invoices', 'unwrapped table reference'),
            ("SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS i JOIN orders o ON TRUE", 'unwrapped'),
            # A wrapper that is not `SELECT *` would silently drop the RLS predicate's columns.
            ('SELECT * FROM (SELECT id FROM invoices) AS invoices', 'unwrapped table reference'),
            # The CTE that would excuse `secret` is declared in a nested scope, so it excuses
            # nothing: `secret` is a real, unwrapped table here.
            ('SELECT * FROM secret WHERE 1 IN (WITH secret AS (SELECT 1) SELECT 1)', 'another scope'),
            ('WITH "secret" AS (SELECT 1) SELECT * FROM SECRET', 'ambiguous'),
            # Quoted identifiers are case-sensitive: `"SECRET"` is the base table, not the CTE.
            ('WITH "secret" AS (SELECT 1) SELECT * FROM "SECRET"', 'another scope'),
            # A non-recursive CTE does not cover a reference to its own name inside its body.
            ('WITH secret AS (SELECT * FROM secret) SELECT * FROM secret', 'without RECURSIVE'),
            # A table function has no name to check a wrapper against; the output check must refuse
            # it on its own, exactly as `_check_from_sources` and `_transform` do on the way in.
            ('SELECT * FROM my_udtf(1)', 'rewrite left an unsupported table reference'),
        ],
    )
    def test_rejects(self, sql: str, match: str) -> None:
        with pytest.raises(RlsError, match=match):
            _check_output(sql, dialect='snowflake')

    @pytest.mark.parametrize(
        'sql',
        [
            'SELECT 1',
            "SELECT COUNT(*) FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices",
            "WITH x AS (SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices) SELECT * FROM x",
            (
                "SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                'UNION ALL SELECT id FROM (SELECT * FROM orders WHERE FALSE) AS orders'
            ),
            # A CTE reference from a scope that really does declare it, at every nesting shape.
            (
                "WITH a AS (SELECT * FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices), "
                'b AS (SELECT * FROM a) SELECT * FROM b'
            ),
            (
                "WITH RECURSIVE r AS (SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                'UNION ALL SELECT id FROM r) SELECT * FROM r'
            ),
            (
                "SELECT 1 FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                'WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)'
            ),
            # Quoted CTE alias and quoted reference agreeing in case: an ordinary CTE reference.
            (
                'WITH "X" AS (SELECT * FROM (SELECT * FROM invoices WHERE country = \'CZ\') AS invoices) '
                'SELECT * FROM "X"'
            ),
        ],
    )
    def test_accepts(self, sql: str) -> None:
        _check_output(sql, dialect='snowflake')
