import textwrap
from pathlib import Path

import pytest

from keboola_mcp_server.rls import RewrittenQuery, RlsError, RlsRules, _check_output, rewrite_query

VALID_YAML = textwrap.dedent(
    """
    dialect: snowflake
    tables:
      in.c-crm.invoices:
        petr: "country = 'CZ'"
        Monika: "country = 'DE'"
        admin: "TRUE"
      in.c-crm.orders:
        petr: "country = 'CZ' AND status <> 'draft'"
      in.c-sales.orders:
        petr: "FALSE"
    """
)

BIGQUERY_YAML = VALID_YAML.replace('dialect: snowflake', 'dialect: bigquery')

# The predicates a rewrite of the hand-built SQL in `TestOutputInvariant` would have inserted.
OUTPUT_PREDICATES = {
    'in.c-crm.invoices': "country = 'CZ'",
    'in.c-sales.orders': 'FALSE',
    'in.c-crm.secret': 'TRUE',
}


@pytest.fixture
def rules(tmp_path: Path) -> RlsRules:
    path = tmp_path / 'rls.yaml'
    path.write_text(VALID_YAML)
    return RlsRules.load(str(path))


@pytest.fixture
def bq_rules(tmp_path: Path) -> RlsRules:
    """The same rules pinned to BigQuery -- `rewrite_query` refuses a dialect the rules are not for."""
    path = tmp_path / 'rls-bigquery.yaml'
    path.write_text(BIGQUERY_YAML)
    return RlsRules.load(str(path))


class TestLoad:
    def test_load_normalizes_keys(self, rules: RlsRules) -> None:
        assert rules.tables['in.c-crm.invoices']['monika'] == "country = 'DE'"
        assert rules.tables['in.c-crm.orders']['petr'] == "country = 'CZ' AND status <> 'draft'"

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [('snowflake', 'snowflake'), ('bigquery', 'bigquery'), ('SnowFlake', 'snowflake'), (' bigquery ', 'bigquery')],
    )
    def test_load_reads_dialect(self, tmp_path: Path, value: str, expected: str) -> None:
        path = tmp_path / 'rls.yaml'
        path.write_text(f'dialect: "{value}"\ntables:\n  in.c-crm.invoices:\n    petr: "TRUE"\n')
        assert RlsRules.load(str(path)).dialect == expected

    def test_load_parses_predicates_in_the_pinned_dialect(self, tmp_path: Path) -> None:
        """The load-time check must be real: Snowflake's `data:name` path syntax is not BigQuery SQL,
        so the same file is accepted under one pin and refused under the other."""
        predicate = "payload:country = 'CZ'"
        path = tmp_path / 'rls.yaml'
        path.write_text(f'dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "{predicate}"\n')
        assert RlsRules.load(str(path)).tables['in.c-crm.invoices']['petr'] == predicate

        path.write_text(f'dialect: bigquery\ntables:\n  in.c-crm.invoices:\n    petr: "{predicate}"\n')
        with pytest.raises(RlsError, match='not valid bigquery SQL'):
            RlsRules.load(str(path))

    @pytest.mark.parametrize(
        ('content', 'match'),
        [
            ('', 'empty'),
            ('dialect: snowflake\ntables: {}', 'no tables'),
            ('dialect: snowflake\nfoo: bar', "'tables'"),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices: "not a mapping"', "'in.c-crm.invoices'"),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: 42', "'petr'"),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "country = = 1"', 'petr'),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: ""', 'petr'),
            ('tables: [\n', 'YAML'),
            # The dialect pin is required and must name one of the two supported backends.
            ('tables:\n  in.c-crm.invoices:\n    petr: "TRUE"', "'dialect'"),
            ('dialect: postgres\ntables:\n  in.c-crm.invoices:\n    petr: "TRUE"', "'dialect'"),
            ('dialect: 42\ntables:\n  in.c-crm.invoices:\n    petr: "TRUE"', "'dialect'"),
            ('dialect:\ntables:\n  in.c-crm.invoices:\n    petr: "TRUE"', "'dialect'"),
            # Two keys that normalize to one: the later would silently win, so refuse both.
            (
                'dialect: snowflake\ntables:\n  in.c-crm.Invoices:\n    petr: "TRUE"\n  in.c-crm.invoices:\n    petr: "FALSE"',
                "duplicate table key 'in.c-crm.invoices'",
            ),
            (
                'dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    Petr: "TRUE"\n    petr: "FALSE"',
                "duplicate user key 'petr'",
            ),
            # YAML 1.1 turns bare `yes`/`on`/`no` into booleans, so such a key is not a table name
            # at all -- and would otherwise be stringified into something the admin never wrote.
            ('dialect: snowflake\ntables:\n  yes:\n    petr: "TRUE"', 'invalid table key'),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    on: "TRUE"', 'invalid user key'),
            ('dialect: snowflake\ntables:\n  42:\n    petr: "TRUE"', 'invalid table key'),
            ('dialect: snowflake\ntables:\n  "":\n    petr: "TRUE"', 'invalid table key'),
            ('dialect: snowflake\ntables:\n  "in.c-crm.*":\n    petr: "TRUE"', 'invalid table key'),
            ('dialect: snowflake\ntables:\n  \'in.c-crm."invoices"\':\n    petr: "TRUE"', 'invalid table key'),
            ('dialect: snowflake\ntables:\n  in.c-crm.my table:\n    petr: "TRUE"', 'invalid table key'),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    "":\n      petr: "TRUE"', 'invalid user key'),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    "pe tr": "TRUE"', 'invalid user key'),
            # A predicate must be a boolean condition, not an arbitrary expression that happens to
            # parse: `WHERE 1` or `WHERE f(x)` is not a filter the admin can reason about.
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "1"', 'boolean condition'),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "is_active"', 'boolean condition'),
            ('dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "f(x)"', 'boolean condition'),
            (
                'dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "CASE WHEN a THEN TRUE ELSE FALSE END"',
                'boolean condition',
            ),
            # A predicate that reaches for another table cannot be wrapped -- refuse it at load time
            # rather than at the first query that happens to trip the output invariant.
            (
                'dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "id IN (SELECT id FROM secret)"',
                'must not reference a table or subquery',
            ),
            (
                'dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "EXISTS (SELECT 1 FROM secret)"',
                'must not reference a table or subquery',
            ),
        ],
    )
    def test_load_rejects_invalid_file(self, tmp_path: Path, content: str, match: str) -> None:
        path = tmp_path / 'rls.yaml'
        path.write_text(content)
        with pytest.raises(RlsError, match=match):
            RlsRules.load(str(path))

    @pytest.mark.parametrize(
        'predicate',
        [
            "country = 'CZ'",
            'TRUE',
            'FALSE',
            "country = 'CZ' AND status <> 'draft'",
            "country IN ('PL', 'SK')",
            'NOT deleted',
            "(country = 'CZ')",
            "((country = 'CZ' OR country = 'SK'))",
            'x IS NULL',
            'amount BETWEEN 1 AND 2',
            "name LIKE 'a%'",
        ],
    )
    def test_load_accepts_boolean_predicates(self, tmp_path: Path, predicate: str) -> None:
        path = tmp_path / 'rls.yaml'
        path.write_text(f'dialect: snowflake\ntables:\n  in.c-crm.invoices:\n    petr: "{predicate}"\n')
        assert RlsRules.load(str(path)).tables['in.c-crm.invoices']['petr'] == predicate

    @pytest.mark.parametrize('key', ['invoices', '.invoices', 'invoices.', '.'])
    def test_load_rejects_unqualified_table_keys(self, tmp_path: Path, key: str) -> None:
        """A rule has to say which bucket it protects: a bare `invoices` would silently cover every
        table of that name in every bucket, including ones its author never saw."""
        path = tmp_path / 'rls.yaml'
        path.write_text(f'dialect: snowflake\ntables:\n  "{key}":\n    petr: "TRUE"\n')
        with pytest.raises(RlsError, match='table key|invalid table key'):
            RlsRules.load(str(path))

    def test_load_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(RlsError, match='not found'):
            RlsRules.load(str(tmp_path / 'nope.yaml'))


class TestPredicateFor:
    @pytest.mark.parametrize(
        ('table_name', 'schema', 'user', 'expected'),
        [
            ('invoices', 'in.c-crm', 'petr', ('in.c-crm.invoices', "country = 'CZ'")),
            ('INVOICES', 'IN.C-CRM', 'PETR', ('in.c-crm.invoices', "country = 'CZ'")),
            ('invoices', 'in.c-crm', 'monika', ('in.c-crm.invoices', "country = 'DE'")),
            ('orders', 'in.c-crm', 'petr', ('in.c-crm.orders', "country = 'CZ' AND status <> 'draft'")),
            # The same table name in another bucket is another rule, not the same one.
            ('orders', 'in.c-sales', 'petr', ('in.c-sales.orders', 'FALSE')),
        ],
    )
    def test_lookup(self, rules: RlsRules, table_name, schema, user, expected) -> None:
        assert rules.predicate_for(table_name=table_name, schema=schema, user=user) == expected

    @pytest.mark.parametrize(
        ('table_name', 'schema', 'user', 'match'),
        [
            ('customers', 'in.c-crm', 'petr', "table 'in.c-crm.customers'"),
            ('invoices', 'in.c-crm', 'nobody', "user 'nobody'"),
            ('orders', 'in.c-crm', 'monika', "user 'monika'"),
            # A rule for the table in one bucket says nothing about the table in another.
            ('invoices', 'in.c-sales', 'petr', "table 'in.c-sales.invoices'"),
            # No bucket at all: there is no key to look up, so there is nothing to allow.
            ('invoices', None, 'petr', 'must be qualified'),
            ('invoices', '', 'petr', 'must be qualified'),
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
                'SELECT COUNT(*) FROM "in.c-crm"."invoices"',
                'snowflake',
                "SELECT COUNT(*) FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                'SELECT i.id FROM "in.c-crm"."invoices" i JOIN "in.c-crm"."orders" AS o ON o.id = i.id',
                'snowflake',
                (
                    "SELECT i.id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS i "
                    "JOIN (SELECT * FROM \"in.c-crm\".\"orders\" WHERE country = 'CZ' AND status <> 'draft') AS o "
                    'ON o.id = i.id'
                ),
                ["in.c-crm.invoices: country = 'CZ'", "in.c-crm.orders: country = 'CZ' AND status <> 'draft'"],
            ),
            (
                # The original alias is quoted -- the rewrite must keep it quoted, not borrow the
                # table identifier's own (unquoted) quoting.
                'SELECT "I".id FROM "in.c-crm"."invoices" AS "I"',
                'snowflake',
                'SELECT "I".id FROM (SELECT * FROM "in.c-crm"."invoices" WHERE country = \'CZ\') AS "I"',
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                'WITH x AS (SELECT * FROM "in.c-crm"."invoices") SELECT * FROM x',
                'snowflake',
                "WITH x AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\") SELECT * FROM x",
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                # A CTE may reference an earlier sibling CTE.
                'WITH a AS (SELECT * FROM "in.c-crm"."invoices"), b AS (SELECT * FROM a) SELECT * FROM b',
                'snowflake',
                (
                    "WITH a AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"), "
                    'b AS (SELECT * FROM a) SELECT * FROM b'
                ),
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                # A recursive CTE references itself from inside its own body.
                'WITH RECURSIVE r AS (SELECT id FROM "in.c-crm"."invoices" UNION ALL SELECT id FROM r) SELECT * FROM r',
                'snowflake',
                (
                    "WITH RECURSIVE r AS (SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'UNION ALL SELECT id FROM r) SELECT * FROM r'
                ),
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                # Quoted CTE alias, quoted reference, same case: the same name on both engines.
                'WITH "X" AS (SELECT * FROM "in.c-crm"."invoices") SELECT * FROM "X"',
                'snowflake',
                (
                    'WITH "X" AS (SELECT * FROM (SELECT * FROM "in.c-crm"."invoices" WHERE country = \'CZ\') AS "invoices") '
                    'SELECT * FROM "X"'
                ),
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                # Unquoted identifiers fold case on both Snowflake and BigQuery, so `x` and `X` are
                # the same name and this is an ordinary CTE reference -- it must not be refused.
                'WITH x AS (SELECT * FROM "in.c-crm"."invoices") SELECT * FROM X',
                'snowflake',
                "WITH x AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\") SELECT * FROM X",
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                'WITH `X` AS (SELECT * FROM `in_c_crm`.`invoices`) SELECT * FROM `X`',
                'bigquery',
                (
                    'WITH `X` AS (SELECT * FROM (SELECT * FROM `in_c_crm`.`invoices` '
                    "WHERE country = 'CZ') AS `invoices`) SELECT * FROM `X`"
                ),
                ["in_c_crm.invoices: country = 'CZ'"],
            ),
            (
                # A subquery may declare its own CTE as long as the name shadows nothing outside it.
                'SELECT * FROM "in.c-crm"."invoices" WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)',
                'snowflake',
                (
                    "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)'
                ),
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                'SELECT * FROM (SELECT id FROM "in.c-crm"."invoices") sub',
                'snowflake',
                "SELECT * FROM (SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\") AS sub",
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                'SELECT id FROM "in.c-crm"."invoices" UNION ALL SELECT id FROM "in.c-sales"."orders"',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'UNION ALL SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ["in.c-crm.invoices: country = 'CZ'", 'in.c-sales.orders: FALSE'],
            ),
            (
                'SELECT id FROM "in.c-crm"."invoices" EXCEPT SELECT id FROM "in.c-sales"."orders"',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'EXCEPT SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ["in.c-crm.invoices: country = 'CZ'", 'in.c-sales.orders: FALSE'],
            ),
            (
                'SELECT id FROM "in.c-crm"."invoices" INTERSECT SELECT id FROM "in.c-sales"."orders"',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'INTERSECT SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ["in.c-crm.invoices: country = 'CZ'", 'in.c-sales.orders: FALSE'],
            ),
            (
                # The same table twice: both references are rewritten, but the disclosure lists the
                # rule once (deduplicated, in first-seen order).
                'SELECT id FROM "in.c-crm"."invoices" UNION ALL SELECT id FROM "in.c-crm"."invoices"',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    "UNION ALL SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\""
                ),
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                # No FROM at all: nothing to filter, nothing to disclose -- must still pass.
                'SELECT 1',
                'snowflake',
                'SELECT 1',
                [],
            ),
            (
                # A FROM-less SELECT may still read the clock and do scalar arithmetic: no predicate
                # could shape such a result, but neither can it disclose anything.
                'SELECT CURRENT_DATE',
                'snowflake',
                'SELECT CURRENT_DATE',
                [],
            ),
            (
                'SELECT 1 + 1 AS x',
                'snowflake',
                'SELECT 1 + 1 AS x',
                [],
            ),
            (
                "SELECT CAST('1' AS INT) AS x, COALESCE(NULL, 1) AS y, CONCAT('a', 'b') AS z",
                'snowflake',
                "SELECT CAST('1' AS INT) AS x, COALESCE(NULL, 1) AS y, CONCAT('a', 'b') AS z",
                [],
            ),
            (
                # A project/database name in front of the dataset is ignored: the workspace can
                # only reach its own project, so the bucket and table are what identify the rule.
                'SELECT COUNT(*) FROM `proj.in_c_crm.invoices`',
                'bigquery',
                (
                    'SELECT COUNT(*) FROM (SELECT * FROM `proj`.`in_c_crm`.`invoices` '
                    "WHERE country = 'CZ') AS `invoices`"
                ),
                ["in_c_crm.invoices: country = 'CZ'"],
            ),
            (
                'SELECT COUNT(*) FROM `in_c_crm`.`invoices` LIMIT 10',
                'bigquery',
                (
                    'SELECT COUNT(*) FROM (SELECT * FROM `in_c_crm`.`invoices` '
                    "WHERE country = 'CZ') AS `invoices` LIMIT 10"
                ),
                ["in_c_crm.invoices: country = 'CZ'"],
            ),
            (
                # A whole statement in parentheses means the statement inside them.
                '(SELECT * FROM "in.c-crm"."invoices")',
                'snowflake',
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                '((SELECT * FROM "in.c-crm"."invoices"))',
                'snowflake',
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                '(SELECT id FROM "in.c-crm"."invoices" UNION ALL SELECT id FROM "in.c-sales"."orders")',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'UNION ALL SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ["in.c-crm.invoices: country = 'CZ'", 'in.c-sales.orders: FALSE'],
            ),
            (
                # A trailing semicolon is still exactly one statement.
                'SELECT * FROM "in.c-crm"."invoices";',
                'snowflake',
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                # `UNNET(...) WITH OFFSET` is an allowed FROM source and the correlated `t.items`
                # still resolves because the wrapper keeps the original alias.
                'SELECT * FROM `in_c_crm`.`invoices` AS t, UNNEST(t.items) AS item WITH OFFSET AS off',
                'bigquery',
                (
                    "SELECT * FROM (SELECT * FROM `in_c_crm`.`invoices` WHERE country = 'CZ') AS t "
                    'CROSS JOIN UNNEST(t.items) AS item WITH OFFSET AS off'
                ),
                ["in_c_crm.invoices: country = 'CZ'"],
            ),
            (
                # Window function + QUALIFY over a protected table.
                'SELECT id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY id) rn FROM "in.c-crm"."invoices" QUALIFY rn = 1',
                'snowflake',
                (
                    'SELECT id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY id) AS rn '
                    "FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" QUALIFY rn = 1"
                ),
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
            (
                # A recursive CTE named after nothing protected, joined with a protected table.
                (
                    'WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) '
                    'SELECT * FROM r, "in.c-crm"."invoices"'
                ),
                'snowflake',
                (
                    'WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) '
                    "SELECT * FROM r, (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\""
                ),
                ["in.c-crm.invoices: country = 'CZ'"],
            ),
        ],
    )
    def test_rewrite(self, rules: RlsRules, bq_rules: RlsRules, sql, dialect, expected_sql, expected_rules) -> None:
        # The rules file pins its own dialect, so a BigQuery query needs the BigQuery rules object.
        out = rewrite_query(sql, user='petr', dialect=dialect, rules=bq_rules if dialect == 'bigquery' else rules)
        assert out == RewrittenQuery(sql=expected_sql, applied_rules=expected_rules)

    @pytest.mark.parametrize(
        ('sql', 'user', 'dialect', 'match'),
        [
            ('DELETE FROM invoices', 'petr', 'snowflake', 'SELECT'),
            ('INSERT INTO invoices SELECT * FROM orders', 'petr', 'snowflake', 'SELECT'),
            ('SELECT 1; SELECT 2', 'petr', 'snowflake', 'one statement'),
            ('SELCT nonsense', 'petr', 'snowflake', 'SELECT'),
            ('SELECT * FROM "in.c-crm"."customers"', 'petr', 'snowflake', "table 'in.c-crm.customers'"),
            ('SELECT * FROM "in.c-crm"."invoices"', 'nobody', 'snowflake', "user 'nobody'"),
            # A reference with no bucket cannot be matched against a rule, so it is refused outright.
            ('SELECT * FROM invoices', 'petr', 'snowflake', 'must be qualified'),
            ('SELECT COUNT(*) FROM invoices', 'petr', 'snowflake', 'must be qualified'),
            # One quoted identifier that happens to contain dots is a table name, not a bucket path.
            ('SELECT * FROM "in.c-crm.invoices"', 'petr', 'snowflake', 'must be qualified'),
            # The rule is keyed by bucket: the same table name in another bucket is not covered.
            ('SELECT * FROM "in.c-sales"."invoices"', 'petr', 'snowflake', "table 'in.c-sales.invoices'"),
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
            ('SELECT * FROM "in.c-crm"."invoices"', 'petr', 'not-a-real-dialect', 'dialect'),
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
            # --- FROM sources with no plain identifier ---
            ('SELECT * FROM IDENTIFIER($tbl)', 'petr', 'snowflake', 'unsupported table reference'),
            ("SELECT * FROM IDENTIFIER('invoices')", 'petr', 'snowflake', 'unsupported table reference'),
            ('SELECT $1 FROM @my_stage/invoices.csv', 'petr', 'snowflake', 'unsupported table reference'),
            ('SELECT * FROM DIRECTORY(@stg)', 'petr', 'snowflake', 'unsupported table reference'),
            (
                'SELECT * FROM SEMANTIC_VIEW(sv METRICS invoices.total)',
                'petr',
                'snowflake',
                'unsupported table reference',
            ),
            ('SELECT * FROM a.b.c.d', 'petr', 'snowflake', 'unsupported table reference'),
            ('SELECT * FROM :tbl', 'petr', 'snowflake', 'unsupported table reference'),
            (
                "SELECT * FROM EXTERNAL_QUERY('conn', 'SELECT * FROM invoices')",
                'petr',
                'bigquery',
                'unsupported table reference',
            ),
            ('SELECT * FROM ML.PREDICT(MODEL `m`, TABLE invoices)', 'petr', 'bigquery', 'unsupported table reference'),
            ("EXECUTE IMMEDIATE 'SELECT * FROM invoices'", 'petr', 'bigquery', 'SELECT'),
            (
                'SELECT * FROM "in.c-crm"."invoices", LATERAL FLATTEN(input => invoices.items) f',
                'petr',
                'snowflake',
                'unsupported FROM source',
            ),
            # --- a table without a rule, in every position a subquery can appear ---
            (
                'SELECT (SELECT MAX(x) FROM "in.c-crm"."secret") FROM "in.c-crm"."invoices"',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM "in.c-crm"."invoices" WHERE EXISTS (SELECT 1 FROM "in.c-crm"."secret")',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM "in.c-crm"."invoices", LATERAL (SELECT * FROM "in.c-crm"."secret") s',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM "in.c-crm"."invoices" ORDER BY (SELECT MAX(x) FROM "in.c-crm"."secret")',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM "in.c-crm"."invoices" LIMIT (SELECT COUNT(*) FROM "in.c-crm"."secret")',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM "in.c-crm"."invoices" QUALIFY id IN (SELECT id FROM "in.c-crm"."secret")',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM (VALUES ((SELECT MAX(x) FROM "in.c-crm"."secret"))) v',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM UNNEST((SELECT ARRAY_AGG(x) FROM `in_c_crm`.`secret`))',
                'petr',
                'bigquery',
                "table 'in_c_crm.secret'",
            ),
            ('SELECT * FROM ("in.c-crm"."secret")', 'petr', 'snowflake', "table 'in.c-crm.secret'"),
            # an alias equal to a protected table name must not stand in for a rule
            (
                'SELECT * FROM "in.c-crm"."secret" AS invoices',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            (
                'SELECT * FROM (SELECT * FROM "in.c-crm"."secret") AS invoices',
                'petr',
                'snowflake',
                "table 'in.c-crm.secret'",
            ),
            # --- CTE shadowing, remaining shapes ---
            (
                (
                    'WITH RECURSIVE invoices AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM invoices WHERE n < 3) '
                    'SELECT * FROM invoices'
                ),
                'petr',
                'snowflake',
                'collide',
            ),
            ('WITH "Invoices" AS (SELECT 1) SELECT * FROM "Invoices"', 'petr', 'snowflake', 'collide'),
            ('WITH `Invoices` AS (SELECT 1) SELECT * FROM `Invoices`', 'petr', 'bigquery', 'collide'),
            (
                'SELECT * FROM secret JOIN (WITH secret AS (SELECT 1) SELECT * FROM secret) s ON TRUE',
                'petr',
                'snowflake',
                'another scope',
            ),
            (
                'SELECT * FROM secret WHERE EXISTS (WITH secret AS (SELECT 1) SELECT 1)',
                'petr',
                'snowflake',
                'another scope',
            ),
            ('SELECT (WITH secret AS (SELECT 1) SELECT 1) FROM secret', 'petr', 'snowflake', 'another scope'),
            (
                'SELECT * FROM secret UNION ALL (WITH secret AS (SELECT 1) SELECT * FROM secret)',
                'petr',
                'snowflake',
                'another scope',
            ),
            (
                'WITH a AS (WITH secret AS (SELECT 1) SELECT * FROM secret) SELECT * FROM a, secret',
                'petr',
                'snowflake',
                'another scope',
            ),
            (
                'WITH a AS (SELECT * FROM secret), secret AS (SELECT 1) SELECT * FROM a',
                'petr',
                'snowflake',
                'another scope',
            ),
            ('WITH secret AS (SELECT 1) SELECT * FROM public.secret', 'petr', 'snowflake', "table 'public.secret'"),
            # --- functions in a query with nothing to filter ---
            # `GET_DDL` reads the catalog, so RLS shapes nothing about what it returns.
            ("SELECT GET_DDL('table', 'invoices')", 'petr', 'snowflake', 'without FROM'),
            ('SELECT LAST_QUERY_ID()', 'petr', 'snowflake', 'without FROM'),
            ('SELECT COUNT(*)', 'petr', 'snowflake', 'without FROM'),
            # A dummy CTE gives the query an `exp.Table` node but still no table to filter, so the
            # ban must look through it rather than count the CTE reference as a real source.
            ("WITH t AS (SELECT 1) SELECT GET_DDL('table', 'invoices') FROM t", 'petr', 'snowflake', 'without FROM'),
            # --- functions banned everywhere, FROM or no FROM ---
            ('SELECT SYSTEM$CANCEL_ALL_QUERIES()', 'petr', 'snowflake', 'not allowed: SYSTEM'),
            (
                'SELECT * FROM "in.c-crm"."invoices" WHERE SYSTEM$TYPEOF(x) = \'a\'',
                'petr',
                'snowflake',
                'not allowed: SYSTEM',
            ),
            ("SELECT SNOWFLAKE.CORTEX.COMPLETE('m', 'p')", 'petr', 'snowflake', 'not allowed: SNOWFLAKE.CORTEX'),
            (
                'SELECT SNOWFLAKE.CORTEX.SENTIMENT(c) FROM "in.c-crm"."invoices"',
                'petr',
                'snowflake',
                'not allowed: SNOWFLAKE.CORTEX',
            ),
        ],
    )
    def test_rewrite_fails_closed(self, rules: RlsRules, bq_rules: RlsRules, sql, user, dialect, match) -> None:
        with pytest.raises(RlsError, match=match):
            rewrite_query(sql, user=user, dialect=dialect, rules=bq_rules if dialect == 'bigquery' else rules)

    @pytest.mark.parametrize('dialect', ['snowflake', 'bigquery'])
    def test_rewrite_refuses_a_dialect_the_rules_are_not_for(
        self, rules: RlsRules, bq_rules: RlsRules, dialect
    ) -> None:
        """A predicate is never transpiled, so running Snowflake rules against a BigQuery workspace
        (or the other way round) would silently change what the filter means. Refuse instead."""
        wrong = bq_rules if dialect == 'snowflake' else rules
        with pytest.raises(RlsError, match=f'rules are for dialect .* but the workspace is {dialect}'):
            rewrite_query('SELECT * FROM "in.c-crm"."invoices"', user='petr', dialect=dialect, rules=wrong)

    @pytest.mark.parametrize(
        'sql',
        [
            'SELECT * FROM in.c-crm.orders',
            'SELECT * FROM out.c-main.tbl',
            'SELECT a, FROM in.c-crm.orders WHERE',
        ],
    )
    def test_parse_errors_are_clean_text_with_a_bucket_hint(self, rules: RlsRules, sql: str) -> None:
        """A parse error is shown to a user and a model, so it must not carry sqlglot's ANSI
        underline escapes -- and when the cause is an unquoted bucket path, it should say so."""
        with pytest.raises(RlsError) as excinfo:
            rewrite_query(sql, user='petr', dialect='snowflake', rules=rules)

        message = str(excinfo.value)
        assert '\x1b' not in message
        assert 'quote the bucket, e.g. "in.c-crm"."orders"' in message

    def test_parse_error_without_a_bucket_path_gets_no_hint(self, rules: RlsRules) -> None:
        with pytest.raises(RlsError) as excinfo:
            rewrite_query('SELECT * FROM t WHERE', user='petr', dialect='snowflake', rules=rules)

        assert 'quote the bucket' not in str(excinfo.value)

    def test_cte_colliding_with_qualified_rule_key_fails_closed(self) -> None:
        """A CTE alias is always bare, so it can never equal a `<schema>.<table>` rule key
        literally -- it is the key's bare table name it shadows. The collision guard must compare
        against that, otherwise `WITH invoices AS (...)` slips past rules keyed
        `in.c-crm.invoices`.
        """
        scoped_rules = RlsRules(tables={'in.c-crm.invoices': {'petr': "country = 'CZ'"}}, dialect='snowflake')
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
        bad_rules = RlsRules(tables={'in.c-crm.invoices': {'petr': 'country = = 1'}}, dialect='snowflake')
        with pytest.raises(RlsError):
            rewrite_query('SELECT * FROM "in.c-crm"."invoices"', user='petr', dialect='snowflake', rules=bad_rules)

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
        bad_rules = RlsRules(tables={'in.c-crm.invoices': {'petr': predicate}}, dialect='snowflake')
        with pytest.raises(RlsError, match=match):
            rewrite_query('SELECT * FROM "in.c-crm"."invoices"', user='petr', dialect='snowflake', rules=bad_rules)


class TestOutputInvariant:
    """`_check_output` is the last-resort safety net: whatever the rewrite produced, it must be one
    SELECT (or set operation) in which every real table sits inside a `(SELECT * FROM t WHERE ...)`
    wrapper we generated, carrying exactly the predicate the rewrite inserted. It is checked on the
    re-parsed output, so it does not trust the rewrite.
    """

    @pytest.mark.parametrize(
        ('sql', 'match'),
        [
            ('CREATE TABLE t AS SELECT 1', 'non-SELECT statement'),
            ('DROP TABLE invoices', 'non-SELECT statement'),
            ('SELECT 1; SELECT 2', 'non-SELECT statement'),
            ('SELCT nonsense', 'non-SELECT statement'),
            ('SELECT * FROM invoices', 'unwrapped table reference'),
            (
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS i JOIN orders o ON TRUE",
                'unwrapped',
            ),
            # A wrapper that is not `SELECT *` would silently drop the RLS predicate's columns.
            ('SELECT * FROM (SELECT id FROM "in.c-crm"."invoices") AS "invoices"', 'unwrapped table reference'),
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
            # A wrapper with no WHERE at all is the whole table: the shape looks right and the data
            # is unfiltered, which is exactly what this net exists to catch.
            ('SELECT * FROM (SELECT * FROM "in.c-crm"."invoices") AS "invoices"', 'wrapper without a WHERE clause'),
            # A WHERE that is not the rule -- weakened, negated or simply a different condition.
            (
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'DE') AS \"invoices\"",
                'not the rule for table',
            ),
            ('SELECT * FROM (SELECT * FROM "in.c-crm"."invoices" WHERE TRUE) AS "invoices"', 'not the rule for table'),
            (
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ' OR TRUE) AS \"invoices\"",
                'not the rule for table',
            ),
            # A table that was wrapped although no rule was ever looked up for it.
            ('SELECT * FROM (SELECT * FROM customers WHERE TRUE) AS customers', 'no rule was looked up for'),
        ],
    )
    def test_rejects(self, sql: str, match: str) -> None:
        with pytest.raises(RlsError, match=match):
            _check_output(sql, dialect='snowflake', predicates=OUTPUT_PREDICATES)

    @pytest.mark.parametrize(
        'sql',
        [
            'SELECT 1',
            "SELECT COUNT(*) FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
            "WITH x AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\") SELECT * FROM x",
            (
                "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                'UNION ALL SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
            ),
            # A CTE reference from a scope that really does declare it, at every nesting shape.
            (
                "WITH a AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"), "
                'b AS (SELECT * FROM a) SELECT * FROM b'
            ),
            (
                "WITH RECURSIVE r AS (SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                'UNION ALL SELECT id FROM r) SELECT * FROM r'
            ),
            (
                "SELECT 1 FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                'WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)'
            ),
            # Quoted CTE alias and quoted reference agreeing in case: an ordinary CTE reference.
            (
                'WITH "X" AS (SELECT * FROM (SELECT * FROM "in.c-crm"."invoices" WHERE country = \'CZ\') AS "invoices") '
                'SELECT * FROM "X"'
            ),
        ],
    )
    def test_accepts(self, sql: str) -> None:
        _check_output(sql, dialect='snowflake', predicates=OUTPUT_PREDICATES)
