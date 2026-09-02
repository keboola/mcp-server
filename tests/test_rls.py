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
                (
                    "SELECT i.id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS i "
                    "JOIN (SELECT * FROM \"in.c-crm\".\"orders\" WHERE country = 'CZ' AND status <> 'draft') AS \"o\" "
                    'ON o.id = i.id'
                ),
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
                (
                    "SELECT id FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices "
                    'UNION ALL SELECT id FROM (SELECT * FROM orders WHERE FALSE) AS orders'
                ),
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
