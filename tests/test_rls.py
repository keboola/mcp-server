import pytest

from keboola_mcp_server.clients.metastore import MetaObjectMeta, MetastoreObject
from keboola_mcp_server.rls import (
    RewrittenQuery,
    RlsError,
    RlsRules,
    _check_output,
    _compile_primitive,
    _normalize_schema,
    _rule_key,
    references_governed_table,
    rewrite_query,
)

# The predicates a rewrite of the hand-built SQL in `TestOutputInvariant` would have inserted.
OUTPUT_PREDICATES = {
    'in.c-crm.invoices': "country = 'CZ'",
    'in.c-sales.orders': 'FALSE',
    'in.c-crm.secret': 'TRUE',
}


# (bucket, table) -> {principal: predicate}, dialect-agnostic; `_rules_for` derives each dialect's
# actual keys the same way `RlsRules.from_metastore` would (bucket normalisation, key casing).
_RAW_TABLES = {
    ('in.c-crm', 'invoices'): {'petr': "country = 'CZ'", 'monika': "country = 'DE'", 'admin': 'TRUE'},
    ('in.c-crm', 'orders'): {'petr': "country = 'CZ' AND status <> 'draft'"},
    ('in.c-sales', 'orders'): {'petr': 'FALSE'},
}


def _rules_for(dialect: str) -> RlsRules:
    return RlsRules(
        tables={
            _rule_key(_normalize_schema(bucket, dialect), table, dialect): users
            for (bucket, table), users in _RAW_TABLES.items()
        },
        dialect=dialect,
    )


@pytest.fixture
def rules() -> RlsRules:
    return _rules_for('snowflake')


@pytest.fixture
def bq_rules() -> RlsRules:
    """The same rules pinned to BigQuery -- `rewrite_query` refuses a dialect the rules are not for."""
    return _rules_for('bigquery')


def _policy_object(
    *,
    obj_id: str = 'obj-1',
    table: str,
    dialect: str = 'snowflake',
    rules_list: list,
    source_project_id: int | None = 1,
    target_project_ids: tuple | None = None,
) -> MetastoreObject:
    return MetastoreObject(
        type='rls-policy',
        id=obj_id,
        attributes={'table': table, 'dialect': dialect, 'rules': rules_list},
        meta=MetaObjectMeta(source_project_id=source_project_id, target_project_ids=target_project_ids),
    )


class TestCompilePrimitive:
    @pytest.mark.parametrize(
        ('condition', 'expected'),
        [
            ({'column': 'country', 'op': 'eq', 'value': 'CZ'}, "country = 'CZ'"),
            ({'column': 'country', 'op': 'ne', 'value': 'CZ'}, "country <> 'CZ'"),
            ({'column': 'amount', 'op': 'gt', 'value': 5}, 'amount > 5'),
            ({'column': 'amount', 'op': 'gte', 'value': 5}, 'amount >= 5'),
            ({'column': 'amount', 'op': 'lt', 'value': 5}, 'amount < 5'),
            ({'column': 'amount', 'op': 'lte', 'value': 5}, 'amount <= 5'),
            ({'column': 'region', 'op': 'in', 'values': ['CZ', 'SK']}, "region IN ('CZ', 'SK')"),
            ({'column': 'region', 'op': 'not_in', 'values': ['CZ']}, "NOT region IN ('CZ')"),
            ({'column': 'deleted_at', 'op': 'is_null'}, 'deleted_at IS NULL'),
            ({'column': 'deleted_at', 'op': 'is_not_null'}, 'NOT deleted_at IS NULL'),
            ({'true': True}, 'TRUE'),
            (
                {'and': [{'column': 'a', 'op': 'eq', 'value': 1}, {'column': 'b', 'op': 'eq', 'value': 2}]},
                'a = 1 AND b = 2',
            ),
            (
                {'or': [{'column': 'a', 'op': 'eq', 'value': 1}, {'column': 'b', 'op': 'eq', 'value': 2}]},
                'a = 1 OR b = 2',
            ),
            (
                {
                    'and': [
                        {'column': 'a', 'op': 'eq', 'value': 1},
                        {'column': 'b', 'op': 'eq', 'value': 2},
                        {'column': 'c', 'op': 'eq', 'value': 3},
                    ]
                },
                '(a = 1 AND b = 2) AND c = 3',
            ),
        ],
    )
    def test_compile_primitive(self, condition, expected) -> None:
        assert _compile_primitive(condition, dialect='snowflake').sql(dialect='snowflake') == expected

    @pytest.mark.parametrize(
        ('condition', 'match'),
        [
            ('not a mapping', 'must be an object'),
            ({'true': False}, 'must be exactly'),
            ({'true': True, 'extra': 1}, 'must be exactly'),
            ({'and': [{'column': 'a', 'op': 'eq', 'value': 1}]}, 'at least 2'),
            ({'and': []}, 'at least 2'),
            ({'column': 'a'}, "missing a valid 'op'"),
            ({'op': 'eq', 'value': 1}, "missing a valid 'column'"),
            ({'column': '', 'op': 'eq', 'value': 1}, "missing a valid 'column'"),
            ({'column': 'a', 'op': 'eq'}, "requires a 'value'"),
            ({'column': 'a', 'op': 'in', 'values': []}, "non-empty 'values'"),
            ({'column': 'a', 'op': 'in', 'values': 'not-a-list'}, "non-empty 'values'"),
            ({'column': 'a', 'op': 'bogus'}, 'unknown condition op'),
        ],
    )
    def test_compile_primitive_rejects_invalid_shapes(self, condition, match) -> None:
        with pytest.raises(RlsError, match=match):
            _compile_primitive(condition, dialect='snowflake')


class TestFromMetastore:
    def test_builds_tables_from_rules(self) -> None:
        obj = _policy_object(
            table='in.c-crm.invoices',
            rules_list=[
                {'principal': 'petr', 'condition': {'column': 'country', 'op': 'eq', 'value': 'CZ'}},
                {'principal': 'Monika', 'condition': {'column': 'country', 'op': 'eq', 'value': 'DE'}},
                {'principal': 'admin', 'condition': {'true': True}},
            ],
        )
        rules = RlsRules.from_metastore([obj], dialect='snowflake', project_id=1)
        assert rules.tables['in.c-crm.invoices']['petr'] == "country = 'CZ'"
        assert rules.tables['in.c-crm.invoices']['monika'] == "country = 'DE'"
        assert rules.tables['in.c-crm.invoices']['admin'] == 'TRUE'

    def test_principals_list_expands_to_individual_entries(self) -> None:
        obj = _policy_object(
            table='in.c-crm.invoices',
            rules_list=[{'principals': ['petr', 'Monika'], 'condition': {'true': True}}],
        )
        rules = RlsRules.from_metastore([obj], dialect='snowflake', project_id=1)
        assert rules.tables['in.c-crm.invoices']['petr'] == 'TRUE'
        assert rules.tables['in.c-crm.invoices']['monika'] == 'TRUE'

    def test_skips_objects_authored_for_a_different_project(self) -> None:
        obj = _policy_object(
            table='in.c-crm.invoices',
            rules_list=[{'principal': 'petr', 'condition': {'true': True}}],
            source_project_id=1,
        )
        rules = RlsRules.from_metastore([obj], dialect='snowflake', project_id=2)
        assert rules.tables == {}

    def test_targeted_scope_applies_only_to_listed_projects(self) -> None:
        obj = _policy_object(
            table='in.c-crm.invoices',
            rules_list=[{'principal': 'petr', 'condition': {'true': True}}],
            source_project_id=1,
            target_project_ids=(2, 3),
        )
        assert 'in.c-crm.invoices' in RlsRules.from_metastore([obj], dialect='snowflake', project_id=2).tables
        assert RlsRules.from_metastore([obj], dialect='snowflake', project_id=99).tables == {}

    def test_normalizes_bigquery_schema(self) -> None:
        obj = _policy_object(
            table='in.c-crm.invoices', dialect='bigquery', rules_list=[{'principal': 'petr', 'condition': {'true': True}}]
        )
        rules = RlsRules.from_metastore([obj], dialect='bigquery', project_id=1)
        assert 'in_c_crm.invoices' in rules.tables

    @pytest.mark.parametrize(
        ('rules_list', 'match'),
        [
            ([{'condition': {'true': True}}], 'no principal'),
            ([{'principal': '', 'condition': {'true': True}}], 'no principal'),
            ([{'principal': 'petr', 'condition': {'column': 'x', 'op': 'bogus'}}], 'unknown condition op'),
            ([{'principal': 'petr'}], 'condition must be an object'),
            ([{'principal': 'pe tr', 'condition': {'true': True}}], 'invalid principal'),
        ],
    )
    def test_rejects_invalid_rule(self, rules_list, match) -> None:
        obj = _policy_object(table='in.c-crm.invoices', rules_list=rules_list)
        with pytest.raises(RlsError, match=match):
            RlsRules.from_metastore([obj], dialect='snowflake', project_id=1)

    def test_rejects_unqualified_table(self) -> None:
        obj = _policy_object(table='invoices', rules_list=[{'principal': 'petr', 'condition': {'true': True}}])
        with pytest.raises(RlsError, match='unqualified table'):
            RlsRules.from_metastore([obj], dialect='snowflake', project_id=1)

    def test_rejects_dialect_mismatch_for_an_applicable_object(self) -> None:
        """Unlike a project-id mismatch (silently skipped, not this project's business), a dialect
        mismatch on an object that DOES apply to this project is an authoring inconsistency: it
        must fail closed, not silently leave the table unfiltered."""
        obj = _policy_object(
            table='in.c-crm.invoices', dialect='bigquery', rules_list=[{'principal': 'petr', 'condition': {'true': True}}]
        )
        with pytest.raises(RlsError, match='dialect'):
            RlsRules.from_metastore([obj], dialect='snowflake', project_id=1)

    def test_rejects_duplicate_principal_across_objects(self) -> None:
        obj_a = _policy_object(
            obj_id='a', table='in.c-crm.invoices', rules_list=[{'principal': 'petr', 'condition': {'true': True}}]
        )
        obj_b = _policy_object(
            obj_id='b', table='in.c-crm.invoices', rules_list=[{'principal': 'petr', 'condition': {'true': True}}]
        )
        with pytest.raises(RlsError, match='multiple applicable policies'):
            RlsRules.from_metastore([obj_a, obj_b], dialect='snowflake', project_id=1)

    def test_rejects_unsupported_dialect(self) -> None:
        with pytest.raises(RlsError, match='unsupported workspace dialect'):
            RlsRules.from_metastore([], dialect='postgres', project_id=1)


class TestIsGovernedAndReferencesGovernedTable:
    def test_is_governed(self, rules: RlsRules) -> None:
        assert rules.is_governed(table_name='invoices', schema='in.c-crm') is True
        assert rules.is_governed(table_name='customers', schema='in.c-crm') is False
        assert rules.is_governed(table_name='invoices', schema=None) is False
        assert rules.is_governed(table_name='invoices', schema='') is False

    def test_references_governed_table_true_when_a_governed_table_is_touched(self, rules: RlsRules) -> None:
        sql = 'SELECT * FROM "in.c-crm"."invoices" JOIN "in.c-crm"."unrelated" ON 1=1'
        assert references_governed_table(sql, dialect='snowflake', rules=rules) is True

    def test_references_governed_table_false_when_nothing_governed_is_touched(self, rules: RlsRules) -> None:
        sql = 'SELECT * FROM "in.c-crm"."unrelated"'
        assert references_governed_table(sql, dialect='snowflake', rules=rules) is False

    def test_references_governed_table_false_for_unparseable_sql(self, rules: RlsRules) -> None:
        assert references_governed_table('not ( valid sql at', dialect='snowflake', rules=rules) is False

    def test_references_governed_table_lenient_about_multiple_statements(self, rules: RlsRules) -> None:
        """Unlike `rewrite_query`, this must not reject multi-statement input outright -- it only
        answers "would rewrite_query need to touch this," and a query with zero governed tables
        should never be routed into the paranoid pipeline just because it has two statements."""
        sql = 'SELECT 1; SELECT 2'
        assert references_governed_table(sql, dialect='snowflake', rules=rules) is False
        sql_governed = 'SELECT 1; SELECT * FROM "in.c-crm"."invoices"'
        assert references_governed_table(sql_governed, dialect='snowflake', rules=rules) is True


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
    def test_ungoverned_table_alone_is_left_untouched(self, rules: RlsRules) -> None:
        """RLS is opt-in per table (see `RlsRules.is_governed`): a table no policy names at all
        must not be refused just because `rewrite_query` was called -- it's the caller's job
        (`references_governed_table`) to only call this function when at least one table in the
        query needs it; this test proves that when it does, an unrelated table is still fine."""
        out = rewrite_query('SELECT * FROM "in.c-crm"."unrelated"', user='petr', dialect='snowflake', rules=rules)
        assert out.sql == 'SELECT * FROM "in.c-crm"."unrelated"'
        assert out.applied_rules == []

    def test_governed_and_ungoverned_tables_in_one_query(self, rules: RlsRules) -> None:
        """A join between a protected and an unrelated table filters only the protected one --
        the unrelated table is neither wrapped nor required to have a rule for `petr`."""
        out = rewrite_query(
            'SELECT i.id FROM "in.c-crm"."invoices" i JOIN "in.c-crm"."unrelated" u ON u.id = i.id',
            user='petr',
            dialect='snowflake',
            rules=rules,
        )
        assert out.sql == (
            'SELECT i.id FROM (SELECT * FROM "in.c-crm"."invoices" WHERE country = \'CZ\') AS i '
            'JOIN "in.c-crm"."unrelated" AS u ON u.id = i.id'
        )
        assert out.applied_rules == ['in.c-crm.invoices']

    @pytest.mark.parametrize(
        ('sql', 'dialect', 'expected_sql', 'expected_rules'),
        [
            (
                'SELECT COUNT(*) FROM "in.c-crm"."invoices"',
                'snowflake',
                "SELECT COUNT(*) FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ['in.c-crm.invoices'],
            ),
            (
                'SELECT i.id FROM "in.c-crm"."invoices" i JOIN "in.c-crm"."orders" AS o ON o.id = i.id',
                'snowflake',
                (
                    "SELECT i.id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS i "
                    "JOIN (SELECT * FROM \"in.c-crm\".\"orders\" WHERE country = 'CZ' AND status <> 'draft') AS o "
                    'ON o.id = i.id'
                ),
                ['in.c-crm.invoices', 'in.c-crm.orders'],
            ),
            (
                # The original alias is quoted -- the rewrite must keep it quoted, not borrow the
                # table identifier's own (unquoted) quoting.
                'SELECT "I".id FROM "in.c-crm"."invoices" AS "I"',
                'snowflake',
                'SELECT "I".id FROM (SELECT * FROM "in.c-crm"."invoices" WHERE country = \'CZ\') AS "I"',
                ['in.c-crm.invoices'],
            ),
            (
                'WITH x AS (SELECT * FROM "in.c-crm"."invoices") SELECT * FROM x',
                'snowflake',
                "WITH x AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\") SELECT * FROM x",
                ['in.c-crm.invoices'],
            ),
            (
                # A CTE may reference an earlier sibling CTE.
                'WITH a AS (SELECT * FROM "in.c-crm"."invoices"), b AS (SELECT * FROM a) SELECT * FROM b',
                'snowflake',
                (
                    "WITH a AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"), "
                    'b AS (SELECT * FROM a) SELECT * FROM b'
                ),
                ['in.c-crm.invoices'],
            ),
            (
                # A recursive CTE references itself from inside its own body.
                'WITH RECURSIVE r AS (SELECT id FROM "in.c-crm"."invoices" UNION ALL SELECT id FROM r) SELECT * FROM r',
                'snowflake',
                (
                    "WITH RECURSIVE r AS (SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'UNION ALL SELECT id FROM r) SELECT * FROM r'
                ),
                ['in.c-crm.invoices'],
            ),
            (
                # Quoted CTE alias, quoted reference, same case: the same name on both engines.
                'WITH "X" AS (SELECT * FROM "in.c-crm"."invoices") SELECT * FROM "X"',
                'snowflake',
                (
                    'WITH "X" AS (SELECT * FROM (SELECT * FROM "in.c-crm"."invoices" WHERE country = \'CZ\') AS "invoices") '
                    'SELECT * FROM "X"'
                ),
                ['in.c-crm.invoices'],
            ),
            (
                # Unquoted identifiers fold case on both Snowflake and BigQuery, so `x` and `X` are
                # the same name and this is an ordinary CTE reference -- it must not be refused.
                'WITH x AS (SELECT * FROM "in.c-crm"."invoices") SELECT * FROM X',
                'snowflake',
                "WITH x AS (SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\") SELECT * FROM X",
                ['in.c-crm.invoices'],
            ),
            (
                'WITH `X` AS (SELECT * FROM `in_c_crm`.`invoices`) SELECT * FROM `X`',
                'bigquery',
                (
                    'WITH `X` AS (SELECT * FROM (SELECT * FROM `in_c_crm`.`invoices` '
                    "WHERE country = 'CZ') AS `invoices`) SELECT * FROM `X`"
                ),
                ['in_c_crm.invoices'],
            ),
            (
                # A subquery may declare its own CTE as long as the name shadows nothing outside it.
                'SELECT * FROM "in.c-crm"."invoices" WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)',
                'snowflake',
                (
                    "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'WHERE 1 IN (WITH t AS (SELECT 1) SELECT * FROM t)'
                ),
                ['in.c-crm.invoices'],
            ),
            (
                'SELECT * FROM (SELECT id FROM "in.c-crm"."invoices") sub',
                'snowflake',
                "SELECT * FROM (SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\") AS sub",
                ['in.c-crm.invoices'],
            ),
            (
                'SELECT id FROM "in.c-crm"."invoices" UNION ALL SELECT id FROM "in.c-sales"."orders"',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'UNION ALL SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ['in.c-crm.invoices', 'in.c-sales.orders'],
            ),
            (
                'SELECT id FROM "in.c-crm"."invoices" EXCEPT SELECT id FROM "in.c-sales"."orders"',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'EXCEPT SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ['in.c-crm.invoices', 'in.c-sales.orders'],
            ),
            (
                'SELECT id FROM "in.c-crm"."invoices" INTERSECT SELECT id FROM "in.c-sales"."orders"',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'INTERSECT SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ['in.c-crm.invoices', 'in.c-sales.orders'],
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
                ['in.c-crm.invoices'],
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
                ['in_c_crm.invoices'],
            ),
            (
                'SELECT COUNT(*) FROM `in_c_crm`.`invoices` LIMIT 10',
                'bigquery',
                (
                    'SELECT COUNT(*) FROM (SELECT * FROM `in_c_crm`.`invoices` '
                    "WHERE country = 'CZ') AS `invoices` LIMIT 10"
                ),
                ['in_c_crm.invoices'],
            ),
            (
                # A whole statement in parentheses means the statement inside them.
                '(SELECT * FROM "in.c-crm"."invoices")',
                'snowflake',
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ['in.c-crm.invoices'],
            ),
            (
                '((SELECT * FROM "in.c-crm"."invoices"))',
                'snowflake',
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ['in.c-crm.invoices'],
            ),
            (
                '(SELECT id FROM "in.c-crm"."invoices" UNION ALL SELECT id FROM "in.c-sales"."orders")',
                'snowflake',
                (
                    "SELECT id FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" "
                    'UNION ALL SELECT id FROM (SELECT * FROM "in.c-sales"."orders" WHERE FALSE) AS "orders"'
                ),
                ['in.c-crm.invoices', 'in.c-sales.orders'],
            ),
            (
                # A trailing semicolon is still exactly one statement.
                'SELECT * FROM "in.c-crm"."invoices";',
                'snowflake',
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\"",
                ['in.c-crm.invoices'],
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
                ['in_c_crm.invoices'],
            ),
            (
                # Window function + QUALIFY over a protected table.
                'SELECT id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY id) rn FROM "in.c-crm"."invoices" QUALIFY rn = 1',
                'snowflake',
                (
                    'SELECT id, ROW_NUMBER() OVER (PARTITION BY country ORDER BY id) AS rn '
                    "FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS \"invoices\" QUALIFY rn = 1"
                ),
                ['in.c-crm.invoices'],
            ),
            (
                # A CTE named after a protected table shadows nothing: the protected reference always
                # carries its bucket, and a CTE alias never can.
                (
                    'WITH RECURSIVE invoices AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM invoices WHERE n < 3) '
                    'SELECT * FROM invoices'
                ),
                'snowflake',
                (
                    'WITH RECURSIVE invoices AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM invoices WHERE n < 3) '
                    'SELECT * FROM invoices'
                ),
                [],
            ),
            (
                'WITH "Invoices" AS (SELECT 1) SELECT * FROM "Invoices"',
                'snowflake',
                'WITH "Invoices" AS (SELECT 1) SELECT * FROM "Invoices"',
                [],
            ),
            (
                'WITH `Invoices` AS (SELECT 1) SELECT * FROM `Invoices`',
                'bigquery',
                'WITH `Invoices` AS (SELECT 1) SELECT * FROM `Invoices`',
                [],
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
                ['in.c-crm.invoices'],
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
            # BigQuery dataset and table names are case-sensitive, so a differently-cased reference
            # names a different table -- and there is no rule for it.
            ('SELECT * FROM `in_c_crm`.`Invoices`', 'petr', 'bigquery', "table 'in_c_crm.Invoices'"),
            ('SELECT * FROM `IN_C_CRM`.`invoices`', 'petr', 'bigquery', "table 'IN_C_CRM.invoices'"),
            # A CTE reading its own name without RECURSIVE resolves to the base table on the
            # engine, whatever the CTE is called.
            (
                'WITH invoices AS (SELECT * FROM invoices) SELECT * FROM invoices',
                'petr',
                'snowflake',
                'without RECURSIVE',
            ),
            # A reference whose quoting differs from the declaration is ambiguous either way.
            ('WITH "orders" AS (SELECT 1) SELECT * FROM ORDERS', 'petr', 'snowflake', 'ambiguous'),
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
            # Same trick aimed at a table that does have a rule: the CTE is declared in a nested
            # scope, so it cannot excuse the outer reference.
            (
                'SELECT * FROM invoices WHERE 1 IN (WITH invoices AS (SELECT 1) SELECT 1)',
                'petr',
                'snowflake',
                'another scope',
            ),
            # A CTE reference whose quoting differs from the declaration is ambiguous -- the engine
            # and the rewriter can disagree about whether it resolves to the CTE or a real table.
            ('WITH "secret" AS (SELECT 1) SELECT * FROM SECRET', 'petr', 'snowflake', 'ambiguous'),
            # Quoted identifiers are case-sensitive on Snowflake and BigQuery, so `"SECRET"` binds to
            # the base table, not to the `"secret"` CTE. Treating them as one name would let the real
            # table through unfiltered.
            ('WITH "secret" AS (SELECT 1) SELECT * FROM "SECRET"', 'petr', 'snowflake', 'another scope'),
            ('WITH "SECRET" AS (SELECT 1) SELECT * FROM "secret"', 'petr', 'snowflake', 'another scope'),
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

    def test_cte_named_after_a_protected_table_still_wraps_the_table(self, rules: RlsRules) -> None:
        """A CTE alias is always bare and a rules key always names a bucket, so a CTE can never
        stand in for a protected table. Naming one after a table is therefore allowed -- and it is
        the natural way to write this query -- while the real reference inside it is still wrapped.
        """
        out = rewrite_query(
            'WITH invoices AS (SELECT * FROM "in.c-crm"."invoices" WHERE amount > 0) SELECT COUNT(*) FROM invoices',
            user='petr',
            dialect='snowflake',
            rules=rules,
        )

        assert out.sql == (
            'WITH invoices AS (SELECT * FROM (SELECT * FROM "in.c-crm"."invoices" '
            'WHERE country = \'CZ\') AS "invoices" WHERE amount > 0) SELECT COUNT(*) FROM invoices'
        )
        assert out.applied_rules == ['in.c-crm.invoices']

    def test_rewrite_predicate_invalid_for_dialect_fails_closed(self) -> None:
        """`_compile_primitive` can only ever produce valid SQL, so this shape isn't reachable via
        `RlsRules.from_metastore()` in practice -- but `rewrite_query()`'s own defensive re-parse of
        the stored predicate text (`_transform`'s `sqlglot.parse_one(..., dialect=dialect)` call) is
        exercised directly here, by building the `RlsRules` object by hand, to prove it fails closed
        rather than raising a raw `sqlglot` exception if a bad string ever got in some other way.
        """
        bad_rules = RlsRules(tables={'in.c-crm.invoices': {'petr': 'country = = 1'}}, dialect='snowflake')
        with pytest.raises(RlsError):
            rewrite_query('SELECT * FROM "in.c-crm"."invoices"', user='petr', dialect='snowflake', rules=bad_rules)

    @pytest.mark.parametrize(
        ('predicate', 'match'),
        [
            # Injection attempt: the predicate must parse as a bare condition, nothing more. The
            # refusal names the rule's key and nothing else -- not the predicate that failed.
            ('TRUE) AS x, (SELECT * FROM secret WHERE (TRUE', 'rule for table in.c-crm.invoices could not be'),
            # A predicate referencing another table leaves that table outside a wrapper -- the
            # output invariant refuses it rather than letting an unfiltered reference through.
            ('id IN (SELECT id FROM secret)', 'unwrapped table reference'),
        ],
    )
    def test_rewrite_rejects_predicates_that_are_not_plain_conditions(self, predicate: str, match: str) -> None:
        bad_rules = RlsRules(tables={'in.c-crm.invoices': {'petr': predicate}}, dialect='snowflake')
        with pytest.raises(RlsError, match=match):
            rewrite_query('SELECT * FROM "in.c-crm"."invoices"', user='petr', dialect='snowflake', rules=bad_rules)


class TestDialectIdentifierSemantics:
    """The two backends resolve names differently, and RLS follows each rather than picking one.

    Verified against a live BigQuery workspace: CTE names there are case-insensitive and backticks
    around one are semantically null, while dataset and table names ARE case-sensitive. Snowflake is
    the other way round for the quoting question, and its workspace FQNs come back quoted in the
    storage case, so rule keys are matched case-insensitively there.
    """

    @pytest.mark.parametrize(
        'sql',
        [
            'WITH secret AS (SELECT 1 AS x) SELECT * FROM SECRET',
            'WITH `secret` AS (SELECT 1 AS x) SELECT * FROM secret',
            'WITH secret AS (SELECT 1 AS x) SELECT * FROM `SECRET`',
            'WITH Secret AS (SELECT 1 AS x) SELECT * FROM secret',
            'WITH `Secret` AS (SELECT 1 AS x) SELECT * FROM `secret`',
        ],
    )
    def test_bigquery_cte_names_ignore_case_and_backticks(self, bq_rules: RlsRules, sql: str) -> None:
        """All five shapes return the CTE row on BigQuery, so all five are CTE references here.
        Refusing them as "ambiguous" or "another scope" would protect nothing and reject real work.
        """
        out = rewrite_query(sql, user='petr', dialect='bigquery', rules=bq_rules)

        assert out.applied_rules == []

    @pytest.mark.parametrize(
        ('sql', 'match'),
        [
            # The looser name rule does not loosen the two that matter.
            ('WITH secret AS (SELECT * FROM `SECRET`) SELECT * FROM secret', 'without RECURSIVE'),
            ('SELECT * FROM secret WHERE 1 IN (WITH SECRET AS (SELECT 1) SELECT 1)', 'another scope'),
        ],
    )
    def test_bigquery_still_refuses_real_shadowing(self, bq_rules: RlsRules, sql: str, match: str) -> None:
        with pytest.raises(RlsError, match=match):
            rewrite_query(sql, user='petr', dialect='bigquery', rules=bq_rules)

    @pytest.mark.parametrize(
        'sql',
        [
            # Snowflake keeps the quoted/unquoted distinction: `"SECRET"` is the base table.
            'WITH "secret" AS (SELECT 1) SELECT * FROM "SECRET"',
            'WITH "SECRET" AS (SELECT 1) SELECT * FROM "secret"',
        ],
    )
    def test_snowflake_quoting_rules_are_unchanged(self, rules: RlsRules, sql: str) -> None:
        with pytest.raises(RlsError, match='another scope'):
            rewrite_query(sql, user='petr', dialect='snowflake', rules=rules)

    def test_snowflake_rule_keys_stay_case_insensitive(self, rules: RlsRules) -> None:
        """A workspace FQN from `get_tables` is quoted in the storage case, and Snowflake resolves
        `"IN.C-CRM"."INVOICES"` and `"in.c-crm"."invoices"` to the same table."""
        out = rewrite_query('SELECT * FROM "IN.C-CRM"."INVOICES"', user='petr', dialect='snowflake', rules=rules)

        assert out.applied_rules == ['in.c-crm.invoices']

    def test_bigquery_rule_keys_keep_their_case(self, bq_rules: RlsRules) -> None:
        assert sorted(bq_rules.tables) == ['in_c_crm.invoices', 'in_c_crm.orders', 'in_c_sales.orders']


class TestDisclosure:
    """What reaches the caller is the fact that a table was filtered -- never the filter itself."""

    def test_applied_rules_are_keys_only_and_deduplicated(self, rules: RlsRules) -> None:
        out = rewrite_query(
            'SELECT id FROM "in.c-crm"."invoices" '
            'UNION ALL SELECT id FROM "in.c-crm"."invoices" '
            'UNION ALL SELECT id FROM "in.c-sales"."orders"',
            user='petr',
            dialect='snowflake',
            rules=rules,
        )

        assert out.applied_rules == ['in.c-crm.invoices', 'in.c-sales.orders']
        assert all("country = 'CZ'" not in entry and 'FALSE' not in entry for entry in out.applied_rules)

    @pytest.mark.parametrize(
        'predicate',
        [
            'country = = 1',  # fails to parse at rewrite time
            'id IN (SELECT id FROM undisclosed_table)',  # leaves a table outside the wrapper
        ],
    )
    def test_predicate_failures_do_not_echo_the_predicate(self, predicate: str, caplog) -> None:
        """A rules object built directly, bypassing `load()`, so the rewrite-time guards are the
        ones under test. The message the caller sees must not carry the predicate, nor a table name
        that appears only inside it -- the detail belongs in the server log."""
        bad_rules = RlsRules(tables={'in.c-crm.invoices': {'petr': predicate}}, dialect='snowflake')

        with caplog.at_level('WARNING', logger='keboola_mcp_server.rls'), pytest.raises(RlsError) as excinfo:
            rewrite_query('SELECT * FROM "in.c-crm"."invoices"', user='petr', dialect='snowflake', rules=bad_rules)

        message = str(excinfo.value)
        assert 'undisclosed_table' not in message
        assert 'country' not in message
        assert caplog.text  # the detail is logged for the operator


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
            # A bare, unqualified table name matches no governed key at all (see `RlsRules.
            # is_governed`) and is therefore *not* an error here -- it's ordinary, ungoverned
            # `query_data` territory, untouched by design. What IS still an error: a table that
            # DOES match a governed key (`in.c-sales.orders`, in `OUTPUT_PREDICATES`) left unwrapped
            # alongside a correctly-wrapped one.
            (
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ') AS i "
                'JOIN "in.c-sales"."orders" o ON TRUE',
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
            (
                'SELECT * FROM (SELECT * FROM "in.c-crm"."invoices") AS "invoices"',
                'rule for table in.c-crm.invoices could not be applied',
            ),
            # A WHERE that is not the rule -- weakened, negated or simply a different condition.
            (
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'DE') AS \"invoices\"",
                'rule for table in.c-crm.invoices could not be applied',
            ),
            (
                'SELECT * FROM (SELECT * FROM "in.c-crm"."invoices" WHERE TRUE) AS "invoices"',
                'rule for table in.c-crm.invoices could not be applied',
            ),
            (
                "SELECT * FROM (SELECT * FROM \"in.c-crm\".\"invoices\" WHERE country = 'CZ' OR TRUE) AS \"invoices\"",
                'rule for table in.c-crm.invoices could not be applied',
            ),
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
