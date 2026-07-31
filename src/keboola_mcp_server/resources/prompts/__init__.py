import logging
from importlib import resources

LOG = logging.getLogger(__name__)

_DIALECT_CONFIGS: dict[str, dict] = {
    'BigQuery': {
        'delimiter': 'backtick (`` ` ``)',
        'col': '`column_name`',
        'fqn': '`project`.`dataset`.`table`',
        'new_table': '`table_name`',
        'extra': [],
        'functions': [
            (
                "Storage columns are untyped text (`STRING`), so empty cells are stored as `''` (not NULL). "
                'Cast text to a typed value with `SAFE_CAST(x AS DATE|TIMESTAMP|NUMERIC)` **before** `DATE_TRUNC`, '
                '`EXTRACT`, or numeric aggregation — passing raw text fails with `... does not support ... argument '
                'type`. BigQuery has **no** `TRY_CAST`; use `SAFE_CAST`, which returns NULL on bad input (including '
                'empty strings).'
            ),
            (
                'For numeric casts use `NUMERIC` (or `BIGNUMERIC`, or `FLOAT64` for ratios/averages), which keep '
                'the fractional part. Never `SAFE_CAST` a possibly-fractional value to bare `INT64` — it '
                'truncates/rounds to an integer and silently corrupts money and durations.'
            ),
            (
                'Window frames: a frame clause (`ROWS`/`RANGE BETWEEN ...`) requires an `ORDER BY` inside the '
                '`OVER (...)` and is only valid on functions that accept a frame (aggregates such as `SUM`/`AVG`, '
                'plus `FIRST_VALUE`/`LAST_VALUE`). Numbering and navigation functions (`ROW_NUMBER`, `RANK`, '
                '`DENSE_RANK`, `LAG`, `LEAD`) take no frame — adding one raises `Syntax error: Unexpected keyword '
                'ROWS`. Example: `SUM(SAFE_CAST(`amount` AS NUMERIC)) OVER (ORDER BY `created_at` ROWS BETWEEN '
                'UNBOUNDED PRECEDING AND CURRENT ROW)`.'
            ),
        ],
    },
    'Snowflake': {
        'delimiter': 'double quote (`"`)',
        'col': '"column_name"',
        'fqn': '"DATABASE"."SCHEMA"."TABLE"',
        'new_table': '"table_name"',
        'extra': [
            (
                'Unquoted identifiers and column aliases are auto-uppercased by Snowflake — '
                'always use delimited identifiers to preserve case.'
            ),
            'Use `LISTAGG` instead of `STRING_AGG`.',
            (
                'In CTEs, use delimited identifiers for every column alias so the name survives '
                'into the outer query unchanged.'
            ),
        ],
        'functions': [
            (
                "Storage columns are untyped text (`VARCHAR`), so empty cells are stored as `''` (not NULL). "
                'Cast text to a typed value with `TRY_CAST(x AS DATE|TIMESTAMP)` (or `TRY_TO_DATE`) **before** '
                '`DATE_TRUNC`, `EXTRACT`, or numeric aggregation — passing raw text fails with `... does not '
                'support VARCHAR argument type`. `TRY_CAST` returns NULL on bad input (including empty strings) '
                'instead of erroring.'
            ),
            (
                'For numeric casts always specify precision and scale — use `TRY_CAST(x AS NUMBER(38,9))` or '
                '`TRY_TO_NUMBER(x, 38, 9)` (or `FLOAT`/`DOUBLE` for ratios/averages). Never cast to bare '
                '`NUMBER`/`DECIMAL`/`NUMERIC`: bare `NUMBER` is `NUMBER(38,0)` (integer) and silently rounds '
                "fractional values (`TRY_CAST('3.75' AS NUMBER)` → 4), corrupting money and durations."
            ),
            (
                'Window frames: a frame clause (`ROWS`/`RANGE BETWEEN ...`) requires an `ORDER BY` inside the '
                '`OVER (...)` and is only valid on aggregate functions (`SUM`/`AVG`, etc.) and `FIRST_VALUE`/'
                '`LAST_VALUE`. Ranking and navigation functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`) '
                'take no frame. Example: `SUM(TRY_CAST("amount" AS NUMBER(38,9))) OVER (ORDER BY "created_at" ROWS '
                'BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.'
            ),
        ],
    },
}


def _build_dialect_section(sql_dialect: str) -> str:
    cfg = _DIALECT_CONFIGS.get(sql_dialect)
    if not cfg:
        LOG.warning('Unknown SQL dialect %r — no dialect-specific identifier guidance will be emitted.', sql_dialect)
        return f'### SQL Identifiers\n\nSQL dialect: **{sql_dialect}**.\n'
    lines = [
        '### SQL Identifiers\n',
        f'This project uses **{sql_dialect}** SQL dialect.',
        f'The delimited identifier character is the {cfg["delimiter"]}.',
        ('**Always wrap every identifier** (column name, table name, alias) ' 'in delimited identifiers:\n'),
        f'- Column reference: {cfg["col"]}',
        f'- Fully qualified table name: {cfg["fqn"]}',
        f'- New table in CREATE TABLE (table name only, no FQN): {cfg["new_table"]}',
        '- Never mix delimiter styles within a single query.\n',
    ]
    for note in cfg['extra']:
        lines.append(f'- {note}')

    functions = cfg.get('functions') or []
    if functions:
        lines.append('\n### SQL Functions & Casting\n')
        for note in functions:
            lines.append(f'- {note}')

    return '\n'.join(lines)


def load_prompt(name: str) -> str:
    return resources.files(__package__).joinpath(name).read_text(encoding='utf-8')


def get_project_system_prompt(sql_dialect: str = '') -> str:
    base = load_prompt('project_system_prompt.md')
    if not sql_dialect:
        return base
    return _build_dialect_section(sql_dialect) + '\n\n---\n\n' + base
