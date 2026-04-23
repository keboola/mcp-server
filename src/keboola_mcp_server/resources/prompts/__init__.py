from importlib import resources

_DIALECT_CONFIGS: dict[str, dict] = {
    'BigQuery': {
        'delimiter': 'backtick (`` ` ``)',
        'col': '`column_name`',
        'fqn': '`project`.`dataset`.`table`',
        'new_table': '`table_name`',
        'extra': [],
    },
    'Snowflake': {
        'delimiter': 'double quote (`"`)',
        'col': '"column_name"',
        'fqn': '"DATABASE"."SCHEMA"."TABLE"',
        'new_table': '"table_name"',
        'extra': [
            'Unquoted identifiers and column aliases are auto-uppercased by Snowflake — '
            'always use delimited identifiers to preserve case.',
            'Use `LISTAGG` instead of `STRING_AGG`.',
            'In CTEs, use delimited identifiers for every column alias so the name survives '
            'into the outer query unchanged.',
        ],
    },
}


def _build_dialect_section(sql_dialect: str) -> str:
    cfg = _DIALECT_CONFIGS.get(sql_dialect)
    if not cfg:
        return f'### SQL Identifiers\n\nSQL dialect: **{sql_dialect}**.\n'
    lines = [
        '### SQL Identifiers\n',
        f'This project uses **{sql_dialect}** SQL dialect.',
        f'The delimited identifier character is the {cfg["delimiter"]}.',
        '**Always wrap every identifier** (column name, table name, alias) ' 'in delimited identifiers:\n',
        f'- Column reference: {cfg["col"]}',
        f'- Fully qualified table name: {cfg["fqn"]}',
        f'- New table in CREATE TABLE (table name only, no FQN): {cfg["new_table"]}',
        '- Never mix delimiter styles within a single query.\n',
    ]
    for note in cfg['extra']:
        lines.append(f'- {note}')
    return '\n'.join(lines)


def load_prompt(name: str) -> str:
    return resources.read_text(__package__, name)


def get_project_system_prompt(sql_dialect: str = '') -> str:
    base = load_prompt('project_system_prompt.md')
    if not sql_dialect:
        return base
    return _build_dialect_section(sql_dialect) + '\n\n---\n\n' + base
