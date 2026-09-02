# RFC: Row-Level Security Query Tool (`query_data_rls`)

Linear: _TBD — pilot; no Linear issue assigned yet_

Status: **Draft / pilot** — this branch is not meant to be merged as-is.

## Problem

Today `query_data` runs whatever SQL the model produces against the Keboola workspace with the
full privileges of the configured token. There is no way to expose a shared table to several
users while letting each of them see only *their* slice (orders per country, invoices per
cost-centre, ...). A model answering on a user's behalf must receive already-filtered data — it
cannot be trusted to add the filter itself.

Agnes (keboola/agnes-the-ai-analyst#1979) solves this with "Table Access Policies": a policy is
data (a SQL `SELECT` attached to a table), the server substitutes it for the table on every read,
enforcement is fail-closed, and every filtered response carries a disclosure. This RFC ports the
*minimum* of that idea into the MCP server. Explicitly out of scope (KISS): groups, `$user_*`
variables, column masking, mapping tables, transpilation between dialects, audit log, admin UI,
hot reload.

## Required Behavior

### Rules file

Rules live in a YAML file whose path is given by the `KBC_RLS_RULES_PATH` environment variable
or the `--rls-rules-path` CLI flag. The path is deployment-level configuration: it is **never**
read from an HTTP header (the `Config` field is not in `_HEADER_ELIGIBLE_FIELDS`).

```yaml
# table -> user -> SQL predicate (inserted into WHERE verbatim, in the workspace dialect)
tables:
  invoices:
    petr: "country = 'CZ'"
    monika: "country = 'DE'"
    admin: "TRUE"                       # unrestricted access must be written explicitly
  in.c-crm.orders:                      # bucket-qualified key, takes precedence over a bare name
    petr: "country = 'CZ' AND status <> 'draft'"
```

- Table keys are matched case-insensitively against the table reference in the SQL. A
  bucket-qualified key (`<bucket>.<table>`, i.e. `<schema>.<name>` in the workspace) is tried
  first, then the bare table name.
- User keys are matched case-insensitively.
- The file is loaded and validated **once at server start**. Every predicate must parse as a SQL
  condition (`sqlglot`, dialect-agnostic parse). An unreadable, malformed or empty file, or an
  unparseable predicate, makes the server fail to start with a message naming the offending
  table/user. No silent defaults.

### SQL rewrite

`rewrite_query(sql, user, dialect) -> (rewritten_sql, applied_rules)` in a new module
`keboola_mcp_server/rls.py`:

1. Parse `sql` with `sqlglot` using the workspace dialect (`snowflake` / `bigquery`, obtained via
   `WorkspaceManager.get_sql_dialect()`).
2. Reject (raise) if the input is not exactly one statement or the statement is not a `SELECT`
   (CTEs and set operations on top of `SELECT` are allowed).
3. Walk every `exp.Table` node that references a real table (names that are CTE aliases defined
   in the same statement are skipped).
4. For each such table look up the predicate for `user`. Missing table or missing user ⇒ raise
   with the table name in the message. Never fall back to an unfiltered table.
5. Replace the node with a subquery `(SELECT * FROM <original table> WHERE <predicate>)` aliased
   to the original alias, or to the bare table name when there was no alias, so the rest of the
   query keeps resolving.
6. Generate SQL back in the same dialect and return it together with a human-readable list of
   applied rules, e.g. `["invoices: country = 'CZ'"]`.

Example — input `SELECT COUNT(*) FROM invoices` for user `petr` becomes
`SELECT COUNT(*) FROM (SELECT * FROM invoices WHERE country = 'CZ') AS invoices`.

The whole path is fail-closed: any exception in parsing, lookup or rewriting propagates as a tool
error and no SQL is sent to the workspace.

### Tool

```python
async def query_data_rls(
    sql_query: str,      # same semantics as query_data
    query_name: str,     # same semantics as query_data
    user: str,           # REQUIRED — identity used to select the RLS rules
    ctx: Context,
) -> RlsQueryDataOutput
```

`RlsQueryDataOutput` extends `QueryDataOutput` with `applied_rules: list[str]` so the model and
the human always see that the result is a filtered slice (Agnes "disclosure"). The tool is
annotated `readOnlyHint=True` and tagged `sql` like `query_data`. Its docstring reuses the SQL
guidance from `query_data` and adds: which user to pass, that results are filtered, and that
tables without a rule for the user are inaccessible.

Query execution (progress notification, disconnect watching, CSV serialisation) is extracted from
`query_data` into a shared private helper in `tools/sql.py`; both tools call it.

### Server behaviour (tool swap)

`add_sql_tools(mcp, rls_rules)`:

- `rls_rules is None` (no path configured) ⇒ register `query_data` only — **no behaviour change**.
- `rls_rules` present ⇒ register `query_data_rls` **only**; `query_data` is not registered at all,
  so it cannot be re-enabled through `X-Allowed-Tools` or any other header.

`create_server()` loads the rules when `config.rls_rules_path` is set and stores them in
`ServerState` so the tool can read them from `ctx.request_context.lifespan_context`.

### Trust boundary (documented limitation)

The `user` argument is supplied by the MCP client / model; the server cannot verify it. This is
the same trust level as today's `X-*` request headers and is acceptable for the pilot. A later
iteration may bind the user from an authenticated HTTP header instead of a tool argument.

## Scope and Constraints

- Only `SELECT` statements; no DDL/DML through the RLS tool.
- No new dependencies: `sqlglot` and `pyyaml` are already required.
- Predicates are written by the admin in the workspace dialect; the server does not transpile.
- No changes to `query_data` behaviour when RLS is not configured.
- All configuration via `Config`; no hardcoded paths or defaults.

## Testing

Unit tests (`tests/test_rls.py`), parametrised where possible:

- rules loading: valid file, missing file, empty file, missing `tables` key, unparseable
  predicate → error names the table/user; case-insensitive lookup; bucket-qualified key
  precedence.
- rewrite: plain `SELECT`, table alias, two-table `JOIN`, `WITH` CTE referencing a real table
  (CTE alias itself not rewritten), nested subquery, `UNION`, bucket-qualified table reference,
  Snowflake and BigQuery quoting round-trip.
- fail-closed: unknown table, unknown user, non-`SELECT`, multiple statements, unparseable SQL.

Tool tests (`tests/tools/test_sql.py`): `query_data_rls` sends the rewritten SQL to
`WorkspaceManager.execute_query` and returns `applied_rules`; error from rewrite never reaches the
workspace.

Config / server tests: new field reachable from env/CLI and unreachable from headers
(`tests/test_config.py`); `add_sql_tools` registers exactly one of the two tools depending on the
rules (`tests/test_server.py`).

`TOOLS.md` regenerated (`tox -e check-tools-docs`). README gets a short "Row-Level Security
(pilot)" section under the local setup.
