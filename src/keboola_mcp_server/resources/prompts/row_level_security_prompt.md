### Row-Level Security

This project enforces row-level security. The MCP server lists and queries only the tables that a security policy
grants to the current user. A table without a policy is hidden for every user, administrators included.

- An empty bucket, a table that is "not found", or a table listed as hidden by policy may be caused by the policies,
  not by missing data. Tell the user this; never claim that such a table does not exist.
- `Row-level security does not allow this query` from `query_data` means that no policy grants the table, or that
  the query shape is refused. Do not retry with rewritten SQL to get around it.
- Policies are managed in Project settings > Data access.
