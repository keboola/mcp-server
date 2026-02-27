# Release Notes

## Version 1.46.0

## Key Changes

### New Features
- **Config-based search across all item types (AI-2161, PR #348)**: Extended the `search` tool beyond bucket/table usage references to support full configuration-aware discovery. Agents can now search across component configurations, transformations, flows, and data apps using JSONPath traversal. Search results include `match_scopes` in each `SearchHit` showing the exact matched JSON paths (e.g., `parameters.api.baseUrl`). Simplified the API by removing `return_all_matches` and `case_sensitive` parameters. Updated tool docs and project system prompt to explicitly cover config-based search usage.
- **Role and toolset restrictions in `get_project_info` (AI-2505, PR #403)**: The `get_project_info` tool now returns `user_role` and `toolset_restrictions` fields derived from the user token's role. This allows AI assistants to understand and respect the limitations of the user's access permissions without having to infer them from trial-and-error tool calls.

### Enhancements
- **OAuth bearer token for Query Service authentication (AI-2638, PR #400)**: Query Service requests now prefer the OAuth bearer token (`Authorization: Bearer`) over the legacy storage token (`X-StorageAPI-Token`) when an OAuth token is available. Falls back transparently to the storage token for backward compatibility. Data app templates updated to detect and use the correct header type.

### Bug Fixes
- **Read-only access enforcement for `readonly` role (AI-2442, PR #397)**: The `readonly` role is now strictly enforced: `on_list_tools` filters the tool list to only tools annotated with `readOnlyHint=True`, and `on_call_tool` rejects any write-tool call with a clear error message. The `guest` role is unchanged — it follows the existing `modify_flow`/`update_flow` filtering and can still call write tools. Also: 403 errors from the analytics event-trigger HTTP call are now silently swallowed (expected for roles that lack write access to the analytics endpoint), and removed the implicit `requests` dependency and its `JSONDecodeError` handler from `cli.py`.
- **Config-based search `match_scopes` broken by `jsonpath_ng` 1.8.0 (AI-2662, PR #401)**: `jsonpath_ng` 1.8.0 (released 2026-02-24) changed `Child.__str__()` to wrap path segments in parentheses and single-quote fields with special characters, corrupting `match_scopes` in search results. Added a `_clean_jsonpath_path_str()` helper to normalize path strings back to clean dot-bracket notation. Dependency constraint bumped from `~=1.7` to `~=1.8`.

## Plans for Customer Communication

- **Communication channels**: No active notification required
- **Timeline**: Deployment will occur automatically via CI/CD pipeline
- **Customer action**: No action required from customers
- **Transparency**: This is a transparent update; new fields in `get_project_info` are additive and backward compatible

## Impact Analysis

- **Affected Users**: All MCP server users; `readonly`-role users are specifically restricted to read-only tools
- **Customer Action Required**: None — all changes are backward compatible
- **Service interruption**: None expected — deployment is transparent
- **Risk level**: Low
  - Config-based search is an additive extension; existing search behavior is preserved
  - `get_project_info` role/toolset fields are new additions that do not affect existing callers
  - OAuth bearer token for Query Service falls back to storage token automatically
  - Read-only role restriction blocks access that was previously (incorrectly) permitted — no valid use case is removed
  - `jsonpath_ng` 1.8.0 fix restores correct behavior that was broken by the dependency update

## Change Type

**Minor** — New features (config-based search, role info in project info) and enhancements with no breaking changes. Bug fixes included.

## Justification

- **Config-based search (AI-2161)**: Agents struggled to find configurations by content (e.g., which component uses a specific API endpoint). The search tool only covered bucket/table usage; configuration inspection required agents to iterate over all configs manually — expensive and error-prone.
- **Role/toolset info in `get_project_info` (AI-2505)**: AI assistants were not aware of the user's role restrictions until they hit a blocked tool call, leading to confusing error-recovery loops. Surfacing role and toolset restrictions upfront enables better agent behavior from the start.
- **OAuth bearer token for Query Service (AI-2638)**: Storage tokens are less secure and OAuth bearer tokens are the standard authentication method. Switching improves security posture while maintaining full backward compatibility.
- **Read-only access enforcement (AI-2442)**: `readonly`-role users could previously see and invoke write tools, which would fail at the Keboola API level with confusing errors. Enforcing the restriction at the MCP layer gives a clear, early rejection. The 403 swallow prevents analytics-event failures from surfacing as tool errors for restricted roles.
- **`jsonpath_ng` 1.8.0 compatibility (AI-2662)**: The `jsonpath_ng` library released a breaking change in its string representation; without this fix, all config-based searches (introduced in the same release) would return corrupted match scopes.

## Testing

This section is to be filled by the release testers. Leave it as it is and just remove this instruction text.

- [ ] Tested with Cursor AI desktop (all transports)
- [ ] Tested with claude.ai web and canary-orion MCP (SSE and Streamable-HTTP)
- [ ] Tested with In Platform Agent on canary-orion
- [ ] Tested with RO chat on canary-orion

## Deployment Plan

- **Method**: Automated CI/CD pipeline triggered from the release branch
- **Channels**: All MCP instances (both public and agent) will be updated in all Keboola stacks. The version will be published to PyPI and Anthropic's MCP registry as well as released on GitHub

## Rollback Plan

Revert to previous `1.44.7` version if critical issues arise. Standard deployment pipeline can be used for rollback. Estimated rollback time: < 15 minutes.

## Post-Release Support Plan

- Monitor application logs for errors in config-based search, Query Service authentication, and role-based tool access
- Standard on-call coverage
- Timeline for post-release monitoring: 24-48 hours
- Monitor for issues and provide support via GitHub Issues
