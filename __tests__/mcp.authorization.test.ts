import { describe, expect, it } from 'vitest';

import {
  hasAuthorizationFilters,
  isToolNameAuthorized,
  parseAuthorizationConfig,
} from '@/mcp/authorization';

// Port of tests/test_authorization.py. The Python middleware reads HTTP headers;
// here the header values are surfaced onto Config (allowedTools / disallowedTools /
// readOnlyMode), parsed by parseAuthorizationConfig, and applied per tool by
// isToolNameAuthorized.

// 3 read-only (get_configs, get_buckets, query_data), 2 write (create_config, update_descriptions).
const TOOLS: { name: string; readOnly: boolean }[] = [
  { name: 'get_configs', readOnly: true },
  { name: 'create_config', readOnly: false },
  { name: 'get_buckets', readOnly: true },
  { name: 'update_descriptions', readOnly: false },
  { name: 'query_data', readOnly: true },
];
const ALL_TOOLS = new Set(TOOLS.map((t) => t.name));
const READ_ONLY_TOOLS = new Set(TOOLS.filter((t) => t.readOnly).map((t) => t.name));

type Headers = { allowedTools?: string; disallowedTools?: string; readOnlyMode?: string };

const filterList = (headers: Headers): Set<string> => {
  const config = parseAuthorizationConfig(headers);
  if (!hasAuthorizationFilters(config)) return new Set(TOOLS.map((t) => t.name));
  return new Set(
    TOOLS.filter((t) => isToolNameAuthorized(t.name, t.readOnly, config)).map((t) => t.name),
  );
};

describe('parseAuthorizationConfig + isToolNameAuthorized (list filtering)', () => {
  it.each<[string, Headers, Set<string>]>([
    ['no_headers', {}, ALL_TOOLS],
    [
      'allowed_tools_only',
      { allowedTools: 'get_configs, get_buckets' },
      new Set(['get_configs', 'get_buckets']),
    ],
    ['read_only_mode_only', { readOnlyMode: 'true' }, READ_ONLY_TOOLS],
    [
      'disallowed_tools_only',
      { disallowedTools: 'create_config, update_descriptions' },
      READ_ONLY_TOOLS,
    ],
    [
      'allowed_and_read_only',
      { allowedTools: 'get_configs, create_config, get_buckets', readOnlyMode: 'true' },
      new Set(['get_configs', 'get_buckets']),
    ],
    [
      'allowed_and_disallowed',
      { allowedTools: 'get_configs, create_config, get_buckets', disallowedTools: 'create_config' },
      new Set(['get_configs', 'get_buckets']),
    ],
    [
      'read_only_and_disallowed',
      { readOnlyMode: 'true', disallowedTools: 'get_configs' },
      new Set(['get_buckets', 'query_data']),
    ],
    [
      'all_three_headers',
      {
        allowedTools: 'get_configs, get_buckets, query_data, create_config',
        readOnlyMode: 'true',
        disallowedTools: 'query_data',
      },
      new Set(['get_configs', 'get_buckets']),
    ],
    ['empty_allowed_tools', { allowedTools: '' }, ALL_TOOLS],
    ['whitespace_only_allowed_tools', { allowedTools: '  ,  ,  ' }, ALL_TOOLS],
    ['empty_disallowed_tools', { disallowedTools: '' }, ALL_TOOLS],
    [
      'allowed_tools_with_whitespace',
      { allowedTools: '  get_configs  ,  get_buckets  ,  ' },
      new Set(['get_configs', 'get_buckets']),
    ],
    [
      'disallowed_tools_with_whitespace',
      { disallowedTools: '  create_config  ,  update_descriptions  ,  ' },
      READ_ONLY_TOOLS,
    ],
  ])('%s', (_label, headers, expected) => {
    expect(filterList(headers)).toEqual(expected);
  });

  it.each(['true', 'True', 'TRUE', '1', 'yes', 'Yes', 'YES'])(
    'read-only mode enabled for truthy value %j',
    (value) => {
      expect(filterList({ readOnlyMode: value })).toEqual(READ_ONLY_TOOLS);
    },
  );

  it.each(['false', 'False', '0', 'no', '', 'random'])(
    'read-only mode disabled for falsy value %j',
    (value) => {
      expect(filterList({ readOnlyMode: value })).toEqual(ALL_TOOLS);
    },
  );
});

describe('isToolNameAuthorized (call decision)', () => {
  it.each<[string, string, boolean, Headers, boolean]>([
    ['no_headers_write_tool', 'create_config', false, {}, true],
    ['no_headers_read_tool', 'get_configs', true, {}, true],
    [
      'allowed_tool_in_list',
      'get_configs',
      true,
      { allowedTools: 'get_configs, get_buckets' },
      true,
    ],
    [
      'allowed_tool_not_in_list',
      'create_config',
      false,
      { allowedTools: 'get_configs, get_buckets' },
      false,
    ],
    ['read_only_mode_read_tool', 'get_configs', true, { readOnlyMode: 'true' }, true],
    ['read_only_mode_write_tool', 'create_config', false, { readOnlyMode: 'true' }, false],
    [
      'disallowed_tool_in_list',
      'create_config',
      false,
      { disallowedTools: 'create_config, update_descriptions' },
      false,
    ],
    [
      'disallowed_tool_not_in_list',
      'get_configs',
      true,
      { disallowedTools: 'create_config, update_descriptions' },
      true,
    ],
    [
      'allowed_and_read_only_write_tool',
      'create_config',
      false,
      { allowedTools: 'get_configs, create_config', readOnlyMode: 'true' },
      false,
    ],
    [
      'allowed_and_disallowed_same_tool',
      'get_configs',
      true,
      { allowedTools: 'get_configs, get_buckets', disallowedTools: 'get_configs' },
      false,
    ],
  ])('%s', (_label, toolName, readOnly, headers, shouldAllow) => {
    const config = parseAuthorizationConfig(headers);
    expect(isToolNameAuthorized(toolName, readOnly, config)).toBe(shouldAllow);
  });
});
