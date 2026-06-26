import { defineConfig } from 'oxlint';

import keboolaShared from '@keboola/oxlint-config';

export default defineConfig({
  extends: [keboolaShared],
  env: {
    node: true,
  },
  ignorePatterns: ['dist/', 'src/keboola_mcp_server/'],
});
