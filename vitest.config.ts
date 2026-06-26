import path from 'node:path';
import { defineConfig } from 'vitest/config';

const r = (p: string) => path.resolve(import.meta.dirname, p);

export default defineConfig({
  test: {
    globals: true,
    include: ['__tests__/**/*.test.ts'],
    testTimeout: 15000,
  },
  resolve: {
    alias: [{ find: /^@\/(.*)/, replacement: `${r('src')}/$1` }],
  },
});
