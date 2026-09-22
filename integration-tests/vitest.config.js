import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    testTimeout: 60000,
    hookTimeout: 30000,
    globals: true,
    sequence: {
      shuffle: false,
    },
    reporters: ['verbose'],
  },
});
