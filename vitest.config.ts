import { defineConfig } from 'vitest/config';

// Vite 6 is a hard floor, not a preference: earlier versions do not know `node:sqlite` and
// strip the prefix, then fail looking for a package called "sqlite" on disk.
export default defineConfig({
  test: { environment: 'node' },
});
