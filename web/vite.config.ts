import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
  build: { outDir: 'dist', emptyOutDir: true },
  server: {
    // `npm run dev:web` serves the UI with HMR and forwards data calls to the API process.
    proxy: Object.fromEntries(
      ['/api', '/ingest', '/metrics', '/health'].map((path) => [
        path,
        { target: 'http://127.0.0.1:8080', changeOrigin: true },
      ]),
    ),
  },
});
