/**
 * Regenerate the README screenshots from a live dashboard.
 *
 * The images in docs/assets are build artifacts, not hand-captured pictures, so they cannot
 * quietly drift once the UI changes. Both colour schemes are captured because dark mode here
 * is a selected set of steps rather than an inverted light mode, and only a render proves it.
 *
 *   npm start &                       # or: BUS=kafka STORE=postgres npm start
 *   npm run traffic -- --backfill 22
 *   npm run capture:demo
 */

import { mkdirSync } from 'node:fs';
import { chromium } from 'playwright';

const BASE_URL = process.env.CAPTURE_URL ?? 'http://127.0.0.1:8080';
const OUT_DIR = 'docs/assets';

async function main(): Promise<void> {
  mkdirSync(OUT_DIR, { recursive: true });
  const browser = await chromium.launch({ channel: 'chrome' });

  for (const scheme of ['light', 'dark'] as const) {
    const context = await browser.newContext({
      viewport: { width: 1180, height: 820 },
      deviceScaleFactor: 2,
      colorScheme: scheme,
    });
    const page = await context.newPage();
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });
    // The dashboard is SSE-driven and renders empty until the first tick arrives.
    await page.waitForFunction(
      () => (document.querySelector('.tile-value')?.textContent ?? '0') !== '0',
      undefined,
      { timeout: 15_000 },
    );
    await page.waitForTimeout(600);
    await page.locator('.page').screenshot({ path: `${OUT_DIR}/dashboard-${scheme}.png` });
    console.log(`wrote ${OUT_DIR}/dashboard-${scheme}.png`);
    await context.close();
  }

  await browser.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
