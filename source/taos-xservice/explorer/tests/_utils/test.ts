import { test as base, expect } from 'playwright/test';

export const test = base.extend({
  page: async ({ page }, use) => {
    await page.addInitScript(() => {
      try {
        window.localStorage.setItem('local_language', 'en');
      } catch {
        // ignore
      }
    });
    await use(page);
  }
});

export { expect };
