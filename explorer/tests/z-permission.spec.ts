import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';

test.describe('Permission / Session', () => {
  test.describe('unauthenticated route protection', () => {
    test.use({ storageState: { cookies: [], origins: [] } });

    const cases: Array<{ name: string; path: string }> = [
      { name: 'Explorer', path: routes.explorer },
      { name: 'DataIn Task', path: routes.dataInTask },
      { name: 'Management User', path: routes.managementUser }
    ];

    for (const c of cases) {
      test(`redirects ${c.name} (${c.path}) to /login`, async ({ page }) => {
        await page.goto(c.path, { waitUntil: 'domcontentloaded' });
        await expect(page).toHaveURL(/\/login(?:\?.*)?$/, { timeout: 15_000 });
      });
    }
  });

  test('redirects to /login when /me returns 401', async ({ page }) => {
    await page.route('**/me', async route => {
      if (route.request().method() !== 'GET') {
        await route.continue();
        return;
      }

      await route.fulfill({
        status: 401,
        contentType: 'application/json',
        body: JSON.stringify({ code: 401, desc: 'Unauthorized' })
      });
    });

    await page.goto(routes.explorer, { waitUntil: 'domcontentloaded' });

    await expect(page).toHaveURL(/\/login(?:\?.*)?$/, { timeout: 15_000 });
    await expect(page.locator('.login-content')).toBeVisible({ timeout: 15_000 });
  });
});
