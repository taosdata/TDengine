import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';

test.describe('Login (unauthenticated)', () => {
  test.use({ storageState: { cookies: [], origins: [] } });

  test('renders login page', async ({ page }) => {
    await page.goto(routes.login, { waitUntil: 'networkidle' });

    await expect(page.locator('.login-content')).toBeVisible();
    await expect(page.locator('.demo-dynamic')).toBeVisible();
    await expect(page.locator('button.signin')).toBeVisible();
    await expect(page.locator('.language')).toBeVisible();
  });

  test('validates required fields', async ({ page }) => {
    await page.goto(routes.login, { waitUntil: 'networkidle' });
    await expect(page.locator('.login-content')).toBeVisible();

    await page.locator('button.signin').click();
    await expect(page.locator('.el-form-item__error').first()).toBeVisible();
  });

  test('logs in with root credentials', async ({ page }) => {
    await page.goto(routes.login, { waitUntil: 'networkidle' });

    const form = page.locator('.demo-dynamic');
    await expect(form).toBeVisible();

    await form.locator('input').first().fill('root');
    await form.locator('input[type="password"]').fill('taosdata');

    await Promise.all([
      page.waitForURL(/\/explorer/, { timeout: 15_000 }),
      page.locator('button.signin').click()
    ]);

    await expect(page.locator('.dbs-tree-header')).toBeVisible({ timeout: 15_000 });
  });
});
