import { expect, type Page } from 'playwright/test';
import { routes } from './routes';

export async function login(page: Page, username = 'root', password = 'taosdata') {
  await page.goto(routes.login, { waitUntil: 'networkidle' });
  await expect(page.locator('.login-content')).toBeVisible();

  const form = page.locator('.demo-dynamic');
  await expect(form).toBeVisible();

  const usernameInput = form.locator('input').first();
  const passwordInput = form.locator('input[type="password"]');

  await usernameInput.fill(username);
  await passwordInput.fill(password);

  await Promise.all([
    page.waitForURL(/\/explorer/, { timeout: 15000 }),
    page.locator('button.signin').click()
  ]);

  await expect(page.locator('.dbs-tree-header')).toBeVisible({ timeout: 15000 });
}
