import { expect, type Page } from 'playwright/test';
import { routes } from './routes';

async function submitLoginForm(page: Page, username: string, password: string) {
  await expect(page.locator('.login-content')).toBeVisible({ timeout: 60_000 });

  const form = page.locator('.demo-dynamic');
  await expect(form).toBeVisible({ timeout: 60_000 });

  const usernameInput = form.locator('input').first();
  const passwordInput = form.locator('input[type="password"]');

  await usernameInput.fill(username);
  await passwordInput.fill(password);

  // Wait a bit for any form validation
  await page.waitForTimeout(500);

  // Try pressing Enter key to submit form
  await passwordInput.press('Enter');

  // Wait for navigation
  await page.waitForURL(/\/explorer/, { timeout: 60_000 });

  // Explorer home should be visible after successful login.
  await expect(page.locator('.dbs-tree-header')).toBeVisible({ timeout: 60_000 });
}

export async function login(page: Page, username = 'root', password = 'taosdata') {
  await page.goto(routes.login, { waitUntil: 'networkidle' });
  await submitLoginForm(page, username, password);
}

export async function ensureLogin(
  page: Page,
  url: string,
  opts: {
    username?: string;
    password?: string;
  } = {}
) {
  const username = opts.username ?? 'root';
  const password = opts.password ?? 'taosdata';

  await page.goto(url, { waitUntil: 'domcontentloaded' });

  let pathname: string | null = null;
  try {
    pathname = new URL(page.url()).pathname;
  } catch {
    // Keep fallback behavior when URL is temporarily unparsable.
    pathname = null;
  }

  // If the route guard redirects us to /login, complete the login flow then retry the original URL.
  if (pathname === routes.login || page.url().includes('/login')) {
    await submitLoginForm(page, username, password);

    // Most flows land on /explorer after login; navigate to the originally requested URL.
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await expect(page).not.toHaveURL(/\/login(?:\?.*)?$/, { timeout: 60_000 });
  }
}
