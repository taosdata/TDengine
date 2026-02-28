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

  await Promise.all([
    page.waitForURL(/\/explorer/, { timeout: 60_000 }),
    page.locator('button.signin').click()
  ]);

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
    // ignore
  }

  // If the route guard redirects us to /login, complete the login flow then retry the original URL.
  if (pathname === routes.login || page.url().includes('/login')) {
    await submitLoginForm(page, username, password);

    // Most flows land on /explorer after login; navigate to the originally requested URL.
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await expect(page).not.toHaveURL(/\/login(?:\?.*)?$/, { timeout: 60_000 });
  }
}
