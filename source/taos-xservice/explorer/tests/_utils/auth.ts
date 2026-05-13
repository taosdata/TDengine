import { expect, type Page } from 'playwright/test';
import { routes } from './routes';

type PageDebugState = {
  consoleMessages: string[];
  pageErrors: string[];
  requestFailures: string[];
  responseErrors: string[];
  cleanup: () => void;
};

function pushDebugLine(lines: string[], value: string, limit = 20) {
  if (lines.length < limit) {
    lines.push(value);
  }
}

function attachPageDebug(page: Page): PageDebugState {
  const consoleMessages: string[] = [];
  const pageErrors: string[] = [];
  const requestFailures: string[] = [];
  const responseErrors: string[] = [];

  const onConsole = (msg: { type(): string; text(): string }) => {
    pushDebugLine(consoleMessages, `[${msg.type()}] ${msg.text()}`);
  };
  const onPageError = (error: Error) => {
    pushDebugLine(pageErrors, error.stack || error.message || String(error));
  };
  const onRequestFailed = (request: {
    method(): string;
    url(): string;
    failure(): { errorText?: string } | null;
  }) => {
    pushDebugLine(
      requestFailures,
      `${request.method()} ${request.url()} -> ${request.failure()?.errorText ?? 'unknown'}`
    );
  };
  const onResponse = (response: { status(): number; url(): string }) => {
    if (response.status() >= 400) {
      pushDebugLine(responseErrors, `${response.status()} ${response.url()}`);
    }
  };

  page.on('console', onConsole);
  page.on('pageerror', onPageError);
  page.on('requestfailed', onRequestFailed);
  page.on('response', onResponse);

  return {
    consoleMessages,
    pageErrors,
    requestFailures,
    responseErrors,
    cleanup: () => {
      page.off('console', onConsole);
      page.off('pageerror', onPageError);
      page.off('requestfailed', onRequestFailed);
      page.off('response', onResponse);
    }
  };
}

async function dumpLoginDebug(page: Page, debug: PageDebugState, label: string) {
  const [title, content, scriptSrcs, styleHrefs] = await Promise.all([
    page.title().catch(() => '<unavailable>'),
    page
      .locator('body')
      .innerHTML()
      .catch(async () => page.content().catch(() => '<unavailable>')),
    page
      .locator('script[src]')
      .evaluateAll(nodes => nodes.map(node => node.getAttribute('src') || '<empty>'))
      .catch(() => [] as string[]),
    page
      .locator('link[rel="stylesheet"]')
      .evaluateAll(nodes => nodes.map(node => node.getAttribute('href') || '<empty>'))
      .catch(() => [] as string[])
  ]);

  console.error(`[${label}] page.url(): ${page.url()}`);
  console.error(`[${label}] page.title(): ${title}`);
  console.error(`[${label}] script srcs: ${scriptSrcs.length ? scriptSrcs.join(', ') : '<none>'}`);
  console.error(`[${label}] stylesheet hrefs: ${styleHrefs.length ? styleHrefs.join(', ') : '<none>'}`);
  console.error(
    `[${label}] console messages: ${debug.consoleMessages.length ? debug.consoleMessages.join(' | ') : '<none>'}`
  );
  console.error(`[${label}] page errors: ${debug.pageErrors.length ? debug.pageErrors.join(' | ') : '<none>'}`);
  console.error(
    `[${label}] failed requests: ${debug.requestFailures.length ? debug.requestFailures.join(' | ') : '<none>'}`
  );
  console.error(
    `[${label}] error responses: ${debug.responseErrors.length ? debug.responseErrors.join(' | ') : '<none>'}`
  );
  console.error(`[${label}] body snippet: ${content.slice(0, 2000).replace(/\s+/g, ' ')}`);
}

async function waitForLoginForm(page: Page, debug?: PageDebugState) {
  const loginContent = page.locator('.login-content');
  const form = page.locator('.demo-dynamic');
  let lastError: unknown;

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      await expect(loginContent).toBeVisible({ timeout: 20_000 });
      await expect(form).toBeVisible({ timeout: 5_000 });
      return;
    } catch (error) {
      lastError = error;
      if (debug) {
        await dumpLoginDebug(page, debug, `login debug attempt ${attempt}`);
      }
      if (attempt < 3) {
        await page.reload({ waitUntil: 'domcontentloaded' });
      }
    }
  }

  throw lastError;
}

async function submitLoginForm(page: Page, username: string, password: string, debug?: PageDebugState) {
  await waitForLoginForm(page, debug);

  const form = page.locator('.demo-dynamic');

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
  const debug = attachPageDebug(page);
  try {
    await page.goto(routes.login, { waitUntil: 'networkidle' });
    await submitLoginForm(page, username, password, debug);
  } finally {
    debug.cleanup();
  }
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
  const debug = attachPageDebug(page);

  try {
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
      await submitLoginForm(page, username, password, debug);

      // Most flows land on /explorer after login; navigate to the originally requested URL.
      await page.goto(url, { waitUntil: 'domcontentloaded' });
      await expect(page).not.toHaveURL(/\/login(?:\?.*)?$/, { timeout: 60_000 });
    }
  } finally {
    debug.cleanup();
  }
}
