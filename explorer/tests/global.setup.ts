import { mkdirSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium, type FullConfig } from 'playwright/test';
import { login } from './_utils/auth';

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const AUTH_DIR = path.join(THIS_DIR, '.auth');
const AUTH_FILE = path.join(AUTH_DIR, 'root.json');

export default async function globalSetup(config: FullConfig) {
  // Allow skipping regeneration for local debugging.
  if (existsSync(AUTH_FILE) && process.env.PLAYWRIGHT_SKIP_GLOBAL_SETUP === 'true') {
    return;
  }

  mkdirSync(AUTH_DIR, { recursive: true });

  const baseURL = config.projects[0]?.use?.baseURL as string | undefined;

  const browser = await chromium.launch();
  const context = await browser.newContext({
    baseURL,
    // Make sure English is set in storageState.
    storageState: { cookies: [], origins: [] }
  });

  await context.addInitScript(() => {
    try {
      window.localStorage.setItem('local_language', 'en');
    } catch {
      // ignore
    }
  });

  const page = await context.newPage();
  await login(page, 'root', 'taosdata');

  await context.storageState({ path: AUTH_FILE });
  await browser.close();
}

export const storageStatePath = 'tests/.auth/root.json';
