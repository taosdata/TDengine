import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium, type FullConfig } from 'playwright/test';
import { login } from './_utils/auth';

const THIS_DIR = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = path.resolve(THIS_DIR, '..');

const AUTH_DIR = path.join(PROJECT_ROOT, '.playwright', '.auth');
const AUTH_FILE = path.join(AUTH_DIR, 'root.json');

export default async function globalSetup(config: FullConfig) {
  // Always regenerate to avoid stale sessions.
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
