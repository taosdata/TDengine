import { expect, type Locator, type Page } from 'playwright/test';
import { routes } from './routes';

function sqlEditorContent(page: Page): Locator {
  return page.locator('.sql-code-editor .cm-editor .cm-content');
}

export async function gotoExplorer(page: Page) {
  await page.goto(routes.explorer, { waitUntil: 'networkidle' });
  await expect(page.locator('.dbs-tree-header')).toBeVisible({ timeout: 15000 });
}

export async function runSql(page: Page, sql: string) {
  // Assumes already authenticated.
  if (!page.url().includes(routes.explorer)) {
    await gotoExplorer(page);
  }

  const editor = sqlEditorContent(page);
  await expect(editor).toBeVisible({ timeout: 15000 });
  await editor.click();

  // CodeMirror: edit via keyboard.
  await page.keyboard.press('Control+A');
  await page.keyboard.type(sql);

  const runBtn = page.locator('.sql-btn').getByRole('button', { name: 'Run' });
  await expect(runBtn).toBeEnabled({ timeout: 15000 });
  await runBtn.click();

  // Run button is disabled while executing.
  await expect(runBtn).toBeEnabled({ timeout: 30000 });
}

export async function runSqlBatch(page: Page, sqls: string[]) {
  for (const sql of sqls) {
    await runSql(page, sql);
  }
}
