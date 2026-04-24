import { expect, type Locator, type Page } from 'playwright/test';
import { routes } from './routes';
import { ensureLogin } from './auth';

function sqlEditorContent(page: Page): Locator {
  return page.locator('.sql-code-editor .cm-editor .cm-content');
}

export async function gotoExplorer(page: Page) {
  await ensureLogin(page, routes.explorer);
  await expect(page.locator('.dbs-tree-header')).toBeVisible({ timeout: 60_000 });
}

export async function runSql(page: Page, sql: string) {
  // Assumes already authenticated.
  if (!page.url().includes(routes.explorer)) {
    await gotoExplorer(page);
  }

  const editor = sqlEditorContent(page);
  await expect(editor).toBeVisible({ timeout: 60_000 });
  await editor.click();

  // CodeMirror: edit via keyboard.
  await page.keyboard.press('Control+A');
  await page.keyboard.type(sql);

  const runBtn = page.locator('.sql-btn').getByRole('button', { name: 'Run' });
  await expect(runBtn).toBeEnabled({ timeout: 60_000 });
  await runBtn.click();

  // Run button is disabled while executing.
  await expect(runBtn).toBeEnabled({ timeout: 30000 });
}

export async function runSqlBatch(page: Page, sqls: string[]) {
  for (const sql of sqls) {
    await runSql(page, sql);
  }
}

export async function waitForPositiveCount(
  page: Page,
  sql: string,
  options?: { timeoutMs?: number; pollMs?: number }
) {
  const timeoutMs = options?.timeoutMs ?? 60_000;
  const pollMs = options?.pollMs ?? 2_000;
  const deadline = Date.now() + timeoutMs;
  const resultCell = page.locator('.gird .el-table__body-wrapper .el-table__row td .cell').first();
  let lastError: unknown;
  let lastCount = 0;

  await gotoExplorer(page);

  while (Date.now() < deadline) {
    try {
      await runSql(page, sql);
      await expect(resultCell).toBeVisible({ timeout: 15_000 });

      const countText = await resultCell.textContent();
      const count = Number(countText?.trim());
      if (Number.isFinite(count)) {
        lastCount = count;
      }
      if (count > 0) {
        return count;
      }
    } catch (error) {
      lastError = error;
    }

    await page.waitForTimeout(pollMs);
  }

  if (lastError instanceof Error) {
    throw lastError;
  }

  throw new Error(`expected positive row count for query, last count was ${lastCount}: ${sql}`);
}
