import type { Page } from 'playwright/test';
import { expect } from './test';
import { runSqlBatch } from './explorerSql';
import { findTaskRow, gotoDataInTask, openRowOperations } from './datain';

function shouldSkipCleanup() {
  return process.env.PLAYWRIGHT_SKIP_CLEANUP === 'true';
}

export function uniqueE2eName(prefix: string) {
  // Keep it URL/SQL safe-ish and sortable.
  return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
}

async function clickConfirmIfAny(page: Page) {
  const confirm = page.locator('.el-message-box__btns .el-button--primary');
  if (await confirm.isVisible().catch(() => false)) {
    await confirm.click();
  }
}

export async function stopTaskBestEffort(page: Page, taskName: string) {
  if (shouldSkipCleanup()) return;
  try {
    await gotoDataInTask(page);
    const row = await findTaskRow(page, taskName);

    await openRowOperations(page, row);
    const menu = page.locator('.el-dropdown-menu:visible');
    await expect(menu).toBeVisible({ timeout: 5_000 });

    const stopItem = menu
      .locator('li')
      .filter({ hasText: /Stop\b/i })
      .first();
    if (!(await stopItem.count())) {
      // Not stoppable (already stopped / not running) or menu differs.
      await page.keyboard.press('Escape').catch(() => {});
      return;
    }

    await stopItem.click();
    await clickConfirmIfAny(page);
  } catch {
    // best-effort
  }
}

export async function deleteTaskBestEffort(page: Page, taskName: string) {
  if (shouldSkipCleanup()) return;
  try {
    await gotoDataInTask(page);
    const row = await findTaskRow(page, taskName);

    await openRowOperations(page, row);
    const menu = page.locator('.el-dropdown-menu:visible');
    await expect(menu).toBeVisible({ timeout: 5_000 });

    const stopItem = menu
      .locator('li')
      .filter({ hasText: /Stop\b/i })
      .first();
    if (await stopItem.count()) {
      await stopItem.click();
      await clickConfirmIfAny(page);
      await page.waitForTimeout(3000);
    }
    const deleteItem = menu
      .locator('li')
      .filter({ hasText: /Delete\b/i })
      .first();
    if (!(await deleteItem.count())) {
      await page.keyboard.press('Escape').catch(() => {});
      return;
    }

    await deleteItem.click();
    await clickConfirmIfAny(page);

    // Wait for the row to disappear (if it was present).
    await expect(row)
      .toHaveCount(0, { timeout: 30_000 })
      .catch(() => {});
  } catch {
    // best-effort
  }
}

export async function cleanupTmqResourcesBestEffort(
  page: Page,
  opts: {
    taskName?: string;
    topics?: string[];
    databases?: string[];
  }
) {
  if (shouldSkipCleanup()) return;
  try {
    // Cleanup order matters: stop/delete tasks first to avoid resource-in-use errors.
    if (opts.taskName) {
      await stopTaskBestEffort(page, opts.taskName);
      await deleteTaskBestEffort(page, opts.taskName);
    }

    const sqls: string[] = [];

    for (const topic of opts.topics ?? []) {
      sqls.push(`DROP TOPIC IF EXISTS ${topic};`);
    }

    for (const db of opts.databases ?? []) {
      sqls.push(`DROP DATABASE IF EXISTS ${db};`);
    }

    if (sqls.length) {
      await runSqlBatch(page, sqls);
    }
  } catch {
    // best-effort
  }
}
