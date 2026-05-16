import { expect, type Locator, type Page } from 'playwright/test';
import { routes } from './routes';
import { ensureLogin } from './auth';

async function clickConfirmIfAny(page: Page) {
  const confirm = page.locator('.el-message-box__btns .el-button--primary');
  if (await confirm.isVisible().catch(() => false)) {
    await confirm.click();
  }
}
export async function gotoDataInTask(page: Page) {
  await ensureLogin(page, routes.dataInTask);
  await expect(page.locator('.tasks-table')).toBeVisible({ timeout: 15000 });
}

export async function openAddSourceFromList(page: Page) {
  const addBtn = page.locator('.title .action-button').filter({ hasText: 'Add Source' });
  if (await addBtn.count()) {
    await addBtn.first().click();
  } else {
    await ensureLogin(page, routes.dataInAdd);
  }
  await expect(page.locator('#name')).toBeVisible({ timeout: 15000 });
}

export async function selectElOptionByText(page: Page, selectId: string, optionText: string) {
  const root = page.locator(`#${selectId}`);
  await root.scrollIntoViewIfNeeded();
  await expect(root).toBeVisible();

  // Element Plus may place the id on the internal readonly <input> (role=combobox),
  // but the visible placeholder can intercept pointer events. Force click to open.
  await root.click({ force: true });

  const dropdown = page.locator('.el-select-dropdown:visible');
  await expect(dropdown).toBeVisible({ timeout: 15_000 });

  const option = dropdown.locator('.el-select-dropdown__item').filter({ hasText: optionText }).first();
  await expect(option).toBeVisible({ timeout: 15_000 });
  await option.click();
}

export async function findTaskRow(page: Page, taskName: string): Promise<Locator> {
  const byRole = page.locator('.tasks-table').getByRole('row', { name: new RegExp(taskName) });
  if (await byRole.count()) {
    return byRole.first();
  }

  const byText = page.locator('.tasks-table .el-table__row').filter({ hasText: taskName });
  await expect(byText.first()).toBeVisible({ timeout: 15000 });
  return byText.first();
}

export async function openRowOperations(page: Page, row: Locator) {
  await row.scrollIntoViewIfNeeded();

  // Close any previously opened dropdown to avoid interacting with a stale menu.
  await page.keyboard.press('Escape').catch(() => {});

  // The operations button is only shown when the table row's `hover` flag is set,
  // which is driven by Element Plus `cell-mouse-enter` events.
  const hoverTarget = row.locator('.name-cell').first();
  if (await hoverTarget.count()) {
    await hoverTarget.hover({ force: true });
  } else {
    await row.hover({ force: true });
  }

  const menu = page.locator('.el-dropdown-menu:visible');

  const openFrom = async (rowForOps: Locator) => {
    const dropdown = rowForOps.locator('.el-dropdown').first();

    const idmpTrigger = dropdown.locator('span.cursor-pointer').first();
    const buttonTrigger = dropdown.locator('button').first();

    // In non-IDMP mode dropdown trigger is hover; in IDMP it's click.
    if (await buttonTrigger.count()) {
      await expect(buttonTrigger).toBeVisible({ timeout: 5_000 });

      await buttonTrigger.hover({ force: true });
      if (!(await menu.isVisible().catch(() => false))) {
        // Fallback: some environments may require click.
        await buttonTrigger.click({ force: true });
      }
    } else {
      await expect(idmpTrigger).toBeVisible({ timeout: 5_000 });
      await idmpTrigger.click({ force: true });
    }

    await expect(menu).toBeVisible({ timeout: 5_000 });
  };

  // First try to open from the provided row.
  try {
    await openFrom(row);
    return;
  } catch {
    // Element Plus fixed-right columns can render operations in a separate fixed table.
    // In that case, resolve the row index in the main body table and open the dropdown
    // from the corresponding fixed-right row.
  }

  const rowIndex = await row.evaluate(el => {
    const tr = el.closest('tr');
    if (!tr) return -1;

    const rows = Array.from(document.querySelectorAll('.tasks-table .el-table__body-wrapper .el-table__row'));
    return rows.indexOf(tr);
  });

  if (rowIndex < 0) {
    throw new Error('Failed to resolve DataIn table row index for operations dropdown');
  }

  const fixedRow = page.locator('.tasks-table .el-table__fixed-right .el-table__row').nth(rowIndex);
  await expect(fixedRow).toBeVisible({ timeout: 5_000 });
  await openFrom(fixedRow);
}

export async function startTaskFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });

  // Element Plus dropdown items are rendered as <li>. Don't rely on ARIA roles.
  const startItem = menu
    .locator('li')
    .filter({ hasText: /Start\b/i })
    .first();
  if (await startItem.count()) {
    await startItem.click();

    const confirm = page.locator('.el-message-box__btns .el-button--primary');
    if (await confirm.isVisible().catch(() => false)) {
      await confirm.click();
    }
    return;
  }

  // Some environments may auto-start the task after creation.
  const stopItem = menu
    .locator('li')
    .filter({ hasText: /Stop\b/i })
    .first();
  if (await stopItem.count()) {
    await page.keyboard.press('Escape');
    return;
  }

  throw new Error('Neither Start nor Stop action is available for the selected task row');
}

export async function stopTaskFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });
  // Some environments may auto-start the task after creation.
  const stopItem = menu
    .locator('li')
    .filter({ hasText: /Stop\b/i })
    .first();
  if (await stopItem.count()) {
    await stopItem.click();
    await clickConfirmIfAny(page);
    return;
  }
  await page.keyboard.press('Escape');
}

export async function viewTaskReadonlyFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });
  await menu
    .locator('li')
    .filter({ hasText: /View\b/i })
    .first()
    .click();

  await expect(page).toHaveURL(/\/dataIn\/.+\/edit\?readonly=true/, { timeout: 15_000 });
  await expect(page.locator('.btn-group-task')).toContainText('Modify');
  await expect(page.locator('.btn-group-task')).toContainText('Back');
}

export async function editTaskFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });
  await menu
    .locator('li')
    .filter({ hasText: /Edit\b/i })
    .first()
    .click();

  await expect(page).toHaveURL(/\/dataIn\/.+\/edit/, { timeout: 15_000 });
  await expect(page.locator('#name')).toBeVisible({ timeout: 5_000 });
}

export async function deleteTaskFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });
  await menu
    .locator('li')
    .filter({ hasText: /Delete\b/i })
    .first()
    .click();

  // Confirmation dialog will appear - caller should handle it
}

export async function copyTaskFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });
  await menu
    .locator('li')
    .filter({ hasText: /Copy\b/i })
    .first()
    .click();

  await expect(page).toHaveURL(/\/dataIn\/add/, { timeout: 15_000 });
  await expect(page.locator('#name')).toBeVisible({ timeout: 5_000 });
}
