import { expect, type Locator, type Page } from 'playwright/test';
import { routes } from './routes';

export async function gotoDataInTask(page: Page) {
  await page.goto(routes.dataInTask, { waitUntil: 'networkidle' });
  await expect(page.locator('.tasks-table')).toBeVisible({ timeout: 15000 });
}

export async function openAddSourceFromList(page: Page) {
  const addBtn = page.locator('.title .action-button').filter({ hasText: 'Add Source' });
  if (await addBtn.count()) {
    await addBtn.first().click();
  } else {
    await page.goto(routes.dataInAdd, { waitUntil: 'networkidle' });
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

  // The operations button is only shown when the table row's `hover` flag is set,
  // which is driven by Element Plus `cell-mouse-enter` events.
  const hoverTarget = row.locator('.name-cell').first();
  if (await hoverTarget.count()) {
    await hoverTarget.hover();
  } else {
    await row.locator('td').nth(1).hover();
  }

  const operationsCell = row.locator('td').last();
  await operationsCell.scrollIntoViewIfNeeded();

  const btn = operationsCell.locator('button').first();
  await expect(btn).toBeVisible({ timeout: 5_000 });

  // el-dropdown is configured with trigger="hover" in non-IDMP mode.
  await btn.hover();
  await expect(page.locator('.el-dropdown-menu:visible')).toBeVisible({ timeout: 5_000 });
}

export async function startTaskFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });

  const startItem = menu.getByRole('menuitem', { name: 'Start' });
  if (await startItem.count()) {
    await startItem.click();

    const confirm = page.locator('.el-message-box__btns .el-button--primary');
    if (await confirm.isVisible().catch(() => false)) {
      await confirm.click();
    }
    return;
  }

  // Some environments may auto-start the task after creation.
  const stopItem = menu.getByRole('menuitem', { name: 'Stop' });
  if (await stopItem.count()) {
    await page.keyboard.press('Escape');
    return;
  }

  throw new Error('Neither Start nor Stop action is available for the selected task row');
}

export async function viewTaskReadonlyFromRow(page: Page, row: Locator) {
  await openRowOperations(page, row);

  const menu = page.locator('.el-dropdown-menu:visible');
  await expect(menu).toBeVisible({ timeout: 5_000 });
  await menu.getByRole('menuitem', { name: 'View' }).click();

  await expect(page).toHaveURL(/\/dataIn\/.+\/edit\?readonly=true/, { timeout: 15_000 });
  await expect(page.locator('.btn-group-task')).toContainText('Modify');
  await expect(page.locator('.btn-group-task')).toContainText('Back');
}
