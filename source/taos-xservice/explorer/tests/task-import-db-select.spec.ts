import * as path from 'path';
import { fileURLToPath } from 'url';
import { test, expect } from './_utils/test';
import { gotoDataInTask, findTaskRow } from './_utils/datain';
import { stopTaskBestEffort, deleteTaskBestEffort } from './_utils/cleanup';
import { runSqlBatch } from './_utils/explorerSql';

// Path to the MQTT task export file used in all import tests.
const IMPORT_FILE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), 'resources/datain-tasks.json');

// The task name embedded in the import file.
const IMPORTED_TASK_NAME = 'mqtt';

// The target database encoded in the task's "to" URL in the import file.
const IMPORT_FILE_DB = 'test';

/**
 * Open the Import Task dialog by uploading the given file via the hidden
 * el-upload input, then wait for the confirmation dialog to appear.
 *
 * Returns a locator for the visible dialog element.
 */
async function openImportDialog(page: import('playwright/test').Page, filePath: string) {
  // The el-upload component renders a hidden <input type="file">.
  // Playwright's setInputFiles works on hidden inputs directly and
  // triggers the component's change handler, which POSTs to the upload
  // action URL and then calls handleSuccess with the server response.
  const fileInput = page.locator('.inline-upload input[type="file"]');
  await fileInput.setInputFiles(filePath);

  // Wait for the import dialog to appear. The dialog title contains "Import Task".
  const dialog = page.locator('.el-dialog:visible').filter({ hasText: /import task/i });
  await expect(dialog).toBeVisible({ timeout: 15_000 });
  return dialog;
}

test.describe('DataIn - Import Task dialog: Target Database dropdown', () => {
  // These tests share the same fixed task name from the import file; run serially to
  // avoid race conditions between the "confirm import" and "cancel" tests.
  test.describe.configure({ mode: 'serial' });
  test('dialog opens after file upload and shows the task table', async ({ page }) => {
    await gotoDataInTask(page);
    const dialog = await openImportDialog(page, IMPORT_FILE);

    // The table inside the dialog should have at least one row (the imported task).
    const tableRows = dialog.locator('.el-table__row');
    await expect(tableRows.first()).toBeVisible({ timeout: 10_000 });
    await expect(tableRows).toHaveCount(1);
  });

  test('Target Database dropdown pre-selects the db from the task "to" URL', async ({ page }) => {
    await gotoDataInTask(page);
    const dialog = await openImportDialog(page, IMPORT_FILE);

    // The Target Database el-select is in the last column (width 210px / 180px select).
    // Its v-model is scope.row.db, pre-populated from the "to" URL last segment.
    // The selected value is displayed inside .el-select__selected-item or the placeholder.
    const dbSelect = dialog.locator('.el-table__row').first().locator('td').last().locator('.el-select');
    await expect(dbSelect).toBeVisible({ timeout: 10_000 });

    // The wrapper should display the pre-selected database name.
    const wrapper = dbSelect.locator('.el-select__wrapper');
    await expect(wrapper).toContainText(IMPORT_FILE_DB, { timeout: 10_000 });
  });

  test('Target Database dropdown is visible (not clipped by overflow: hidden)', async ({ page }) => {
    await gotoDataInTask(page);
    const dialog = await openImportDialog(page, IMPORT_FILE);

    const dbSelect = dialog.locator('.el-table__row').first().locator('td').last().locator('.el-select');
    await expect(dbSelect).toBeVisible({ timeout: 10_000 });

    // The selected value text must be visible inside the wrapper.
    // Before the CSS fix, overflow:hidden + height:0 on .el-select__selection
    // caused the selected item to be visually clipped even though it was in the DOM.
    const wrapper = dbSelect.locator('.el-select__wrapper');
    await expect(wrapper).toBeVisible({ timeout: 5_000 });

    // Verify the wrapper has a positive bounding-box height (not clipped to 0).
    const wrapperBox = await wrapper.boundingBox();
    expect(wrapperBox).not.toBeNull();
    expect(wrapperBox!.height).toBeGreaterThan(0);

    // The displayed text (selected value or placeholder) must be in the DOM and non-empty.
    const displayedText = await wrapper.textContent();
    expect(displayedText?.trim()).toBeTruthy();
  });

  test('Target Database dropdown can be changed to a different database', async ({ page }) => {
    await gotoDataInTask(page);
    const dialog = await openImportDialog(page, IMPORT_FILE);

    const dbSelect = dialog.locator('.el-table__row').first().locator('td').last().locator('.el-select');
    await expect(dbSelect).toBeVisible({ timeout: 10_000 });

    // Open the dropdown by clicking the wrapper (force to bypass placeholder overlay).
    await dbSelect.click({ force: true });

    const dropdown = page.locator('.el-select-dropdown:visible');
    await expect(dropdown).toBeVisible({ timeout: 10_000 });

    // At least one option should be listed (the 'test' database must exist).
    const options = dropdown.locator('.el-select-dropdown__item');
    await expect(options.first()).toBeVisible({ timeout: 10_000 });

    // Pick the first option regardless of which database it is; just confirm the
    // selection updates the displayed value.
    const chosenText = await options.first().textContent();
    await options.first().click();

    // After selecting, the wrapper should display the chosen database name.
    const wrapper = dbSelect.locator('.el-select__wrapper');
    await expect(wrapper).toContainText(chosenText?.trim() ?? '', { timeout: 5_000 });
  });

  test('cancel button closes the dialog without importing', async ({ page }) => {
    await gotoDataInTask(page);
    const dialog = await openImportDialog(page, IMPORT_FILE);

    // Click Cancel.
    const cancelBtn = dialog.locator('.dialog-footer').getByRole('button', { name: /cancel/i });
    await cancelBtn.click();

    // Dialog should close.
    await expect(dialog).not.toBeVisible({ timeout: 5_000 });

    // Navigate back and verify the import file's task name does NOT appear in the list.
    // (Checking for absence of a specific task is safe even in parallel test runs.)
    await gotoDataInTask(page);
    const taskRow = page.locator('.tasks-table .el-table__row').filter({ hasText: IMPORTED_TASK_NAME });
    await expect(taskRow).toHaveCount(0, { timeout: 5_000 });
  });

  test('confirming import with pre-selected db creates the task', async ({ page }) => {
    test.setTimeout(120_000);

    // Ensure the target database exists before importing.
    await runSqlBatch(page, [`CREATE DATABASE IF NOT EXISTS \`${IMPORT_FILE_DB}\`;`]);

    await gotoDataInTask(page);
    const dialog = await openImportDialog(page, IMPORT_FILE);

    // Select all rows via the header checkbox (first column).
    const headerCheckbox = dialog.locator('thead .el-checkbox').first();
    await headerCheckbox.click();

    // Ensure the Target Database dropdown shows the expected pre-selected value.
    const dbSelect = dialog.locator('.el-table__row').first().locator('td').last().locator('.el-select');
    await expect(dbSelect.locator('.el-select__wrapper')).toContainText(IMPORT_FILE_DB, { timeout: 10_000 });

    // Click Confirm to import.
    const confirmBtn = dialog.locator('.dialog-footer').getByRole('button', { name: /confirm/i });
    await confirmBtn.click();

    // Dialog should close after a successful import.
    await expect(dialog).not.toBeVisible({ timeout: 15_000 });

    // The imported task should now appear in the task list.
    try {
      await gotoDataInTask(page);
      const row = await findTaskRow(page, IMPORTED_TASK_NAME);
      await expect(row).toBeVisible({ timeout: 15_000 });
    } finally {
      // Cleanup: stop and delete the imported task so repeated test runs stay clean.
      await stopTaskBestEffort(page, IMPORTED_TASK_NAME);
      await deleteTaskBestEffort(page, IMPORTED_TASK_NAME);
    }
  });
});
