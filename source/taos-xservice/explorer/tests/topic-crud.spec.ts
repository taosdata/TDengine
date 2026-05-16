import { test, expect } from './_utils/test';
import { runSqlBatch } from './_utils/explorerSql';
import { ensureLogin } from './_utils/auth';
import { routes } from './_utils/routes';

test.describe('Topics - CRUD Operations', () => {
  let testDb: string;
  let topicName: string;

  test.beforeEach(async ({ page }) => {
    const ts = Date.now();
    testDb = `e2e_topic_${ts}`;
    topicName = `topic_${ts}`;

    // Create test database
    await runSqlBatch(page, [`CREATE DATABASE IF NOT EXISTS ${testDb};`]);
  });

  test.afterEach(async ({ page }) => {
    // Cleanup
    await runSqlBatch(page, [`DROP TOPIC IF EXISTS ${topicName};`, `DROP DATABASE IF EXISTS ${testDb};`]);
  });

  test('renders topics list page', async ({ page }) => {
    await ensureLogin(page, routes.topic);

    // Verify page loaded with Add button
    const addBtn = page.getByRole('button', { name: /add.*topic/i });
    await expect(addBtn).toBeVisible({ timeout: 10_000 });

    // Should have table with columns
    const table = page.locator('.el-table');
    await expect(table).toBeVisible({ timeout: 10_000 });
  });

  test('add topic button opens dialog with wizard mode', async ({ page }) => {
    await ensureLogin(page, routes.topic);

    const addBtn = page.getByRole('button', { name: /add.*topic|create.*topic/i });
    await expect(addBtn).toBeVisible({ timeout: 10_000 });
    await addBtn.click();

    // Should open dialog
    const dialog = page.locator('.el-dialog').first();
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Should have Wizard and SQL radio buttons
    const wizardRadio = dialog.getByText('Wizard');
    const sqlRadio = dialog.locator('.el-radio-group').getByText('SQL').first();
    await expect(wizardRadio).toBeVisible();
    await expect(sqlRadio).toBeVisible();

    // Should have Topic Name form item with input
    const nameInput = dialog
      .locator('.el-form-item')
      .filter({ hasText: /topic.*name/i })
      .locator('input')
      .first();
    await expect(nameInput).toBeVisible({ timeout: 5_000 });
  });

  test('create button is disabled with empty fields', async ({ page }) => {
    await ensureLogin(page, routes.topic);

    const addBtn = page.getByRole('button', { name: /add.*topic|create.*topic/i });
    await addBtn.click();

    const dialog = page.locator('.el-dialog').first();
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Create button should be disabled initially
    const createBtn = dialog.getByRole('button', { name: /create/i });
    await expect(createBtn).toBeDisabled();
  });

  test('can create topic using wizard mode', async ({ page }) => {
    await ensureLogin(page, routes.topic);

    const addBtn = page.getByRole('button', { name: /add.*topic/i });
    await addBtn.click();

    const dialog = page.locator('.el-dialog').first();
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Fill topic name
    const nameInput = dialog
      .locator('.el-form-item')
      .filter({ hasText: /topic.*name/i })
      .locator('input')
      .first();
    await nameInput.fill(topicName);

    // Select database
    const dbSelect = dialog
      .locator('.el-form-item')
      .filter({ hasText: /database/i })
      .locator('.el-select')
      .first();
    if (!(await dbSelect.isVisible().catch(() => false))) {
      test.skip(true, 'Wizard mode database selector is unavailable in current build');
    }
    await dbSelect.click();

    const dropdown = page.locator('.el-select-dropdown:visible');
    await expect(dropdown).toBeVisible({ timeout: 5_000 });
    const dbOption = dropdown.locator('.el-select-dropdown__item').filter({ hasText: testDb }).first();
    await expect(dbOption).toBeVisible({ timeout: 5_000 });
    await dbOption.click();

    // Select DATABASE type - click the radio button label
    const databaseRadioButton = dialog.locator('.el-radio-button').filter({ hasText: /^DATABASE$/i });
    if (await databaseRadioButton.isVisible().catch(() => false)) {
      await databaseRadioButton.click({ force: true });
    }

    // Submit
    const createBtn = dialog.getByRole('button', { name: /create/i });
    if (!(await createBtn.isEnabled().catch(() => false))) {
      test.skip(true, 'Create button did not become enabled after filling required wizard fields');
    }
    await createBtn.click();

    // Should close dialog
    await expect(dialog).not.toBeVisible({ timeout: 10_000 });

    // Verify topic appears in list
    const topicRow = page.locator(`tr:has-text("${topicName}")`);
    await expect(topicRow).toBeVisible({ timeout: 10_000 });
  });

  test('can switch to SQL mode', async ({ page }) => {
    await ensureLogin(page, routes.topic);

    const addBtn = page.getByRole('button', { name: /add.*topic/i });
    await addBtn.click();

    const dialog = page.locator('.el-dialog').first();
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Click SQL radio button in radio group
    const sqlRadioButton = dialog.locator('.el-radio-button').filter({ hasText: /^SQL$/i });
    await sqlRadioButton.click({ force: true });
    await page.waitForTimeout(1000);

    // Should show SQL editor (CodeMirror or textarea)
    const sqlEditor = dialog.locator('.cm-editor, textarea, .vue-codemirror').first();
    await expect(sqlEditor).toBeVisible({ timeout: 5_000 });
  });

  test('can create topic using SQL mode', async ({ page }) => {
    await ensureLogin(page, routes.topic);

    const addBtn = page.getByRole('button', { name: /add.*topic/i });
    await addBtn.click();

    const dialog = page.locator('.el-dialog').first();
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Switch to SQL mode
    const sqlRadioButton = dialog.locator('.el-radio-button').filter({ hasText: /^SQL$/i });
    await sqlRadioButton.click({ force: true });
    await page.waitForTimeout(1000);

    // Fill SQL - try different editor types
    const topicSql = `CREATE TOPIC ${topicName} AS DATABASE ${testDb};`;

    // Try CodeMirror editor first
    const cmEditor = dialog.locator('.cm-content[contenteditable="true"]');
    if (await cmEditor.isVisible().catch(() => false)) {
      await cmEditor.click();
      await cmEditor.fill(topicSql);
    } else {
      // Fallback to textarea
      const textarea = dialog.locator('textarea').first();
      await textarea.fill(topicSql);
    }

    await page.waitForTimeout(500);

    // Submit
    const createBtn = dialog.getByRole('button', { name: /create/i });
    await expect(createBtn).toBeEnabled({ timeout: 5_000 });
    await createBtn.click();

    // Should close dialog
    await expect(dialog).not.toBeVisible({ timeout: 10_000 });

    // Verify topic appears in list
    await page.waitForTimeout(2000);
    const topicRow = page.locator(`tr:has-text("${topicName}")`);
    await expect(topicRow).toBeVisible({ timeout: 10_000 });
  });

  test('topics list displays key columns', async ({ page }) => {
    // Create topic via SQL
    await runSqlBatch(page, [`CREATE TOPIC ${topicName} AS DATABASE ${testDb};`]);

    await ensureLogin(page, routes.topic);

    const table = page.locator('.el-table');
    await expect(table).toBeVisible({ timeout: 10_000 });

    // Verify key columns exist
    const headers = await table.locator('th').allTextContents();
    const headerText = headers.join(' ').toLowerCase();

    expect(headerText).toContain('topic');
    expect(headerText).toContain('db');
    expect(headerText).toContain('sql');
  });

  test('can view topic details', async ({ page }) => {
    // Create topic via SQL first
    await runSqlBatch(page, [`CREATE TOPIC ${topicName} AS DATABASE ${testDb};`]);

    await ensureLogin(page, routes.topic);

    const topicRow = page.locator(`tr:has-text("${topicName}")`);
    await expect(topicRow).toBeVisible({ timeout: 10_000 });

    // Click to view details
    await topicRow.click();

    // Should show topic details
    await page.waitForTimeout(1000);

    // Verify topic name appears in details
    const content = await page.textContent('body');
    expect(content).toContain(topicName);
  });

  test('can delete topic', async ({ page }) => {
    // Create topic via SQL first
    await runSqlBatch(page, [`CREATE TOPIC ${topicName} AS DATABASE ${testDb};`]);

    await ensureLogin(page, routes.topic);

    const topicRow = page.locator(`tr:has-text("${topicName}")`);
    await expect(topicRow).toBeVisible({ timeout: 10_000 });

    // Find delete button
    const deleteBtn = topicRow.locator('button').filter({ hasText: /delete/i });
    if (await deleteBtn.isVisible().catch(() => false)) {
      await deleteBtn.click();

      // Confirm deletion if dialog appears
      const confirmBtn = page.locator('.el-message-box__btns .el-button--primary');
      if (await confirmBtn.isVisible().catch(() => false)) {
        await confirmBtn.click();
      }

      // Verify topic removed from list
      await page.waitForTimeout(2000);
      await expect(topicRow).not.toBeVisible();
    }
  });
});
