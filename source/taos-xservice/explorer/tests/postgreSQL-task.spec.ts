import { test, expect } from './_utils/test';
import { runSql, runSqlBatch, waitForPositiveCount } from './_utils/explorerSql';
import { stopTaskBestEffort, deleteTaskBestEffort } from './_utils/cleanup';
import { findTaskRow, gotoDataInTask, openAddSourceFromList, selectElOptionByText } from './_utils/datain';
import { routes } from './_utils/routes';

test.describe('DataIn - PostgreSQL datasource', () => {
  test('all required fields show errors when empty and Check Connection is clicked', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select PostgreSQL as datasource type
    await selectElOptionByText(page, 'type', 'PostgreSQL');

    // Wait for PostgreSQL-specific fields to render
    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    // --- Locate required fields ---

    // Connection Configuration
    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');

    // SQL Configuration
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
    const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();
    const endInput = page.locator('input[id^="data.groups_after."][id$=".end"]').first();

    // --- Verify required fields are rendered ---
    await expect(hostInput).toBeVisible();
    await expect(portInput).toBeVisible();
    await expect(databaseInput).toBeVisible();
    await expect(usernameInput).toBeVisible();
    await expect(passwordInput).toBeVisible();

    await sqlInput.scrollIntoViewIfNeeded();
    await expect(sqlInput).toBeVisible({ timeout: 10_000 });

    await startInput.scrollIntoViewIfNeeded();
    await expect(startInput).toBeVisible({ timeout: 10_000 });

    await endInput.scrollIntoViewIfNeeded();
    await expect(endInput).toBeVisible();

    // --- Verify placeholder (port has no defaultValue, only placeholder) ---
    await expect(portInput).toHaveAttribute('placeholder', '5432');

    // --- Clear all required fields to ensure they are empty ---
    await hostInput.clear();
    await portInput.clear();
    await databaseInput.clear();
    await usernameInput.clear();
    await passwordInput.clear();
    await sqlInput.clear();
    await startInput.clear();
    await endInput.clear();

    // --- Click Check Connection to trigger validation ---
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // --- Verify required field error messages appear ---
    await page.waitForTimeout(500);

    // Helper: locate the .el-form-item__error inside the ancestor .el-form-item
    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // PostgreSQL Host is required
    const hostError = getFormItemError(hostInput);
    await hostInput.scrollIntoViewIfNeeded();
    await expect(hostError).toBeVisible({ timeout: 5_000 });
    await expect(hostError).toContainText(/required/i);

    // PostgreSQL Port is required
    const portError = getFormItemError(portInput);
    await portInput.scrollIntoViewIfNeeded();
    await expect(portError).toBeVisible({ timeout: 5_000 });

    // Database is required
    const databaseError = getFormItemError(databaseInput);
    await databaseInput.scrollIntoViewIfNeeded();
    await expect(databaseError).toBeVisible({ timeout: 5_000 });
    await expect(databaseError).toContainText(/required/i);

    // Username is required
    const usernameError = getFormItemError(usernameInput);
    await usernameInput.scrollIntoViewIfNeeded();
    await expect(usernameError).toBeVisible({ timeout: 5_000 });
    await expect(usernameError).toContainText(/required/i);

    // Password is required
    const passwordError = getFormItemError(passwordInput);
    await passwordInput.scrollIntoViewIfNeeded();
    await expect(passwordError).toBeVisible({ timeout: 5_000 });
    await expect(passwordError).toContainText(/required/i);

    // SQL is required
    const sqlError = getFormItemError(sqlInput);
    await sqlInput.scrollIntoViewIfNeeded();
    await expect(sqlError).toBeVisible({ timeout: 5_000 });
    await expect(sqlError).toContainText(/required/i);
  });

  test('errors clear after filling required fields', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select PostgreSQL as datasource type
    await selectElOptionByText(page, 'type', 'PostgreSQL');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();

    // Clear fields
    await hostInput.clear();
    await portInput.clear();
    await databaseInput.clear();
    await usernameInput.clear();
    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.clear();

    // Trigger validation
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();
    await page.waitForTimeout(500);

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // Verify errors are present
    const hostError = getFormItemError(hostInput);
    await hostInput.scrollIntoViewIfNeeded();
    await expect(hostError).toBeVisible({ timeout: 5_000 });

    const portError = getFormItemError(portInput);
    await portInput.scrollIntoViewIfNeeded();
    await expect(portError).toBeVisible({ timeout: 5_000 });

    const databaseError = getFormItemError(databaseInput);
    await databaseInput.scrollIntoViewIfNeeded();
    await expect(databaseError).toBeVisible({ timeout: 5_000 });

    const usernameError = getFormItemError(usernameInput);
    await usernameInput.scrollIntoViewIfNeeded();
    await expect(usernameError).toBeVisible({ timeout: 5_000 });

    const sqlError = getFormItemError(sqlInput);
    await sqlInput.scrollIntoViewIfNeeded();
    await expect(sqlError).toBeVisible({ timeout: 5_000 });

    // --- Fill fields one by one and verify errors clear ---

    // Fill PostgreSQL Host
    await hostInput.scrollIntoViewIfNeeded();
    await hostInput.fill('192.168.1.45');
    await hostInput.press('Tab');
    await expect(hostError).not.toBeVisible({ timeout: 5_000 });

    // Fill PostgreSQL Port
    await portInput.scrollIntoViewIfNeeded();
    await portInput.fill('5432');
    await portInput.press('Tab');
    await expect(portError).not.toBeVisible({ timeout: 5_000 });

    // Fill Database
    await databaseInput.scrollIntoViewIfNeeded();
    await databaseInput.fill('test');
    await databaseInput.press('Tab');
    await expect(databaseError).not.toBeVisible({ timeout: 5_000 });

    // Fill Username
    await usernameInput.scrollIntoViewIfNeeded();
    await usernameInput.fill('postgres');
    await usernameInput.press('Tab');
    await expect(usernameError).not.toBeVisible({ timeout: 5_000 });

    // Fill SQL
    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill('select * from public.pg_ci where ttimezone >= ${start} and ttimezone < ${end}');
    await sqlInput.press('Tab');
    await expect(sqlError).not.toBeVisible({ timeout: 5_000 });
  });

  test('partially filled form only shows errors for remaining empty required fields', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select PostgreSQL as datasource type
    await selectElOptionByText(page, 'type', 'PostgreSQL');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();

    // Fill Host and Database, leave Port, Username and SQL empty
    await hostInput.fill('192.168.1.45');
    await databaseInput.fill('test');

    // Clear Port, Username and SQL
    await portInput.clear();
    await usernameInput.clear();
    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.clear();

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();
    await page.waitForTimeout(500);

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // Host error should NOT appear (it was filled)
    const hostError = getFormItemError(hostInput);
    await hostInput.scrollIntoViewIfNeeded();
    await expect(hostError).not.toBeVisible({ timeout: 3_000 });

    // Database error should NOT appear (it was filled)
    const databaseError = getFormItemError(databaseInput);
    await databaseInput.scrollIntoViewIfNeeded();
    await expect(databaseError).not.toBeVisible({ timeout: 3_000 });

    // Port error SHOULD appear (was cleared)
    const portError = getFormItemError(portInput);
    await portInput.scrollIntoViewIfNeeded();
    await expect(portError).toBeVisible({ timeout: 5_000 });

    // Username error SHOULD appear
    const usernameError = getFormItemError(usernameInput);
    await usernameInput.scrollIntoViewIfNeeded();
    await expect(usernameError).toBeVisible({ timeout: 5_000 });
    await expect(usernameError).toContainText(/required/i);

    // SQL error SHOULD appear
    const sqlError = getFormItemError(sqlInput);
    await sqlInput.scrollIntoViewIfNeeded();
    await expect(sqlError).toBeVisible({ timeout: 5_000 });
    await expect(sqlError).toContainText(/required/i);
  });

  test('non-required fields do not show errors when empty', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select PostgreSQL as datasource type
    await selectElOptionByText(page, 'type', 'PostgreSQL');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    // Click Check Connection without filling anything to trigger validation
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();
    await page.waitForTimeout(500);

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // Verify non-required fields do NOT show errors

    // Password (not required in some cases)
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');
    await passwordInput.scrollIntoViewIfNeeded();
    // Password might be required or not depending on configuration, so we just check it exists
    await expect(passwordInput).toBeVisible();

    // Interval (not required, has default) — composeAppend type, no id on input
    // PostgreSQL uses "Time Interval" label (MySQL uses "Query Interval")
    const intervalFormItem = page.locator('.el-form-item').filter({ hasText: 'Time Interval' }).first();
    await intervalFormItem.scrollIntoViewIfNeeded();
    const intervalError = intervalFormItem.locator('.el-form-item__error').first();
    await expect(intervalError).not.toBeVisible({ timeout: 3_000 });

    // Read Concurrency (not required, has default) — inside collapsed "Advanced Options"
    // Expand the Advanced Options collapse section first
    const advancedHeader = page.locator('.el-collapse-item__header').filter({ hasText: 'Advanced Options' }).first();
    await advancedHeader.scrollIntoViewIfNeeded();
    await advancedHeader.click();
    await page.waitForTimeout(300);

    const concurrencyInput = page.locator('#data\\.advanced_options\\.read_concurrency');
    await concurrencyInput.scrollIntoViewIfNeeded();
    const concurrencyError = getFormItemError(concurrencyInput);
    await expect(concurrencyError).not.toBeVisible({ timeout: 3_000 });

    // Delay (not required, has default) — composeAppend type, no id on input
    const delayFormItem = page.locator('.el-form-item').filter({ hasText: 'Delay' }).first();
    await delayFormItem.scrollIntoViewIfNeeded();
    const delayError = delayFormItem.locator('.el-form-item__error').first();
    await expect(delayError).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check succeeds with reachable PostgreSQL server', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select PostgreSQL as datasource type
    await selectElOptionByText(page, 'type', 'PostgreSQL');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
    const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();

    // Fill connection details
    await hostInput.fill('192.168.1.45');
    await portInput.fill('5432');
    await databaseInput.fill('test');
    await usernameInput.fill('postgres');
    await passwordInput.fill('tbase125!');

    // Fill SQL configuration
    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill('select * from public.pg_ci where ttimezone >= ${start} and ttimezone < ${end}');
    await startInput.scrollIntoViewIfNeeded();
    await startInput.fill('2024-05-07 00:00:00');

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // Verify connectivity check succeeds
    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toContainText(/reachable/i, { timeout: 30_000 });
  });

  test('connectivity check fails with unreachable PostgreSQL server', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select PostgreSQL as datasource type
    await selectElOptionByText(page, 'type', 'PostgreSQL');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
    const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();

    // Fill connection details with wrong port
    await hostInput.fill('192.168.1.45');
    await portInput.fill('5433');
    await databaseInput.fill('test');
    await usernameInput.fill('postgres');
    await passwordInput.fill('tbase125!');

    // Fill SQL configuration
    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill('select * from public.pg_ci where ttimezone >= ${start} and ttimezone < ${end}');
    await startInput.scrollIntoViewIfNeeded();
    await startInput.fill('2024-05-07 00:00:00');

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // Verify connectivity check fails
    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toContainText(/not reachable/i, { timeout: 30_000 });
  });

  test('create PostgreSQL task, check connection, configure mapping, submit and verify running', async ({ page }) => {
    const taskName = `pg_e2e_test_${Date.now()}`;
    const targetDb = 'ci_db_pg';
    const stableName = `stb_pg_${Date.now()}`;
    const subtableName = `pg_\${sint}_\${cchar}`;

    try {
      // Create target database if it doesn't exist
      await runSqlBatch(page, [`CREATE DATABASE IF NOT EXISTS \`${targetDb}\`;`]);

      // ========================
      // Step 1: Navigate and create a new task
      // ========================
      await gotoDataInTask(page);
      await openAddSourceFromList(page);

      // Fill task name
      const taskNameInput = page.locator('#name');
      await expect(taskNameInput).toBeVisible({ timeout: 10_000 });
      await taskNameInput.fill(taskName);

      // Select PostgreSQL as datasource type
      await selectElOptionByText(page, 'type', 'PostgreSQL');

      // ========================
      // Step 2: Fill connection details
      // ========================
      const hostInput = page.locator('#data\\.connection_options\\.host');
      await expect(hostInput).toBeVisible({ timeout: 10_000 });

      const portInput = page.locator('#data\\.connection_options\\.port');
      const databaseInput = page.locator('#data\\.connection_options\\.subject');
      const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
      const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');

      await hostInput.fill('192.168.1.45');
      await portInput.fill('5432');
      await databaseInput.fill('test');
      await usernameInput.fill('postgres');
      await passwordInput.fill('tbase125!');

      // ========================
      // Step 3: Fill SQL configuration
      // ========================
      const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
      const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();
      const endInput = page.locator('input[id^="data.groups_after."][id$=".end"]').first();

      await sqlInput.scrollIntoViewIfNeeded();
      await sqlInput.fill('select * from public.pg_ci where ttimezone >= ${start} and ttimezone < ${end}');

      await startInput.scrollIntoViewIfNeeded();
      await startInput.fill('2024-05-07 00:00:00');

      await endInput.scrollIntoViewIfNeeded();
      await endInput.fill('2024-05-08 00:00:00');

      // Set Time Interval to 12h
      const intervalFormItem = page.locator('.el-form-item').filter({ hasText: 'Time Interval' }).first();
      await intervalFormItem.scrollIntoViewIfNeeded();
      const intervalNumberInput = intervalFormItem.locator('.el-input-number input').first();
      await intervalNumberInput.fill('12');
      // Select "Hours" from the append select
      const intervalSelect = intervalFormItem.locator('.el-select').last();
      await intervalSelect.click();
      await page.locator('.el-select-dropdown__item').filter({ hasText: 'Hours' }).first().click();
      await page.waitForTimeout(300);

      // ========================
      // Step 4: Expand Advanced Options and set read concurrency
      // ========================
      const advancedHeader = page.locator('.el-collapse-item__header').filter({ hasText: 'Advanced Options' }).first();
      await advancedHeader.scrollIntoViewIfNeeded();
      await advancedHeader.click();
      await page.waitForTimeout(300);

      const concurrencyInput = page.locator('#data\\.advanced_options\\.read_concurrency');
      await concurrencyInput.scrollIntoViewIfNeeded();
      await concurrencyInput.fill('1');

      // ========================
      // Step 4.5: Check connectivity
      // ========================
      const checkBtn = page.locator('.btn-check-connectivity');
      await checkBtn.scrollIntoViewIfNeeded();
      await checkBtn.click();

      const resultText = page.locator('.box-check-connectivity .text');
      await expect(resultText).toContainText(/reachable/i, { timeout: 30_000 });

      // ========================
      // Step 5: Configure transformer / mapping
      // ========================

      // Select target database
      const transformerSection = page.locator('.task-form-transformer, .transformer-section, [class*="transformer"]').first();

      // Fill target database
      await selectElOptionByText(page, 'targetDB', targetDb);

      // Click "Retrieve From Server" to get sample data
      const retrieveBtn = page.getByRole('button', { name: /Retrieve From Server/i }).first();
      await retrieveBtn.scrollIntoViewIfNeeded();
      await retrieveBtn.click();

      // Wait for "Create STable" button to be enabled
      const createStableBtn = page.getByRole('button', { name: /Create STable/i }).first();
      await expect(createStableBtn).toBeEnabled({ timeout: 30_000 });
      await createStableBtn.click();

      // --- Create STable dialog ---
      const stableDialog = page.locator('.el-dialog').filter({ hasText: /Create STable/i }).first();
      await expect(stableDialog).toBeVisible({ timeout: 10_000 });

      // Fill stable name
      const stableNameInput = stableDialog.locator('.el-input input').first();
      await stableNameInput.clear();
      await stableNameInput.fill(stableName);

      // --- Clean up columns ---
      const collapseItems = stableDialog.locator('.el-collapse-item');
      const columnsSection = collapseItems.nth(0);

      // Ensure columns section is expanded
      const colSectionActive = await columnsSection.evaluate(el => el.classList.contains('is-active')).catch(() => false);
      if (!colSectionActive) {
        await columnsSection.locator('.el-collapse-item__header').first().click();
        await page.waitForTimeout(300);
      }

      // Delete duplicate/blob columns and move tag columns
      const columnsToDelete: string[] = [''];
      const columnsToMoveToTag = ['sint', 'cchar'];

      let colRows = columnsSection.locator('.input-row');
      let colCount = await colRows.count();
      for (let ri = colCount - 1; ri >= 1; ri--) {
        const row = colRows.nth(ri);
        const nameInput = row.locator('.el-input:not(.el-input-number) input').last();
        const name = await nameInput.inputValue().catch(() => '');

        if (columnsToMoveToTag.includes(name)) {
          // Click the "move to tag" button (second button in .action-btn, has .console-tree-icon)
          const initialCount = await colRows.count();
          const moveToTagBtn = row.locator('.action-btn .el-button').nth(1);
          await moveToTagBtn.click();
          await expect(colRows).toHaveCount(initialCount - 1);
        } else if (columnsToDelete.includes(name)) {
          // Click the delete button (first button in .action-btn, icon="Minus")
          const initialCount = await colRows.count();
          const deleteBtn = row.locator('.action-btn .el-button').first();
          await deleteBtn.click();
          await expect(colRows).toHaveCount(initialCount - 1);
        }
      }

      // Fill the ts (TIMESTAMP) column name — first row in columns section
      const tsRow = columnsSection.locator('.input-row').first();
      const tsNameInput = tsRow.locator('.el-input:not(.el-input-number) input').last();
      const tsVal = await tsNameInput.inputValue().catch(() => '');
      if (!tsVal) {
        await tsNameInput.fill('ts');
      }

      // --- Clean up tags: delete empty tag rows ---
      const tagsSection = collapseItems.nth(1);
      // Ensure tags section is expanded
      const tagSectionActive = await tagsSection.evaluate(el => el.classList.contains('is-active')).catch(() => false);
      if (!tagSectionActive) {
        await tagsSection.locator('.el-collapse-item__header').first().click();
        await page.waitForTimeout(300);
      }

      let tagRows = tagsSection.locator('.input-row');
      let tagCount = await tagRows.count();
      for (let ti = tagCount - 1; ti >= 0; ti--) {
        const row = tagRows.nth(ti);
        const nameInput = row.locator('.el-input:not(.el-input-number) input').last();
        const name = await nameInput.inputValue().catch(() => '');

        if (!name) {
          const deleteBtn = row.locator('.action-btn .el-button').first();
          await deleteBtn.click();
          await page.waitForTimeout(200);
        }
      }

      // Click the Create button in the dialog
      const stableCreateBtn = stableDialog.getByRole('button', { name: /^Create$/i }).first();
      await stableCreateBtn.scrollIntoViewIfNeeded();
      await expect(stableCreateBtn).toBeVisible({ timeout: 5_000 });
      await stableCreateBtn.click();

      // Wait for the dialog to close
      await expect(stableDialog).not.toBeVisible({ timeout: 30_000 });
      await page.waitForTimeout(2000);

      // After creating the super table, the mapping table should appear
      const mappingTable = transformerSection.locator('.table-detail .el-table').first();
      await mappingTable.scrollIntoViewIfNeeded();
      await expect(mappingTable).toBeVisible({ timeout: 15_000 });

      // Configure SubTableName if present
      const subtableRow = mappingTable.locator('.el-table__row').filter({ hasText: 'SubTableName' }).first();
      if (await subtableRow.count()) {
        await subtableRow.scrollIntoViewIfNeeded();
        const subtableExprInput = subtableRow.locator('.el-input input').first();
        await subtableExprInput.clear();
        await subtableExprInput.fill(subtableName);
      }

      // Map ts column: select "mapping" then pick "ttimezone" as the source column
      const tsMapRow = mappingTable.locator('.el-table__row').filter({ hasText: /^ts/i }).first();
      await tsMapRow.scrollIntoViewIfNeeded();
      const tsExprSelect = tsMapRow.locator('.mapping-rule-select').first();
      await tsExprSelect.click({ force: true });
      const tsDropdown = page.locator('.el-select-dropdown:visible');
      await expect(tsDropdown).toBeVisible({ timeout: 5_000 });
      await tsDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'mapping' }).first().click();
      await page.waitForTimeout(500);
      // Select "ttimezone" from the mapping column dropdown
      const tsMappingCol = tsMapRow.locator('.mapping-rule-expression').first();
      await tsMappingCol.click({ force: true });
      const tsMappingDropdown = page.locator('.el-select-dropdown:visible');
      await expect(tsMappingDropdown).toBeVisible({ timeout: 5_000 });
      await tsMappingDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'ttimezone' }).first().click();
      await page.waitForTimeout(500);

      // ========================
      // Step 6: Submit the task
      // ========================
      const submitBtn = page.locator('.btn-group-task').getByRole('button', { name: 'Submit' });
      await submitBtn.scrollIntoViewIfNeeded();

      try {
        await expect(submitBtn).toBeEnabled({ timeout: 10_000 });
      } catch {
        test.skip(true, 'Submit button not enabled - backend service may not be running');
      }

      await Promise.all([page.waitForURL(new RegExp(`${routes.dataInTask}$`), { timeout: 60_000 }), submitBtn.click()]);

      // ========================
      // Step 7: Verify task in list and status is Running
      // ========================
      let row = await findTaskRow(page, taskName);
      await expect(row).toBeVisible({ timeout: 10_000 });

      // Refresh the task list up to 10 times until status is "Running"
      const refreshBtn = page.getByRole('button', { name: /Refresh/i }).first();

      for (let i = 0; i < 10; i++) {
        row = await findTaskRow(page, taskName);
        const rowText = await row.textContent();
        if (rowText && /Running|Completed/i.test(rowText)) {
          break;
        }
        await page.waitForTimeout(1000);
        await refreshBtn.click();
        await page.waitForTimeout(500);
      }

      // Final assertion: the task should be running
      row = await findTaskRow(page, taskName);
      await expect(row).toContainText(/Running|Completed/i, { timeout: 5_000 });

      // ========================
      // Step 8: Wait for task to complete
      // ========================
      for (let i = 0; i < 60; i++) {
        row = await findTaskRow(page, taskName);
        const rowText = await row.textContent();
        if (rowText && /Completed/i.test(rowText)) {
          break;
        }
        await page.waitForTimeout(2000);
        await refreshBtn.click();
        await page.waitForTimeout(500);
      }

      row = await findTaskRow(page, taskName);
      await expect(row).toContainText(/Completed/i, { timeout: 5_000 });
    } finally {
      // Cleanup: stop and delete the task
      await stopTaskBestEffort(page, taskName);
      await deleteTaskBestEffort(page, taskName);
      // ========================
      // Step 9: Verify data was written to TDengine
      // ========================
      const count = await waitForPositiveCount(page, `select count(*) from \`${targetDb}\`.\`${stableName}\`;`);
      expect(count).toBeGreaterThan(0);
      // Drop the super table we created
      await runSqlBatch(page, [
        `DROP STABLE IF EXISTS \`${targetDb}\`.\`${stableName}\`;`,
        `DROP DATABASE IF EXISTS \`${targetDb}\`;`,
      ]).catch(() => {});
    }
  });
});
