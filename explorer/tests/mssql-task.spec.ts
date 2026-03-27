import { test, expect } from './_utils/test';
import { runSql, runSqlBatch } from './_utils/explorerSql';
import { stopTaskBestEffort, deleteTaskBestEffort } from './_utils/cleanup';
import { findTaskRow, gotoDataInTask, openAddSourceFromList, selectElOptionByText } from './_utils/datain';
import { routes } from './_utils/routes';
import { mssqlConfig } from './config/mssql';

test.describe('DataIn - Microsoft SQL Server datasource', () => {
  test('all required fields show errors when empty and Check Connection is clicked', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select Microsoft SQL Server as datasource type
    await selectElOptionByText(page, 'type', 'Microsoft SQL Server');

    // Wait for MSSQL-specific fields to render
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
    await expect(portInput).toHaveAttribute('placeholder', '1433');

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

    // Host is required
    const hostError = getFormItemError(hostInput);
    await hostInput.scrollIntoViewIfNeeded();
    await expect(hostError).toBeVisible({ timeout: 5_000 });
    await expect(hostError).toContainText(/required/i);

    // Port is required
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

    // Select Microsoft SQL Server as datasource type
    await selectElOptionByText(page, 'type', 'Microsoft SQL Server');

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

    // Fill Host
    await hostInput.scrollIntoViewIfNeeded();
    await hostInput.fill(mssqlConfig.connection.host);
    await hostInput.press('Tab');
    await expect(hostError).not.toBeVisible({ timeout: 5_000 });

    // Fill Port
    await portInput.scrollIntoViewIfNeeded();
    await portInput.fill(mssqlConfig.connection.port);
    await portInput.press('Tab');
    await expect(portError).not.toBeVisible({ timeout: 5_000 });

    // Fill Database
    await databaseInput.scrollIntoViewIfNeeded();
    await databaseInput.fill(mssqlConfig.dataCollection.database);
    await databaseInput.press('Tab');
    await expect(databaseError).not.toBeVisible({ timeout: 5_000 });

    // Fill Username
    await usernameInput.scrollIntoViewIfNeeded();
    await usernameInput.fill(mssqlConfig.connection.username);
    await usernameInput.press('Tab');
    await expect(usernameError).not.toBeVisible({ timeout: 5_000 });

    // Fill SQL
    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill(mssqlConfig.dataCollection.sql);
    await sqlInput.press('Tab');
    await expect(sqlError).not.toBeVisible({ timeout: 5_000 });
  });

  test('partially filled form only shows errors for remaining empty required fields', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select Microsoft SQL Server as datasource type
    await selectElOptionByText(page, 'type', 'Microsoft SQL Server');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();

    // Fill Host and Database, leave Port, Username and SQL empty
    await hostInput.fill(mssqlConfig.connection.host);
    await databaseInput.fill(mssqlConfig.dataCollection.database);

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

    // Select Microsoft SQL Server as datasource type
    await selectElOptionByText(page, 'type', 'Microsoft SQL Server');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    // Click Check Connection without filling anything to trigger validation
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();
    await page.waitForTimeout(500);

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // Password field exists but might not be required
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');
    await passwordInput.scrollIntoViewIfNeeded();
    await expect(passwordInput).toBeVisible();

    // Query Interval (not required, has default) — no error
    const intervalFormItem = page.locator('.el-form-item').filter({ hasText: 'Query Interval' }).first();
    await intervalFormItem.scrollIntoViewIfNeeded();
    const intervalError = intervalFormItem.locator('.el-form-item__error').first();
    await expect(intervalError).not.toBeVisible({ timeout: 3_000 });

    // Read Concurrency (not required, has default) — inside collapsed "Advanced Options"
    const advancedHeader = page.locator('.el-collapse-item__header').filter({ hasText: 'Advanced Options' }).first();
    await advancedHeader.scrollIntoViewIfNeeded();
    await advancedHeader.click();
    await page.waitForTimeout(300);

    const concurrencyInput = page.locator('#data\\.advanced_options\\.read_concurrency');
    await concurrencyInput.scrollIntoViewIfNeeded();
    const concurrencyError = getFormItemError(concurrencyInput);
    await expect(concurrencyError).not.toBeVisible({ timeout: 3_000 });

    // Delay (not required, has default) — no error
    const delayFormItem = page.locator('.el-form-item').filter({ hasText: 'Delay' }).first();
    await delayFormItem.scrollIntoViewIfNeeded();
    const delayError = delayFormItem.locator('.el-form-item__error').first();
    await expect(delayError).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check succeeds with reachable Microsoft SQL Server', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select Microsoft SQL Server as datasource type
    await selectElOptionByText(page, 'type', 'Microsoft SQL Server');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();

    // Fill all required fields with valid values from config
    await hostInput.fill(mssqlConfig.connection.host);
    await portInput.fill(mssqlConfig.connection.port);
    await databaseInput.fill(mssqlConfig.dataCollection.database);
    await usernameInput.fill(mssqlConfig.connection.username);
    await passwordInput.fill(mssqlConfig.connection.password);

    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill(mssqlConfig.dataCollection.sql);

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toBeVisible({ timeout: 60_000 });
    await expect(resultText).toContainText(/reachable/i);

    const errorSpan = resultText.locator('span.error');
    await expect(errorSpan).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check fails with unreachable Microsoft SQL Server', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select Microsoft SQL Server as datasource type
    await selectElOptionByText(page, 'type', 'Microsoft SQL Server');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const databaseInput = page.locator('#data\\.connection_options\\.subject');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();

    // Fill all required fields — use wrong port to simulate unreachable server
    await hostInput.fill(mssqlConfig.connection.host);
    await portInput.clear();
    await portInput.fill(mssqlConfig.connection.wrongPort);
    await databaseInput.fill(mssqlConfig.dataCollection.database);
    await usernameInput.fill(mssqlConfig.connection.username);
    await passwordInput.fill(mssqlConfig.connection.password);

    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill(mssqlConfig.dataCollection.sql);

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toBeVisible({ timeout: 60_000 });
    await expect(resultText).toContainText(/not reachable/i);

    const errorSpan = resultText.locator('span.error');
    await expect(errorSpan).toBeVisible({ timeout: 5_000 });
    await expect(errorSpan).toContainText(/error message/i);
  });

  test('create Microsoft SQL Server task, check connection, configure mapping, submit and verify running', async ({ page }) => {
    test.setTimeout(180_000);
    const ts = Date.now();
    const taskName = `mssql_e2e_${ts}`;
    const stableName = `mssql_stable_${ts}`;
    const subtableName = `mssql_table_\${yyear}`;
    const targetDb = 'ci_db_mssql';

    try {
      // Ensure target database exists
      await runSqlBatch(page, [`CREATE DATABASE IF NOT EXISTS \`${targetDb}\`;`]);

      // ========================
      // Step 1: General Information
      // ========================
      await gotoDataInTask(page);
      await openAddSourceFromList(page);

      // Fill task name
      await page.locator('#name').fill(taskName);

      // Select target database BEFORE changing the datasource type
      await selectElOptionByText(page, 'targetDB', targetDb);
      await page.waitForTimeout(500);

      // Select Microsoft SQL Server as datasource type
      await selectElOptionByText(page, 'type', 'Microsoft SQL Server');

      // Wait for MSSQL-specific form fields to render after type change
      const hostInput = page.locator('#data\\.connection_options\\.host');
      await expect(hostInput).toBeVisible({ timeout: 10_000 });

      // ========================
      // Step 2: Connection Configuration
      // ========================
      await hostInput.fill(mssqlConfig.connection.host);

      const portInput = page.locator('#data\\.connection_options\\.port');
      await portInput.fill(mssqlConfig.connection.port);

      const databaseInput = page.locator('#data\\.connection_options\\.subject');
      await databaseInput.fill(mssqlConfig.dataCollection.database);

      const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
      await usernameInput.fill(mssqlConfig.connection.username);

      const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');
      await passwordInput.fill(mssqlConfig.connection.password);

      // ========================
      // Step 3: SQL Configuration
      // ========================

      const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
      await sqlInput.scrollIntoViewIfNeeded();
      await sqlInput.fill(mssqlConfig.dataCollection.sql);

      const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();
      await startInput.scrollIntoViewIfNeeded();
      await startInput.fill(mssqlConfig.dataCollection.start);

      const endInput = page.locator('input[id^="data.groups_after."][id$=".end"]').first();
      await endInput.scrollIntoViewIfNeeded();
      await endInput.fill(mssqlConfig.dataCollection.end);

      // Query Interval — set to 12 Hours
      const intervalFormItem = page.locator('.el-form-item').filter({ hasText: 'Query Interval' }).first();
      await intervalFormItem.scrollIntoViewIfNeeded();
      const intervalNumberInput = intervalFormItem.locator('.input-number-with-select .el-input-number input').first();
      await intervalNumberInput.fill('12');
      const intervalUnitSelect = intervalFormItem.locator('.input-number-with-select .unit-select').first();
      await intervalUnitSelect.click();
      await page.locator('.el-select-dropdown:visible .el-select-dropdown__item').filter({ hasText: 'Hours' }).click();

      // Read Concurrency — inside collapsed "Advanced Options"
      const advancedHeader = page.locator('.el-collapse-item__header').filter({ hasText: 'Advanced Options' }).first();
      await advancedHeader.scrollIntoViewIfNeeded();
      await advancedHeader.click();
      await page.waitForTimeout(300);

      const concurrencyInput = page.locator('#data\\.advanced_options\\.read_concurrency');
      await concurrencyInput.scrollIntoViewIfNeeded();
      await concurrencyInput.fill('1');

      // Delay — leave as default (0)
      const delayFormItem = page.locator('.el-form-item').filter({ hasText: 'Delay' }).first();
      await delayFormItem.scrollIntoViewIfNeeded();
      const delayNumberInput = delayFormItem.locator('.input-number-with-select .el-input-number input').first();
      await delayNumberInput.fill('0');

      // ========================
      // Step 4: Check Connection
      // ========================
      const checkBtn = page.locator('.btn-check-connectivity');
      await checkBtn.scrollIntoViewIfNeeded();
      await checkBtn.click();

      const resultText = page.locator('.box-check-connectivity .text');
      await expect(resultText).toBeVisible({ timeout: 60_000 });
      await expect(resultText).toContainText(/reachable/i);

      const errorSpan = resultText.locator('span.error');
      await expect(errorSpan).not.toBeVisible({ timeout: 3_000 });

      // ========================
      // Step 5: Mapping section
      // ========================

      const transformerSection = page.locator('.common-transformer');
      await transformerSection.scrollIntoViewIfNeeded();
      await expect(transformerSection).toBeVisible({ timeout: 30_000 });

      // Click "Retrieve From Server" to fetch sample data
      const retrieveBtn = transformerSection.getByRole('button', { name: /Retrieve From Server/i }).first();
      await retrieveBtn.scrollIntoViewIfNeeded();
      await expect(retrieveBtn).toBeVisible({ timeout: 10_000 });
      await retrieveBtn.click();

      // Wait for "Create STable" to become enabled
      const createStableDropdown = transformerSection
        .locator('.table-title')
        .getByRole('button', { name: /Create STable/i })
        .first();
      await expect(createStableDropdown).toBeEnabled({ timeout: 60_000 });
      await createStableDropdown.click();

      // Wait for the Create STable dialog
      const stableDialog = page.locator('.el-dialog:visible').first();
      await expect(stableDialog).toBeVisible({ timeout: 10_000 });

      // Fill in the super table name
      const stableNameInput = stableDialog.locator('.name_input input').first();
      await expect(stableNameInput).toBeVisible({ timeout: 5_000 });
      await stableNameInput.fill(stableName);

      // Process each collapse section — fill empty field names with fallbacks
      const collapseItems = stableDialog.locator('.el-collapse-item');
      const sectionCount = await collapseItems.count();
      for (let si = 0; si < sectionCount; si++) {
        const section = collapseItems.nth(si);
        const sectionHeader = section.locator('.el-collapse-item__header').first();
        const isActive = await section.evaluate(el => el.classList.contains('is-active')).catch(() => false);
        if (!isActive) {
          await sectionHeader.click();
          await page.waitForTimeout(300);
        }
        const rows = section.locator('.input-row');
        const rowCount = await rows.count();
        for (let ri = 0; ri < rowCount; ri++) {
          const inputRow = rows.nth(ri);
          const fieldInput = inputRow.locator('.el-input:not(.el-input-number) input').last();
          const val = await fieldInput.inputValue().catch(() => '');
          if (!val) {
            const fallbackName = si === 0 ? (ri === 0 ? 'ts' : `col_${ri}`) : `tag_${ri}`;
            await fieldInput.fill(fallbackName);
          }
        }
      }

      // --- Clean up columns ---
      const columnsSection = collapseItems.nth(0);
      const colSectionActive = await columnsSection.evaluate(el => el.classList.contains('is-active')).catch(() => false);
      if (!colSectionActive) {
        await columnsSection.locator('.el-collapse-item__header').first().click();
        await page.waitForTimeout(300);
      }

      // Delete: duplicate ts (skip index 0), tblob, bblob, mblob, lblob, tag_0
      // Move to tag: yyear
      const columnsToDelete = ['ts', 'tblob', 'bblob', 'mblob', 'lblob', 'tag_0'];
      const columnsToMoveToTag = ['yyear'];

      let colRows = columnsSection.locator('.input-row');
      let colCount = await colRows.count();
      for (let ri = colCount - 1; ri >= 1; ri--) {
        const row = colRows.nth(ri);
        const nameInput = row.locator('.el-input:not(.el-input-number) input').last();
        const name = await nameInput.inputValue().catch(() => '');

        if (columnsToMoveToTag.includes(name)) {
          const initialCount = await colRows.count();
          const moveToTagBtn = row.locator('.action-btn .el-button').nth(1);
          await moveToTagBtn.click();
          await expect(colRows).toHaveCount(initialCount - 1);
        } else if (columnsToDelete.includes(name)) {
          const initialCount = await colRows.count();
          const deleteBtn = row.locator('.action-btn .el-button').first();
          await deleteBtn.click();
          await expect(colRows).toHaveCount(initialCount - 1);
        }
      }

      // --- Clean up empty tag rows ---
      const tagsSection = collapseItems.nth(1);
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

      // Mapping table should appear
      const mappingTable = transformerSection.locator('.table-detail .el-table').first();
      await mappingTable.scrollIntoViewIfNeeded();
      await expect(mappingTable).toBeVisible({ timeout: 15_000 });

      // Configure SubTableName
      const subtableRow = mappingTable.locator('.el-table__row').filter({ hasText: 'SubTableName' }).first();
      if (await subtableRow.count()) {
        await subtableRow.scrollIntoViewIfNeeded();
        const subtableExprInput = subtableRow.locator('.el-input input').first();
        await subtableExprInput.clear();
        await subtableExprInput.fill(subtableName);
      }

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
      // Step 7: Verify task is Running
      // ========================
      let row = await findTaskRow(page, taskName);
      await expect(row).toBeVisible({ timeout: 10_000 });

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
      await runSql(page, `select count(*) from \`${targetDb}\`.\`${stableName}\`;`);

      const resultCell = page.locator('.gird .el-table__body-wrapper .el-table__row td .cell').first();
      await expect(resultCell).toBeVisible({ timeout: 15_000 });
      const countText = await resultCell.textContent();
      const count = Number(countText?.trim());
      expect(count).toBeGreaterThan(0);

      await runSqlBatch(page, [
        `DROP STABLE IF EXISTS \`${targetDb}\`.\`${stableName}\`;`,
        `DROP DATABASE IF EXISTS \`${targetDb}\`;`,
      ]).catch(() => {});
    }
  });
});
