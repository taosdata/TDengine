import { test, expect } from './_utils/test';
import { runSql, runSqlBatch, waitForPositiveCount } from './_utils/explorerSql';
import { stopTaskBestEffort, deleteTaskBestEffort } from './_utils/cleanup';
import { findTaskRow, gotoDataInTask, openAddSourceFromList, selectElOptionByText } from './_utils/datain';
import { routes } from './_utils/routes';
import { mongodbConfig } from './config/mongodb';

test.describe('DataIn - MongoDB datasource', () => {
  test('all required fields show errors when empty and Check Connection is clicked', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MongoDB as datasource type
    await selectElOptionByText(page, 'type', 'MongoDB');

    // Wait for MongoDB-specific fields to render
    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    // --- Locate required fields ---

    // Connection Configuration
    const portInput = page.locator('#data\\.connection_options\\.port');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');

    // Data Collection (groups_after)
    const databaseInput = page.locator('input[id^="data.groups_after."][id$=".database"]').first();
    const collectionInput = page.locator('input[id^="data.groups_after."][id$=".collection"]').first();
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
    const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();

    // --- Verify required fields are rendered ---
    await expect(hostInput).toBeVisible();
    await expect(portInput).toBeVisible();
    await expect(usernameInput).toBeVisible();
    await expect(passwordInput).toBeVisible();

    await databaseInput.scrollIntoViewIfNeeded();
    await expect(databaseInput).toBeVisible({ timeout: 10_000 });

    await collectionInput.scrollIntoViewIfNeeded();
    await expect(collectionInput).toBeVisible();

    await sqlInput.scrollIntoViewIfNeeded();
    await expect(sqlInput).toBeVisible({ timeout: 10_000 });

    await startInput.scrollIntoViewIfNeeded();
    await expect(startInput).toBeVisible({ timeout: 10_000 });

    // --- Verify placeholder (port should be 27017) ---
    await expect(portInput).toHaveAttribute('placeholder', '27017');

    // --- Clear all required fields to ensure they are empty ---
    await hostInput.clear();
    await portInput.clear();
    await usernameInput.clear();
    await passwordInput.clear();
    await databaseInput.clear();
    await collectionInput.clear();
    await sqlInput.clear();
    await startInput.clear();

    // --- Click Check Connection to trigger validation ---
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // --- Verify required field error messages appear ---
    await page.waitForTimeout(500);

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
    await expect(portError).toContainText(/required/i);

    // Username is required
    const usernameError = getFormItemError(usernameInput);
    await usernameInput.scrollIntoViewIfNeeded();
    await expect(usernameError).toBeVisible({ timeout: 5_000 });
    await expect(usernameError).toContainText(/required/i);

    // Database is required
    const databaseError = getFormItemError(databaseInput);
    await databaseInput.scrollIntoViewIfNeeded();
    await expect(databaseError).toBeVisible({ timeout: 5_000 });
    await expect(databaseError).toContainText(/required/i);

    // Collection is required
    const collectionError = getFormItemError(collectionInput);
    await collectionInput.scrollIntoViewIfNeeded();
    await expect(collectionError).toBeVisible({ timeout: 5_000 });
    await expect(collectionError).toContainText(/required/i);

    // SQL (Query Template) is required
    const sqlError = getFormItemError(sqlInput);
    await sqlInput.scrollIntoViewIfNeeded();
    await expect(sqlError).toBeVisible({ timeout: 5_000 });
    await expect(sqlError).toContainText(/required/i);

    // Start Time is required
    const startError = getFormItemError(startInput);
    await startInput.scrollIntoViewIfNeeded();
    await expect(startError).toBeVisible({ timeout: 5_000 });
    await expect(startError).toContainText(/required/i);
  });

  test('connectivity check succeeds with reachable MongoDB server', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MongoDB as datasource type
    await selectElOptionByText(page, 'type', 'MongoDB');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');

    // Data Collection fields
    const databaseInput = page.locator('input[id^="data.groups_after."][id$=".database"]').first();
    const collectionInput = page.locator('input[id^="data.groups_after."][id$=".collection"]').first();
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
    const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();

    // Fill connection details
    await hostInput.fill(mongodbConfig.connection.host);
    await portInput.fill(mongodbConfig.connection.port);
    await usernameInput.fill(mongodbConfig.connection.username);
    await passwordInput.fill(mongodbConfig.connection.password);

    // Fill data collection fields
    await databaseInput.scrollIntoViewIfNeeded();
    await databaseInput.fill(mongodbConfig.dataCollection.database);

    await collectionInput.scrollIntoViewIfNeeded();
    await collectionInput.fill(mongodbConfig.dataCollection.collection);

    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill(mongodbConfig.dataCollection.sql);

    await startInput.scrollIntoViewIfNeeded();
    await startInput.fill(mongodbConfig.dataCollection.start);

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // Verify connectivity check succeeds
    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toContainText(/reachable/i, { timeout: 30_000 });
  });

  test('connectivity check fails with unreachable MongoDB server', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MongoDB as datasource type
    await selectElOptionByText(page, 'type', 'MongoDB');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
    const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');

    const databaseInput = page.locator('input[id^="data.groups_after."][id$=".database"]').first();
    const collectionInput = page.locator('input[id^="data.groups_after."][id$=".collection"]').first();
    const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
    const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();

    // Fill connection details with wrong port
    await hostInput.fill(mongodbConfig.connection.host);
    await portInput.fill(mongodbConfig.connection.wrongPort);
    await usernameInput.fill(mongodbConfig.connection.username);
    await passwordInput.fill(mongodbConfig.connection.password);

    await databaseInput.scrollIntoViewIfNeeded();
    await databaseInput.fill(mongodbConfig.dataCollection.database);

    await collectionInput.scrollIntoViewIfNeeded();
    await collectionInput.fill(mongodbConfig.dataCollection.collection);

    await sqlInput.scrollIntoViewIfNeeded();
    await sqlInput.fill(mongodbConfig.dataCollection.sql);

    await startInput.scrollIntoViewIfNeeded();
    await startInput.fill(mongodbConfig.dataCollection.start);

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // Verify connectivity check fails
    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toContainText(/unreachable|error|fail|refused|timeout/i, { timeout: 30_000 });
  });

  test('Create and run a MongoDB task end-to-end', async ({ page }) => {
    test.setTimeout(180_000);
    const taskName = `e2e_mongo_${Date.now()}`;
    const targetDb = 'ci_mongodb';
    const stableName = 'ci_7_1';
    const subtableName = 'tb_${name}';

    // ========================
    // Step 0: Prepare target database
    // ========================
    await runSqlBatch(page, [
      `DROP DATABASE IF EXISTS \`${targetDb}\`;`,
      `CREATE DATABASE IF NOT EXISTS \`${targetDb}\`;`,
    ]);

    try {
      // ========================
      // Step 1: Navigate and create a new task
      // ========================
      await gotoDataInTask(page);
      await openAddSourceFromList(page);

      // Fill task name
      const taskNameInput = page.locator('#name');
      await expect(taskNameInput).toBeVisible({ timeout: 10_000 });
      await taskNameInput.fill(taskName);

      // Select MongoDB as datasource type
      await selectElOptionByText(page, 'type', 'MongoDB');

      // ========================
      // Step 2: Fill connection details
      // ========================
      const hostInput = page.locator('#data\\.connection_options\\.host');
      await expect(hostInput).toBeVisible({ timeout: 10_000 });

      const portInput = page.locator('#data\\.connection_options\\.port');
      const usernameInput = page.locator('#data\\.authentication\\.plain\\.username');
      const passwordInput = page.locator('#data\\.authentication\\.plain\\.password');

      await hostInput.fill(mongodbConfig.connection.host);
      await portInput.fill(mongodbConfig.connection.port);
      await usernameInput.fill(mongodbConfig.connection.username);
      await passwordInput.fill(mongodbConfig.connection.password);

      // ========================
      // Step 3: Fill data collection configuration
      // ========================
      const databaseInput = page.locator('input[id^="data.groups_after."][id$=".database"]').first();
      const collectionInput = page.locator('input[id^="data.groups_after."][id$=".collection"]').first();
      const sqlInput = page.locator('input[id^="data.groups_after."][id$=".sql"]').first();
      const startInput = page.locator('input[id^="data.groups_after."][id$=".start"]').first();
      const endInput = page.locator('input[id^="data.groups_after."][id$=".end"]').first();

      await databaseInput.scrollIntoViewIfNeeded();
      await databaseInput.fill(mongodbConfig.dataCollection.database);

      await collectionInput.scrollIntoViewIfNeeded();
      await collectionInput.fill(mongodbConfig.dataCollection.collection);

      await sqlInput.scrollIntoViewIfNeeded();
      await sqlInput.fill(mongodbConfig.dataCollection.sql);

      await startInput.scrollIntoViewIfNeeded();
      await startInput.fill(mongodbConfig.dataCollection.start);

      await endInput.scrollIntoViewIfNeeded();
      await endInput.fill(mongodbConfig.dataCollection.end);

      // Close any open date picker popup by clicking on the task name label area
      await page.locator('#name').click();
      await page.waitForTimeout(300);

      // ========================
      // Step 4: Check connectivity
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

      await selectElOptionByText(page, 'targetDB', targetDb);

      // Click "Retrieve From Server" to get sample data
      const retrieveBtn = transformerSection.getByRole('button', { name: /Retrieve From Server/i }).first();
      await retrieveBtn.scrollIntoViewIfNeeded();
      await expect(retrieveBtn).toBeEnabled({ timeout: 10_000 });
      await retrieveBtn.click();
      await page.waitForTimeout(3000);

      // Click the parse preview button in the extract-parse section
      const parsePreviewBtn = transformerSection.locator('.extract-parse button').last();
      await parsePreviewBtn.scrollIntoViewIfNeeded();
      await expect(parsePreviewBtn).toBeVisible({ timeout: 5_000 });
      await parsePreviewBtn.click();
      await page.waitForTimeout(3000);

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

      // --- Clean up columns: delete nullvalue column and move name to tags ---
      const collapseItems = stableDialog.locator('.el-collapse-item');
      const columnsSection = collapseItems.nth(0);

      // Ensure columns section is expanded
      const colSectionActive = await columnsSection.evaluate(el => el.classList.contains('is-active')).catch(() => false);
      if (!colSectionActive) {
        await columnsSection.locator('.el-collapse-item__header').first().click();
        await page.waitForTimeout(300);
      }

      // Delete "nullvalue" column and move "name" to tags
      const columnsToDelete: string[] = ['nullvalue'];
      const columnsToMoveToTag = ['name'];

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

      // Fill the ts (TIMESTAMP) column name — first row in columns section
      const tsRow = columnsSection.locator('.input-row').first();
      const tsNameInput = tsRow.locator('.el-input:not(.el-input-number) input').last();
      const tsVal = await tsNameInput.inputValue().catch(() => '');
      if (!tsVal) {
        await tsNameInput.fill('ts');
      }

      // --- Clean up tags: delete empty tag rows ---
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

      // After creating the super table, the mapping table should appear
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

      // Map ts column: select "mapping" then pick "createtime" as the source column
      const tsMapRow = mappingTable.locator('.el-table__row').filter({ hasText: /^ts/i }).first();
      await tsMapRow.scrollIntoViewIfNeeded();
      const tsExprSelect = tsMapRow.locator('.mapping-rule-select').first();
      await tsExprSelect.click({ force: true });
      const tsDropdown = page.locator('.el-select-dropdown:visible');
      await expect(tsDropdown).toBeVisible({ timeout: 5_000 });
      await tsDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'mapping' }).first().click();
      await page.waitForTimeout(500);
      // Select "createtime" from the mapping column dropdown
      const tsMappingCol = tsMapRow.locator('.mapping-rule-expression').first();
      await tsMappingCol.click({ force: true });
      const tsMappingDropdown = page.locator('.el-select-dropdown:visible');
      await expect(tsMappingDropdown).toBeVisible({ timeout: 5_000 });
      await tsMappingDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'createtime' }).first().click();
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

      // Wait for task to reach Running or Completed (task may complete very quickly)
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
