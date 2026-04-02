import { test, expect } from './_utils/test';
import { gotoExplorer, runSqlBatch } from './_utils/explorerSql';
import * as fs from 'fs';

test.describe('Explorer - Export Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await gotoExplorer(page);
  });

  test('exports query results to CSV file', async ({ page }, testInfo) => {
    // Execute a query with known results
    const editor = page.locator('.sql-code-editor .cm-editor .cm-content');
    await editor.click();
    await page.keyboard.press('Control+A');
    await page.keyboard.type('SELECT * FROM information_schema.ins_databases LIMIT 5;');

    const runBtn = page.locator('.sql-btn').getByRole('button', { name: 'Run' });
    await expect(runBtn).toBeEnabled({ timeout: 60_000 });
    await runBtn.click();
    await expect(runBtn).toBeEnabled({ timeout: 30_000 });

    // Wait for results to appear in the SQL result grid.
    await expect(page.locator('.gird .el-table__header-wrapper th').first()).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.gird .el-table__body-wrapper .el-table__row').first()).toBeVisible({
      timeout: 10_000
    });

    // Setup download listener
    const downloadPromise = page.waitForEvent('download');

    // Click export button
    const exportBtn = page.locator('.panel-right').getByRole('button', { name: 'Export' });
    await expect(exportBtn).toBeEnabled({ timeout: 5_000 });
    await exportBtn.click();

    // Confirm export dialog
    const confirmBtn = page.locator('.el-message-box__btns .el-button--primary');
    await expect(confirmBtn).toBeVisible({ timeout: 5_000 });
    await confirmBtn.click();

    // Wait for download
    const download = await downloadPromise;

    // Verify download
    expect(download.suggestedFilename()).toMatch(/\.csv$/i);

    // Save and verify file content
    const downloadPath = testInfo.outputPath(download.suggestedFilename());
    await download.saveAs(downloadPath);

    expect(fs.existsSync(downloadPath)).toBeTruthy();
    const fileSize = fs.statSync(downloadPath).size;
    expect(fileSize).toBeGreaterThan(0);

    // Cleanup
    fs.unlinkSync(downloadPath);
  });

  test('export button is disabled when no results', async ({ page }) => {
    const exportBtn = page.locator('.panel-right').getByRole('button', { name: 'Export' });
    await expect(exportBtn).toBeDisabled();
  });

  test('export handles large result sets', async ({ page }) => {
    // Create test database with data
    const testDb = `e2e_export_${Date.now()}`;
    await runSqlBatch(page, [
      `CREATE DATABASE IF NOT EXISTS ${testDb};`,
      `CREATE TABLE ${testDb}.test_table (ts TIMESTAMP, val INT);`,
      `INSERT INTO ${testDb}.test_table VALUES ${Array.from({ length: 100 }, (_, i) => `(NOW + ${i}s, ${i})`).join(
        ','
      )};`
    ]);

    try {
      // Execute query
      const editor = page.locator('.sql-code-editor .cm-editor .cm-content');
      await editor.click();
      await page.keyboard.press('Control+A');
      await page.keyboard.type(`SELECT * FROM ${testDb}.test_table;`);

      const runBtn = page.locator('.sql-btn').getByRole('button', { name: 'Run' });
      await runBtn.click();
      await expect(runBtn).toBeEnabled({ timeout: 30_000 });

      // Wait for result grid to render at least one row (UI pagination size is environment dependent).
      await expect(page.locator('.gird .el-table__header-wrapper th').first()).toBeVisible({ timeout: 30_000 });
      await expect(page.locator('.gird .el-table__body-wrapper .el-table__row').first()).toBeVisible({
        timeout: 30_000
      });

      // Export should work
      const downloadPromise = page.waitForEvent('download', { timeout: 30_000 });
      const exportBtn = page.locator('.panel-right').getByRole('button', { name: 'Export' });
      await exportBtn.click();

      const confirmBtn = page.locator('.el-message-box__btns .el-button--primary');
      await confirmBtn.click();

      const download = await downloadPromise;
      expect(download.suggestedFilename()).toBeTruthy();
    } finally {
      await runSqlBatch(page, [`DROP DATABASE IF EXISTS ${testDb};`]);
    }
  });

  test('export preserves column names and data types', async ({ page }, testInfo) => {
    const testDb = `e2e_export_types_${Date.now()}`;
    await runSqlBatch(page, [
      `CREATE DATABASE IF NOT EXISTS ${testDb};`,
      `CREATE TABLE ${testDb}.types_test (
        ts TIMESTAMP,
        int_col INT,
        float_col FLOAT,
        bool_col BOOL,
        str_col NCHAR(50)
      );`,
      `INSERT INTO ${testDb}.types_test VALUES (NOW, 123, 45.67, true, 'test string');`
    ]);

    try {
      const editor = page.locator('.sql-code-editor .cm-editor .cm-content');
      await editor.click();
      await page.keyboard.press('Control+A');
      await page.keyboard.type(`SELECT * FROM ${testDb}.types_test;`);

      const runBtn = page.locator('.sql-btn').getByRole('button', { name: 'Run' });
      await runBtn.click();
      await expect(runBtn).toBeEnabled({ timeout: 30_000 });

      // Wait for result grid and expected columns to render.
      const resultGrid = page.locator('.gird');
      await expect(resultGrid.locator('.el-table__header-wrapper th').first()).toBeVisible({ timeout: 10_000 });
      await expect(resultGrid).toContainText('int_col', { timeout: 10_000 });
      await expect(resultGrid).toContainText('float_col', { timeout: 10_000 });

      const downloadPromise = page.waitForEvent('download');
      const exportBtn = page.locator('.panel-right').getByRole('button', { name: 'Export' });
      await exportBtn.click();

      const confirmBtn = page.locator('.el-message-box__btns .el-button--primary');
      await confirmBtn.click();

      const download = await downloadPromise;
      const downloadPath = testInfo.outputPath(download.suggestedFilename());
      await download.saveAs(downloadPath);

      // Verify file contains expected data
      const content = fs.readFileSync(downloadPath, 'utf-8');
      expect(content).toContain('int_col');
      expect(content).toContain('float_col');
      expect(content).toContain('123');
      expect(content).toContain('test string');

      fs.unlinkSync(downloadPath);
    } finally {
      await runSqlBatch(page, [`DROP DATABASE IF EXISTS ${testDb};`]);
    }
  });
});
