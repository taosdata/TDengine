import { test, expect } from './_utils/test';
import { runSql, gotoExplorer } from './_utils/explorerSql';

test.describe('Explorer SQL Error Handling', () => {
  test('shows error message for invalid SQL', async ({ page }) => {
    // Execute invalid SQL that will definitely fail
    await runSql(page, 'SELECT * FROM __table_that_does_not_exist_12345__;');

    // Wait for error notification or message to appear
    const errorMessage = page.locator('.el-message--error, .el-notification--error');
    await expect(errorMessage).toBeVisible({ timeout: 10_000 });

    // Verify Run button is re-enabled after error (using correct selector)
    const runBtn = page.locator('.sql-btn').getByRole('button', { name: 'Run' });
    await expect(runBtn).toBeEnabled({ timeout: 15_000 });
  });

  test('handles syntax error gracefully', async ({ page }) => {
    // Execute SQL with syntax error
    await runSql(page, 'SELCT * FORM invalid_syntax;'); // spellchecker:disable-line

    // Error should be displayed
    const errorIndicator = page.locator('.el-message--error, .el-notification--error, .error-message');
    await expect(errorIndicator).toBeVisible({ timeout: 10_000 });

    // Editor should remain functional (using correct selector)
    const editor = page.locator('.sql-code-editor .cm-editor');
    await expect(editor).toBeVisible();
  });

  test('executes multiple statements and shows batch results', async ({ page }) => {
    await gotoExplorer(page);

    // Execute multiple valid statements
    const batchSql = `SELECT server_version();
SELECT server_status();
SELECT client_version();`;

    await runSql(page, batchSql);

    // Wait for results to render
    await page.waitForTimeout(2000);

    // Run button should be enabled after batch execution
    const runBtn = page.locator('.sql-btn').getByRole('button', { name: 'Run' });
    await expect(runBtn).toBeEnabled({ timeout: 15_000 });

    // At least one result table should be visible
    const resultTable = page.locator('.gird .el-table__header-wrapper');
    await expect(resultTable.first()).toBeVisible({ timeout: 10_000 });
  });

  test('editor remains responsive after error', async ({ page }) => {
    // Execute invalid SQL
    await runSql(page, 'INVALID SQL STATEMENT;');

    // Wait for error
    await page.waitForTimeout(1000);

    // Try to execute valid SQL after error
    await runSql(page, 'SELECT server_version();');

    // Should show results successfully
    const resultTable = page.locator('.gird .el-table__header-wrapper th');
    await expect(resultTable.first()).toBeVisible({ timeout: 10_000 });
  });
});
