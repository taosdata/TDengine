import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';

test.describe('Topic Management', () => {
  test('renders topic list and refresh button works', async ({ page }) => {
    await page.goto(routes.topic || '/topic', { waitUntil: 'networkidle' });

    // Wait for table to be visible
    const table = page.locator('.el-table');
    await expect(table).toBeVisible({ timeout: 10_000 });

    // Find and click refresh button
    const refreshBtn = page.getByRole('button', { name: /refresh/i });
    if ((await refreshBtn.count()) > 0) {
      await refreshBtn.click();

      // Wait a moment for refresh to complete
      await page.waitForTimeout(1000);

      // Table should still be visible after refresh
      await expect(table).toBeVisible();
    }
  });

  test('opens Create Topic dialog', async ({ page }) => {
    await page.goto(routes.topic || '/topic', { waitUntil: 'networkidle' });

    // Find and click Create/Add Topic button
    const createBtn = page.getByRole('button', { name: /create|add.*topic/i });
    await expect(createBtn).toBeVisible({ timeout: 10_000 });
    await createBtn.click();

    // Dialog should appear
    const dialog = page.locator('.el-dialog');
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Dialog should have title containing "Topic"
    const dialogTitle = dialog.locator('.el-dialog__title');
    await expect(dialogTitle).toContainText(/topic/i);
  });

  test('Create Topic dialog has SQL mode', async ({ page }) => {
    await page.goto(routes.topic || '/topic', { waitUntil: 'networkidle' });

    const createBtn = page.getByRole('button', { name: /create|add.*topic/i });
    await createBtn.click();

    const dialog = page.locator('.el-dialog');
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Look for SQL tab or SQL mode toggle
    const sqlTab = dialog.getByRole('tab', { name: /sql/i });
    const sqlRadio = dialog.locator('input[type="radio"][value*="sql"], label:has-text("SQL")');

    const hasSqlMode = (await sqlTab.count()) > 0 || (await sqlRadio.count()) > 0;
    expect(hasSqlMode).toBeTruthy();
  });
});

test.describe('Stream Management', () => {
  test('opens Create Stream dialog', async ({ page }) => {
    await page.goto(routes.stream || '/stream', { waitUntil: 'networkidle' });

    // Find and click Create Stream button
    const createBtn = page.getByRole('button', { name: /create|add.*stream/i });
    await expect(createBtn).toBeVisible({ timeout: 10_000 });
    await createBtn.click();

    // Dialog should appear
    const dialog = page.locator('.el-dialog');
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Should have SQL input area (textarea or input field)
    const sqlInput = dialog.locator('textarea, input[type="text"], .sql-input, .cm-editor');
    await expect(sqlInput.first()).toBeVisible({ timeout: 5_000 });
  });

  test('validates SQL input in Create Stream', async ({ page }) => {
    await page.goto(routes.stream || '/stream', { waitUntil: 'networkidle' });

    const createBtn = page.getByRole('button', { name: /create|add.*stream/i });
    await createBtn.click();

    const dialog = page.locator('.el-dialog');
    await expect(dialog).toBeVisible({ timeout: 5_000 });

    // Try to submit without SQL
    const submitBtn = dialog.locator('.el-dialog__footer').getByRole('button', { name: /create|submit|ok/i });
    if ((await submitBtn.count()) > 0) {
      await submitBtn.click();

      // Should show validation error or remain on dialog
      await page.waitForTimeout(500);

      // Dialog should still be visible (validation failed)
      await expect(dialog).toBeVisible();
    }
  });
});
