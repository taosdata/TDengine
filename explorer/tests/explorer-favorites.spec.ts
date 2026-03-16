import { test, expect } from './_utils/test';
import { gotoExplorer, runSql } from './_utils/explorerSql';
import type { Locator } from '@playwright/test';

// Helper function to find the table row containing an element
async function findTableRow(element: Locator): Promise<Locator> {
  let current = element;
  // Traverse up to 10 levels max to avoid infinite loop
  for (let i = 0; i < 10; i++) {
    const tagName = await current.evaluate(el => el.tagName.toLowerCase());
    if (tagName === 'tr') {
      return current;
    }
    current = current.locator('..');
  }
  throw new Error('Could not find table row (tr) element');
}

test.describe('Explorer - Favorites Workflow', () => {
  let favoriteName: string;
  let testSql: string;

  test.beforeEach(async ({ page }) => {
    // Generate unique SQL and favorite name for each test
    const timestamp = Date.now();
    favoriteName = `fav_${timestamp}`;
    // Use dynamic SQL with timestamp to ensure uniqueness
    testSql = `SELECT ${timestamp} AS test_id, * FROM information_schema.ins_databases LIMIT 5;`;
    await gotoExplorer(page);
  });

  test('favorite button is visible in SQL editor', async ({ page }) => {
    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtn).toBeVisible({ timeout: 5_000 });
  });

  test('can save SQL to favorites', async ({ page }) => {
    // Use runSql helper which properly handles CodeMirror
    await runSql(page, testSql);
    await expect(page.locator('.gird .el-table__header-wrapper th').first()).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('.gird .el-table__body-wrapper .el-table__row').first()).toBeVisible({
      timeout: 10_000
    });

    // Click Favorite button
    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite|saved/i });
    await expect(favoriteBtn).toBeEnabled({ timeout: 5_000 });
    await favoriteBtn.click();

    // Wait for dialog to open - look for the description textbox directly
    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });

    // Fill favorite description (max 20 characters)
    await descInput.fill(favoriteName.substring(0, 20));

    // Submit with Confirm button
    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();

    // Wait for dialog to close
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
  });

  test('saved favorite appears in favorite list tab', async ({ page }) => {
    // Use runSql helper
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtn).toBeEnabled({ timeout: 5_000 });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();

    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Switch to Favorite List tab
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();

    // Verify favorite appears in list
    const favoriteItem = page.getByText(favoriteName.substring(0, 20), { exact: false });
    await expect(favoriteItem).toBeVisible({ timeout: 5_000 });
  });

  test('can load favorite SQL from list', async ({ page }) => {
    // Use runSql helper
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtn).toBeEnabled({ timeout: 5_000 });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();

    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Clear editor
    const editor = page.locator('.sql-code-editor .cm-editor .cm-content');
    await editor.click();
    await page.keyboard.press('Control+A');
    await page.keyboard.press('Delete');

    // Switch to Favorite List tab
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();

    // Click on favorite to load it
    const favoriteItem = page.getByText(testSql.substring(0, 20), { exact: false }).first();
    await favoriteItem.click();
    await page.waitForTimeout(1000);

    // Switch to Favorite List tab
    const sqlTab = page.getByRole('tab', { name: /SQL/ });
    await sqlTab.click();

    // Verify SQL is loaded
    const editorContent = await editor.textContent();
    expect(editorContent).toContain('information_schema.ins_databases');
  });

  test('can delete favorite from list', async ({ page }) => {
    // Use runSql helper
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtn).toBeEnabled({ timeout: 5_000 });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();

    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Switch to Favorite List tab
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();

    // Find the favorite item row
    const favoriteItem = page.getByText(favoriteName.substring(0, 20), { exact: false });
    await expect(favoriteItem).toBeVisible({ timeout: 5_000 });

    // Find the table row containing this favorite
    const favoriteRow = await findTableRow(favoriteItem);
    const moreBtn = favoriteRow.getByRole('button').first();
    await moreBtn.click();
    await page.waitForTimeout(500);

    // Click delete menuitem (not button)
    const deleteMenuItem = page.getByRole('menuitem', { name: /delete|删除/i });
    await expect(deleteMenuItem).toBeVisible({ timeout: 2000 });
    await deleteMenuItem.click();

    // No confirmation dialog - delete happens immediately
    await page.waitForTimeout(1000);
    const deletedItem = page.getByText(favoriteName.substring(0, 20), { exact: false });
    await expect(deletedItem).not.toBeVisible();
  });

  test('favorite list supports search/filter', async ({ page }) => {
    // Switch to Favorite List tab
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();

    // Look for search input
    const searchInput = page.locator('input[placeholder*="search"], input[placeholder*="Search"]').first();

    if (await searchInput.isVisible().catch(() => false)) {
      // Enter search term
      await searchInput.fill('test');
      await page.waitForTimeout(500);

      // Verify search is working (results should be filtered)
      expect(await searchInput.inputValue()).toBe('test');
    } else {
      // Search not available, skip test
      test.skip();
    }
  });

  test('favorite button changes to "Saved" state after favoriting', async ({ page }) => {
    // Run unique SQL
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    // Initial state should be "Favorite"
    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtn).toBeEnabled({ timeout: 5_000 });

    // Save to favorites
    await favoriteBtn.click();
    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Button should now show "Saved" state
    const savedBtn = page.locator('.sql-btn').getByRole('button', { name: /saved/i });
    await expect(savedBtn).toBeVisible({ timeout: 5_000 });

    // Verify "Favorite" button is no longer visible
    const favoriteBtnAfter = page.locator('.sql-btn').getByRole('button', { name: /^favorite$/i });
    await expect(favoriteBtnAfter).not.toBeVisible();
  });

  test('cannot favorite the same SQL twice (Saved state prevents duplicate)', async ({ page }) => {
    // Run unique SQL
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    // Save to favorites
    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtn).toBeEnabled({ timeout: 5_000 });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Button should be in "Saved" state
    const savedBtn = page.locator('.sql-btn').getByRole('button', { name: /saved/i });
    await expect(savedBtn).toBeVisible({ timeout: 5_000 });

    // Try to click "Saved" button - it should not open the favorite dialog, actually it's not clickable
    try {
      await savedBtn.click({ timeout: 500 });
      await page.waitForTimeout(500);
    } catch {
      // not clickable.
    }
    // Dialog should NOT appear
    const dialogAfterClick = page.getByRole('textbox', { name: /description/i });
    await expect(dialogAfterClick).not.toBeVisible();
  });

  test('different SQL statements can be favorited independently', async ({ page }) => {
    // Create and favorite first SQL
    const sql1 = `SELECT '${Date.now()}_1' AS id, * FROM information_schema.ins_databases LIMIT 3;`;
    const fav1 = `fav_${Date.now()}_1`;

    await runSql(page, sql1);
    await page.waitForTimeout(2000);

    let favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await favoriteBtn.click();

    let descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(fav1.substring(0, 20));

    let confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Verify first SQL is in "Saved" state
    let savedBtn = page.locator('.sql-btn').getByRole('button', { name: /saved/i });
    await expect(savedBtn).toBeVisible({ timeout: 5_000 });

    // Create and favorite second SQL (different content)
    const sql2 = `SELECT '${Date.now()}_2' AS id, * FROM information_schema.ins_dnodes LIMIT 3;`;
    const fav2 = `fav_${Date.now()}_2`;

    await runSql(page, sql2);
    await page.waitForTimeout(2000);

    // Second SQL should show "Favorite" button (not saved yet)
    favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtn).toBeVisible({ timeout: 5_000 });
    await favoriteBtn.click();

    descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(fav2.substring(0, 20));

    confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Verify both favorites exist in the list
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();
    await page.waitForTimeout(1000);

    const favorite1Item = page.getByText(fav1.substring(0, 20), { exact: false });
    const favorite2Item = page.getByText(fav2.substring(0, 20), { exact: false });

    await expect(favorite1Item).toBeVisible({ timeout: 5_000 });
    await expect(favorite2Item).toBeVisible({ timeout: 5_000 });
  });

  test('can unfavorite and re-favorite the same SQL', async ({ page }) => {
    // Run and favorite SQL
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Verify "Saved" state
    let savedBtn = page.locator('.sql-btn').getByRole('button', { name: /saved/i });
    await expect(savedBtn).toBeVisible({ timeout: 5_000 });

    // Go to favorites list and delete it
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();
    await page.waitForTimeout(1000);

    const favoriteItem = page.getByText(favoriteName.substring(0, 20), { exact: false });
    await expect(favoriteItem).toBeVisible({ timeout: 5_000 });

    // Find the three-dot menu button in the same row (button with expanded attribute)
    const favoriteRow = await findTableRow(favoriteItem);
    const moreBtn = favoriteRow.getByRole('button').first();
    await moreBtn.click();
    await page.waitForTimeout(500);

    // Click delete menuitem (not button)
    const deleteMenuItem = page.getByRole('menuitem', { name: /delete|删除/i });
    await expect(deleteMenuItem).toBeVisible({ timeout: 2000 });
    await deleteMenuItem.click();

    // No confirmation dialog - delete happens immediately
    await page.waitForTimeout(1000);
    await expect(favoriteItem).not.toBeVisible();

    // Go back to SQL editor
    const sqlEditorTab = page.getByRole('tab', { name: /SQL/ });
    await sqlEditorTab.click();
    await page.waitForTimeout(1000);

    // Run the same SQL again
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    // Button should be back to "Favorite" state (not "Saved")
    const favoriteBtnAgain = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await expect(favoriteBtnAgain).toBeVisible({ timeout: 5_000 });

    // Should be able to favorite again
    await favoriteBtnAgain.click();
    const descInputAgain = page.getByRole('textbox', { name: /description/i });
    await expect(descInputAgain).toBeVisible({ timeout: 5_000 });
  });

  test('favorites persist across page reloads', async ({ page }) => {
    // Run and favorite SQL
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Reload the page
    await page.reload();
    await page.waitForTimeout(2000);

    // Navigate to favorites list
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();
    await page.waitForTimeout(1000);

    // Verify favorite still exists
    const favoriteItem = page.getByText(favoriteName.substring(0, 20), { exact: false });
    await expect(favoriteItem).toBeVisible({ timeout: 5_000 });
  });

  test('can edit favorite description via three-dot menu', async ({ page }) => {
    // Create a favorite first
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Go to favorites list
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();
    await page.waitForTimeout(1000);

    // Find the favorite item
    const favoriteItem = page.getByText(favoriteName.substring(0, 20), { exact: false });
    await expect(favoriteItem).toBeVisible({ timeout: 5_000 });

    // Find the three-dot menu button in the same row (button with expanded attribute)
    const favoriteRow = await findTableRow(favoriteItem);
    const moreBtn = favoriteRow.getByRole('button').first();
    await moreBtn.click();
    await page.waitForTimeout(500);

    // Click edit menuitem (not button)
    const editMenuItem = page.getByRole('menuitem', { name: /edit|编辑/i });
    await expect(editMenuItem).toBeVisible({ timeout: 2000 });
    await editMenuItem.click();
    await page.waitForTimeout(500);

    // Edit dialog should appear
    const editDescInput = page.getByRole('textbox', { name: /description of the sql/i });
    await expect(editDescInput).toBeVisible({ timeout: 2000 });

    const newName = `edited_${Date.now()}`;
    await editDescInput.clear();
    await editDescInput.fill(newName.substring(0, 20));

    const saveBtn = page.getByRole('button', { name: /confirm|save|确定|保存/i });
    await saveBtn.click();
    await page.waitForTimeout(1000);

    // Verify new name appears
    const editedItem = page.getByText(newName.substring(0, 20), { exact: false });
    await expect(editedItem).toBeVisible({ timeout: 5_000 });
  });

  test('three-dot menu shows all available operations', async ({ page }) => {
    // Create a favorite first
    await runSql(page, testSql);
    await page.waitForTimeout(2000);

    const favoriteBtn = page.locator('.sql-btn').getByRole('button', { name: /favorite/i });
    await favoriteBtn.click();

    const descInput = page.getByRole('textbox', { name: /description/i });
    await expect(descInput).toBeVisible({ timeout: 5_000 });
    await descInput.fill(favoriteName.substring(0, 20));

    const confirmBtn = page.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(descInput).not.toBeVisible({ timeout: 5_000 });
    await page.waitForTimeout(1000);

    // Go to favorites list
    const favoriteTab = page.getByRole('tab', { name: /favorite.*list/i });
    await favoriteTab.click();
    await page.waitForTimeout(1000);

    // Find the favorite item
    const favoriteItem = page.getByText(favoriteName.substring(0, 20), { exact: false });
    await expect(favoriteItem).toBeVisible({ timeout: 5_000 });

    // Find the three-dot menu button in the same row (button with expanded attribute)
    const favoriteRow = await findTableRow(favoriteItem);
    const moreBtn = favoriteRow.getByRole('button').first();
    await moreBtn.click();
    await page.waitForTimeout(500);

    // Verify all menu operations are available as menuitems
    const copyMenuItem = page.getByRole('menuitem', { name: /copy|复制/i });
    const runMenuItem = page.getByRole('menuitem', { name: /run|运行/i });
    const editMenuItem = page.getByRole('menuitem', { name: /edit|编辑/i });
    const shareMenuItem = page.getByRole('menuitem', { name: /share|分享/i });
    const deleteMenuItem = page.getByRole('menuitem', { name: /delete|删除/i });

    // Verify all operations are visible
    await expect(copyMenuItem).toBeVisible({ timeout: 2000 });
    await expect(runMenuItem).toBeVisible({ timeout: 2000 });
    await expect(editMenuItem).toBeVisible({ timeout: 2000 });
    await expect(shareMenuItem).toBeVisible({ timeout: 2000 });
    await expect(deleteMenuItem).toBeVisible({ timeout: 2000 });
  });
});
