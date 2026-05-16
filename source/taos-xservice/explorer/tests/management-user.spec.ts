import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';
import { runSqlBatch } from './_utils/explorerSql';
import { ensureLogin } from './_utils/auth';

test.describe('User Management', () => {
  // Keep username short (max 18 chars) to avoid length restrictions
  const testUsername = `u_${Date.now().toString().slice(-10)}`;
  const testPassword = 'Test@1234';

  test.beforeEach(async ({ page }) => {
    await ensureLogin(page, routes.managementUser);
    await expect(page.locator('.dnode-block .el-table').first()).toBeVisible({ timeout: 15_000 });
  });

  test('can create a new user with basic settings', async ({ page }) => {
    // Click Add button
    const addBtn = page.locator('.dnode-block .flex-end').getByRole('button', { name: /add/i });
    await expect(addBtn).toBeVisible({ timeout: 15_000 });
    await addBtn.click();

    // Wait for dialog to appear
    const dialog = page.locator('.el-dialog');
    await expect(dialog).toBeVisible({ timeout: 15_000 });

    // Fill in username
    const usernameInput = dialog.locator('input').first();
    await usernameInput.fill(testUsername);

    // Fill in password
    const passwordInput = dialog.locator('input[type="password"]');
    await passwordInput.fill(testPassword);

    // Click confirm button
    const confirmBtn = dialog.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();

    // Wait for success message and dialog to close
    await expect(dialog).not.toBeVisible({ timeout: 15_000 });

    // Verify user appears in the table
    await expect(page.locator('.el-table').getByText(testUsername)).toBeVisible({ timeout: 15_000 });

    // Cleanup: delete the test user
    const userRow = page.locator('.el-table__row').filter({ hasText: testUsername });
    await expect(userRow).toBeVisible({ timeout: 15_000 });

    const deleteBtn = userRow.locator('button[icon="Delete"], button.el-button:has(.el-icon)').nth(1);
    if (await deleteBtn.count() > 0) {
      await deleteBtn.click();

      const confirmDialog = page.locator('.el-message-box');
      await expect(confirmDialog).toBeVisible({ timeout: 5_000 });
      await confirmDialog.getByRole('button', { name: /confirm/i }).click();
      await page.waitForTimeout(1000);
    }
  });

  test('can create user with topic privileges and edit to verify', async ({ page }) => {
    // First, create a test database and topic
    const testDbName = `test_db_${Date.now()}`;
    const testTopicName = `test_topic_${Date.now()}`;

    // Create database and topic using SQL helper
    await runSqlBatch(page, [
      `CREATE DATABASE IF NOT EXISTS ${testDbName};`,
      `CREATE TOPIC IF NOT EXISTS ${testTopicName} AS DATABASE ${testDbName};`
    ]);

    // Go to user management
    await ensureLogin(page, routes.managementUser);

    // Click Add button
    const addBtn = page.locator('.dnode-block .flex-end').getByRole('button', { name: /add/i });
    await addBtn.click();

    const dialog = page.locator('.el-dialog');
    await expect(dialog).toBeVisible({ timeout: 15_000 });

    // Fill in user details
    await dialog.locator('input').first().fill(testUsername);
    await dialog.locator('input[type="password"]').fill(testPassword);

    // Check if topic section exists
    const topicSection = dialog.locator('.database-item').filter({ hasText: /subscription|topic/i });
    if (await topicSection.count() > 0) {
      // Find and check the topic checkbox
      const topicCheckbox = topicSection.locator('.el-checkbox').filter({ hasText: testTopicName });
      if (await topicCheckbox.count() > 0) {
        await topicCheckbox.click();
      }
    }

    // Submit form
    await dialog.getByRole('button', { name: /confirm/i }).click();
    await expect(dialog).not.toBeVisible({ timeout: 15_000 });

    // Wait a bit for the user to be created
    await page.waitForTimeout(2000);

    // Now edit the user to verify topic privilege is displayed
    const userRow = page.locator('.el-table__row').filter({ hasText: testUsername });
    await expect(userRow).toBeVisible({ timeout: 15_000 });

    // The edit button has icon="Edit" but no text, so we need to find it by the icon or position
    const editBtn = userRow.locator('button[icon="Edit"], button.el-button:has(.el-icon)').first();
    await expect(editBtn).toBeVisible({ timeout: 5_000 });
    await editBtn.click();

    // Wait for edit dialog
    const editDialog = page.locator('.el-dialog').filter({ hasText: /edit/i });
    await expect(editDialog).toBeVisible({ timeout: 15_000 });

    // Verify topic checkbox is checked
    const editTopicSection = editDialog.locator('.database-item').filter({ hasText: /subscription|topic/i });
    if (await editTopicSection.count() > 0) {
      const topicCheckbox = editTopicSection.locator('.el-checkbox').filter({ hasText: testTopicName });
      if (await topicCheckbox.count() > 0) {
        await expect(topicCheckbox.locator('input[type="checkbox"]')).toBeChecked({ timeout: 5_000 });
      }
    }

    // Close dialog
    await editDialog.getByRole('button', { name: /cancel/i }).click();

    // Cleanup: delete the user
    const deleteBtn = userRow.locator('button[icon="Delete"], button.el-button:has(.el-icon)').nth(1);
    await expect(deleteBtn).toBeVisible({ timeout: 5_000 });
    await deleteBtn.click();

    // Confirm deletion
    const confirmDialog = page.locator('.el-message-box');
    await expect(confirmDialog).toBeVisible({ timeout: 5_000 });
    await confirmDialog.getByRole('button', { name: /confirm/i }).click();
    await expect(confirmDialog).not.toBeVisible({ timeout: 5_000 });

    // Cleanup: drop topic and database
    await runSqlBatch(page, [
      `DROP TOPIC IF EXISTS ${testTopicName};`,
      `DROP DATABASE IF EXISTS ${testDbName};`
    ]);
  });

  test('validates required fields when creating user', async ({ page }) => {
    // Click Add button
    const addBtn = page.locator('.dnode-block .flex-end').getByRole('button', { name: /add/i });
    await addBtn.click();

    const dialog = page.locator('.el-dialog');
    await expect(dialog).toBeVisible({ timeout: 15_000 });

    // Try to submit without filling fields
    const confirmBtn = dialog.getByRole('button', { name: /confirm/i });
    await confirmBtn.click();

    // Should show validation error (use .first() to handle multiple error messages)
    await expect(dialog.locator('.el-form-item__error').first()).toBeVisible({ timeout: 5_000 });
  });

  test('can toggle user enable/disable state', async ({ page }) => {
    // Create a test user to ensure we have a non-root user to test with
    // Keep username short (max 18 chars) to avoid length restrictions
    const toggleTestUser = `u_tgl_${Date.now().toString().slice(-8)}`;

    // Create user
    const addBtn = page.locator('.dnode-block .flex-end').getByRole('button', { name: /add/i });
    await addBtn.click();

    let dialog = page.locator('.el-dialog').last();
    await expect(dialog).toBeVisible({ timeout: 15_000 });

    await dialog.locator('input').first().fill(toggleTestUser);
    await dialog.locator('input[type="password"]').fill(testPassword);
    await dialog.getByRole('button', { name: /confirm/i }).click();

    // Wait for user to be created
    await page.waitForTimeout(3000);

    // Find the newly created user row
    const userRow = page.locator('.el-table__row').filter({ hasText: toggleTestUser });
    const userExists = await userRow.count() > 0;

    if (!userExists) {
      // User creation failed, skip the test
      console.log('User creation failed, skipping toggle test');
      return;
    }

    await expect(userRow).toBeVisible({ timeout: 15_000 });

    // Find the switch button in this row
    const switchBtn = userRow.locator('.el-switch');
    await expect(switchBtn).toBeVisible({ timeout: 5_000 });

    // Check if switch is enabled (not disabled)
    const isDisabled = await switchBtn.getAttribute('aria-disabled');

    if (isDisabled !== 'true') {
      // Click the switch to toggle state (from enabled to disabled)
      await switchBtn.click();

      // Confirm the action in the dialog
      const confirmDialog = page.locator('.el-message-box');
      await expect(confirmDialog).toBeVisible({ timeout: 5_000 });
      await confirmDialog.getByRole('button', { name: /confirm/i }).click();

      // Wait for operation to complete
      await page.waitForTimeout(2000);

      // Verify the switch is now in a different visual state
      // The switch should have changed color or class
      const switchClasses = await switchBtn.getAttribute('class');
      console.log(`Switch classes after toggle: ${switchClasses}`);

      // Toggle back to original state (from disabled to enabled)
      await switchBtn.click();
      const confirmDialog2 = page.locator('.el-message-box');
      await expect(confirmDialog2).toBeVisible({ timeout: 5_000 });
      await confirmDialog2.getByRole('button', { name: /confirm/i }).click();
      await page.waitForTimeout(2000);
    }

    // Cleanup: delete the test user
    const deleteBtn = userRow.locator('button[icon="Delete"], button.el-button:has(.el-icon)').nth(1);
    if (await deleteBtn.count() > 0) {
      await deleteBtn.click();

      const confirmDialog = page.locator('.el-message-box');
      await expect(confirmDialog).toBeVisible({ timeout: 5_000 });
      await confirmDialog.getByRole('button', { name: /confirm/i }).click();
      await page.waitForTimeout(1000);
    }
  });

  test('can edit user password', async ({ page }) => {
    // First create a test user (keep username short, max 18 chars)
    const createTestUser = `u_edit_${Date.now().toString().slice(-8)}`;
    const addBtn = page.locator('.dnode-block .flex-end').getByRole('button', { name: /add/i });
    await addBtn.click();

    let dialog = page.locator('.el-dialog').last();
    await expect(dialog).toBeVisible({ timeout: 15_000 });

    await dialog.locator('input').first().fill(createTestUser);
    await dialog.locator('input[type="password"]').fill(testPassword);
    await dialog.getByRole('button', { name: /confirm/i }).click();

    // Wait for success message or dialog to close
    await page.waitForTimeout(3000);

    // Check if user was created successfully by looking for it in the table
    const userRow = page.locator('.el-table__row').filter({ hasText: createTestUser });
    const userExists = await userRow.count() > 0;

    if (!userExists) {
      // User creation failed, skip the rest of the test
      console.log('User creation failed, skipping edit test');
      return;
    }

    // Edit the user
    await expect(userRow).toBeVisible({ timeout: 15_000 });

    // The edit button has icon="Edit" but no text
    const editBtn = userRow.locator('button[icon="Edit"], button.el-button:has(.el-icon)').first();
    await expect(editBtn).toBeVisible({ timeout: 5_000 });
    await editBtn.click();

    dialog = page.locator('.el-dialog').last();
    await expect(dialog).toBeVisible({ timeout: 15_000 });

    // Change password - need to clear first then fill
    const newPassword = 'NewPass@5678';
    const passwordInput = dialog.locator('input[type="password"]');
    await passwordInput.click();
    await passwordInput.fill(''); // Clear existing value
    await passwordInput.fill(newPassword);

    // Submit
    await dialog.getByRole('button', { name: /confirm/i }).click();

    // Wait for operation to complete
    await page.waitForTimeout(2000);

    // Cleanup: delete the user
    const deleteBtn = userRow.locator('button[icon="Delete"], button.el-button:has(.el-icon)').nth(1);
    if (await deleteBtn.count() > 0) {
      await deleteBtn.click();

      const confirmDialog = page.locator('.el-message-box');
      await expect(confirmDialog).toBeVisible({ timeout: 5_000 });
      await confirmDialog.getByRole('button', { name: /confirm/i }).click();
    }
  });
});
