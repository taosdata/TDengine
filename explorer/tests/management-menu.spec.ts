import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';

test.describe('Management (smoke)', () => {
  test('redirects /management to /management/user', async ({ page }) => {
    await page.goto(routes.management, { waitUntil: 'networkidle' });
    await expect(page).toHaveURL(/\/management\/user$/, { timeout: 15_000 });

    await expect(page.locator('.dnode-block .el-table').first()).toBeVisible({ timeout: 15_000 });
  });

  test('can open Add User dialog', async ({ page }) => {
    await page.goto(routes.managementUser, { waitUntil: 'networkidle' });

    const header = page.locator('.dnode-block .flex-end');
    const addBtn = header.getByRole('button', { name: 'Add' });

    await expect(addBtn).toBeVisible({ timeout: 15_000 });
    await addBtn.click();

    await expect(page.locator('.el-dialog')).toBeVisible({ timeout: 15_000 });
  });

  test('tabs update route (if Backup tab exists)', async ({ page }) => {
    await page.goto(routes.managementUser, { waitUntil: 'networkidle' });

    const backupTab = page.getByRole('tab', { name: /backup/i });
    if (!(await backupTab.count())) {
      test.skip(true, 'Backup tab not available in this build/license');
    }

    await backupTab.first().click();
    await expect(page).toHaveURL(/\/management\/backup$/, { timeout: 15_000 });
  });
});
