import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';

test.describe('Sider / Routing (smoke)', () => {
  test('redirects /dataIn to /dataIn/Task', async ({ page }) => {
    await page.goto(routes.dataIn, { waitUntil: 'networkidle' });
    await expect(page).toHaveURL(/\/dataIn\/Task$/, { timeout: 15_000 });
  });

  test('Management menu is visible for root and navigates to /management/user', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    // Prefer accessible-role locators: stable and independent of DOM implementation details.
    const managementMenuItem = page.getByRole('menuitem', { name: /^management$/i });

    // In some builds, the menu visibility depends on localStorage.username.
    if (!(await managementMenuItem.isVisible())) {
      await page.addInitScript(() => {
        try {
          window.localStorage.setItem('username', 'root');
        } catch {
          // ignore
        }
      });
      await page.reload({ waitUntil: 'networkidle' });
    }

    await expect(managementMenuItem).toBeVisible({ timeout: 15_000 });
    await managementMenuItem.click();

    await expect(page).toHaveURL(/\/management\/user$/, { timeout: 15_000 });
    await expect(page.locator('.dnode-block .el-table').first()).toBeVisible({ timeout: 15_000 });
  });
});
