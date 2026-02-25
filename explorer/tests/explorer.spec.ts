import { test, expect } from './_utils/test';
import { runSql } from './_utils/explorerSql';

test.describe('Explorer (SQL smoke)', () => {
  test('can run a SELECT and open export confirm', async ({ page }) => {
    await runSql(page, 'select server_version();');

    // At least one column header should be rendered.
    await expect(page.locator('.gird .el-table__header-wrapper th').first()).toBeVisible({ timeout: 15_000 });

    const exportBtn = page.locator('.panel-right').getByRole('button', { name: 'Export' });
    await expect(exportBtn).toBeEnabled({ timeout: 15_000 });

    await exportBtn.click();

    const msgBox = page.locator('.el-message-box');
    await expect(msgBox).toBeVisible({ timeout: 15_000 });

    await page.locator('.el-message-box__btns .el-button--primary').click();
    await expect(msgBox).toBeHidden({ timeout: 15_000 });
  });
});
