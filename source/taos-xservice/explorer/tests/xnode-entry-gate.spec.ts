import { test, expect } from './_utils/test';
import { gotoDataInTask } from './_utils/datain';
import { routes } from './_utils/routes';
import { TimeBasedXor } from '../src/utils/timeBasedXor';

const XNODE_COLUMN_META = [
  ['id', 'INT'],
  ['endpoint', 'VARCHAR'],
  ['status', 'VARCHAR']
] as const;

function mockShowXnodes(page: import('playwright/test').Page, xnodes: unknown[][]) {
  const xor = new TimeBasedXor(300);
  let showXnodeCall = 0;

  const routePromise = page.route('**/rest/sql**', async route => {
    const postData = route.request().postData() ?? '';
    let sql = postData;

    try {
      sql = xor.decrypt(postData);
    } catch {
      sql = postData;
    }

    if (/^\s*show\s+xnodes\b/i.test(sql)) {
      showXnodeCall += 1;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ code: 0, column_meta: XNODE_COLUMN_META, data: xnodes })
      });
      return;
    }

    await route.fallback();
  });

  return {
    routePromise,
    get showCount() {
      return showXnodeCall;
    }
  };
}

function didFileChooserOpen(page: import('playwright/test').Page, timeoutMs = 300) {
  return Promise.race([
    page.waitForEvent('filechooser').then(() => true),
    page.waitForTimeout(timeoutMs).then(() => false)
  ]);
}

test.describe('DataIn XNode entry gate — no XNodes', () => {
  test('Create New Task shows prompt and routes to Cluster on confirm', async ({ page }) => {
    const gate = mockShowXnodes(page, []);
    await gate.routePromise;
    await gotoDataInTask(page);

    await page.getByRole('button', { name: /Create New Task/i }).click();
    const dialog = page.getByRole('dialog', { name: /XNode Required/i });
    await expect(dialog).toBeVisible({ timeout: 15_000 });
    await expect(dialog.getByText(/create an XNode on the Cluster page/i)).toBeVisible({ timeout: 15_000 });
    await expect(dialog.getByRole('button', { name: 'Cancel' })).toBeVisible({ timeout: 15_000 });
    await dialog.getByRole('button', { name: /Go Create/i }).click();

    await expect(page).toHaveURL(new RegExp(`${routes.cluster}$`), { timeout: 15_000 });
    await expect(page.getByRole('tab', { name: /Cluster/i })).toHaveAttribute('aria-selected', 'true');
  });

  test('Import Task shows prompt instead of opening file chooser', async ({ page }) => {
    const gate = mockShowXnodes(page, []);
    await gate.routePromise;
    await gotoDataInTask(page);

    await page.locator('button').filter({ hasText: /^Import Task$/ }).first().click();
    const dialog = page.getByRole('dialog', { name: /XNode Required/i });
    await expect(dialog).toBeVisible({ timeout: 15_000 });
    await expect(dialog.getByText(/create an XNode on the Cluster page/i)).toBeVisible({ timeout: 15_000 });
    await expect(dialog.getByRole('button', { name: 'Cancel' })).toBeVisible({ timeout: 15_000 });

    const fileChooserTriggered = didFileChooserOpen(page);
    await dialog.getByRole('button', { name: 'Cancel' }).click();
    expect(await fileChooserTriggered).toBe(false);
    await expect(page).toHaveURL(new RegExp(`${routes.dataInTask}$`), { timeout: 15_000 });

  });
});

test.describe('DataIn XNode entry gate — XNode exists', () => {
  test('Create New Task routes directly to /dataIn/add without showing prompt', async ({ page }) => {
    const gate = mockShowXnodes(page, [[1, 'localhost:7100', 'online']]);
    await gate.routePromise;
    await gotoDataInTask(page);

    await page.getByRole('button', { name: /Create New Task/i }).click();

    await expect(page).toHaveURL(/\/dataIn\/add/, { timeout: 15_000 });
    await expect(page.getByRole('dialog', { name: /XNode Required/i })).not.toBeVisible();
  });

  test('Import Task opens file chooser directly without showing prompt', async ({ page }) => {
    const gate = mockShowXnodes(page, [[1, 'localhost:7100', 'online']]);
    await gate.routePromise;
    await gotoDataInTask(page);

    const fileChooser = page.waitForEvent('filechooser', { timeout: 10_000 });
    await page.locator('button').filter({ hasText: /^Import Task$/ }).first().click();

    // File chooser must open — confirms user activation was preserved.
    await fileChooser;
    await expect(page.getByRole('dialog', { name: /XNode Required/i })).not.toBeVisible();
  });
});
