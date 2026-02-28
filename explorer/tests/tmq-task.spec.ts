import { test, expect } from './_utils/test';
import { runSqlBatch } from './_utils/explorerSql';
import { cleanupTmqResourcesBestEffort } from './_utils/cleanup';
import { openAddSourceFromList, selectElOptionByText } from './_utils/datain';
import { routes } from './_utils/routes';
import { ensureLogin } from './_utils/auth';

test.describe('DataIn - TMQ connectivity check', () => {
  // test.describe.configure({ mode: 'serial' });
  test('check connection succeeds for TMQ', async ({ page }) => {
    const ts = Date.now();

    const srcDb = `e2e_src_cc_${ts}`;
    const dstDb = `e2e_dst_cc_${ts}`;
    const topic = `e2e_topic_cc_${ts}`;

    const taskName = `e2e_tmq_cc_${ts}`;
    const clientId = `e2e_client_cc_${ts}`;
    const topicDsn = `ws://root:taosdata@127.0.0.1:6041/${topic}`;

    try {
      await runSqlBatch(page, [
        `CREATE DATABASE IF NOT EXISTS ${srcDb};`,
        `CREATE DATABASE IF NOT EXISTS ${dstDb};`,
        `CREATE TOPIC IF NOT EXISTS ${topic} AS DATABASE ${srcDb};`
      ]);

      await ensureLogin(page, routes.dataInTask);
      await openAddSourceFromList(page);

      await page.locator('#name').fill(taskName);
      await selectElOptionByText(page, 'targetDB', dstDb);

      const endpoint = page.locator('#data\\.connection_options\\.endpoint');
      // Avoid relying on a hardcoded groups_after UUID; match by stable prefix+suffix.
      const clientIdInput = page
        .locator('input[id^="data.groups_after."][id$=".client.id"]')
        .first();

      await expect(endpoint).toBeVisible();
      await endpoint.fill(topicDsn);

      await expect(clientIdInput).toBeVisible();
      await clientIdInput.fill(clientId);

      const checkBtn = page.locator('.btn-check-connectivity');
      await checkBtn.scrollIntoViewIfNeeded();
      await checkBtn.click();

      const result = page.locator('.box-check-connectivity .text');
      await expect(result).toBeVisible({ timeout: 60_000 });
      await expect(result).toContainText(/reachable/i);
    } finally {
      await cleanupTmqResourcesBestEffort(page, {
        topics: [topic],
        databases: [srcDb, dstDb]
      });
    }
  });
});
