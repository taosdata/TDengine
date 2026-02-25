import { test, expect } from './_utils/test';
import { runSqlBatch } from './_utils/explorerSql';
import { openAddSourceFromList, selectElOptionByText } from './_utils/datain';
import { routes } from './_utils/routes';

test.describe('DataIn - TMQ connectivity check', () => {
  test('check connection succeeds for TMQ', async ({ page }) => {
    const ts = Date.now();

    const srcDb = `e2e_src_cc_${ts}`;
    const dstDb = `e2e_dst_cc_${ts}`;
    const topic = `e2e_topic_cc_${ts}`;

    const taskName = `e2e_tmq_cc_${ts}`;
    const clientId = `e2e_client_cc_${ts}`;
    const topicDsn = `ws://root:taosdata@127.0.0.1:6041/${topic}`;

    await runSqlBatch(page, [
      `CREATE DATABASE IF NOT EXISTS ${srcDb};`,
      `CREATE DATABASE IF NOT EXISTS ${dstDb};`,
      `CREATE TOPIC IF NOT EXISTS ${topic} AS DATABASE ${srcDb};`
    ]);

    await page.goto(routes.dataInTask, { waitUntil: 'networkidle' });
    await openAddSourceFromList(page);

    await page.locator('#name').fill(taskName);
    await selectElOptionByText(page, 'targetDB', dstDb);

    const endpoint = page.locator('#data\\.connection_options\\.endpoint');
    const clientIdInput = page.locator(
      '#data\\.groups_after\\.d5209d3d-4964-437b-8762-f76a279adbc6\\.client\\.id'
    );

    await endpoint.fill(topicDsn);
    await clientIdInput.fill(clientId);

    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    const result = page.locator('.box-check-connectivity .text');
    await expect(result).toBeVisible({ timeout: 60_000 });
    await expect(result).toContainText(/reachable/i);
  });
});
