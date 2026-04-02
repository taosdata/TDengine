import { test, expect } from './_utils/test';
import { runSqlBatch } from './_utils/explorerSql';
import { cleanupTmqResourcesBestEffort } from './_utils/cleanup';
import {
  findTaskRow,
  gotoDataInTask,
  openAddSourceFromList,
  selectElOptionByText,
  startTaskFromRow,
  stopTaskFromRow
} from './_utils/datain';
import { routes } from './_utils/routes';

test.describe('DataIn - TMQ task E2E (integrated env)', () => {
  // test.describe.configure({ mode: 'serial' });
  test('create TMQ task and start it', async ({ page }) => {
    const ts = Date.now();

    const srcDb = `e2e_src_${ts}`;
    const dstDb = `e2e_dst_${ts}`;
    const topic = `e2e_topic_${ts}`;

    const taskName = `e2e_tmq_${ts}`;
    const clientId = `e2e_client_${ts}`;
    const topicDsn = `ws://root:taosdata@127.0.0.1:6041/${topic}`;

    try {
      await runSqlBatch(page, [
        `CREATE DATABASE IF NOT EXISTS ${srcDb};`,
        `CREATE DATABASE IF NOT EXISTS ${dstDb};`,
        `CREATE TOPIC IF NOT EXISTS ${topic} AS DATABASE ${srcDb};`
      ]);

      await gotoDataInTask(page);
      await openAddSourceFromList(page);

      await page.locator('#name').fill(taskName);
      await selectElOptionByText(page, 'targetDB', dstDb);

      const endpoint = page.locator('#data\\.connection_options\\.endpoint');
      // Avoid relying on a hardcoded groups_after UUID; match by stable prefix+suffix.
      const clientIdInput = page.locator('input[id^="data.groups_after."][id$=".client.id"]').first();

      await expect(endpoint).toBeVisible();
      await endpoint.fill(topicDsn);

      await expect(clientIdInput).toBeVisible();
      await clientIdInput.fill(clientId);

      const submitBtn = page.locator('.btn-group-task').getByRole('button', { name: 'Submit' });

      await Promise.all([page.waitForURL(new RegExp(`${routes.dataInTask}$`), { timeout: 60_000 }), submitBtn.click()]);

      let row = await findTaskRow(page, taskName);
      await expect(row).toBeVisible();

      // Start task if it's not already running.
      await startTaskFromRow(page, row);

      // Wait for status to be in a started state.
      row = await findTaskRow(page, taskName);
      await expect(row).toContainText(/Queued|Started|Running/, { timeout: 10_000 });

      // Stop task if it's running.
      await stopTaskFromRow(page, row);

      // Wait for status to be in a stopped state.
      row = await findTaskRow(page, taskName);
      await expect(row).toContainText(/Stopping|Stopped/, { timeout: 10_000 });
    } finally {
      await cleanupTmqResourcesBestEffort(page, {
        taskName,
        topics: [topic],
        databases: [srcDb, dstDb]
      });
    }
  });
});
