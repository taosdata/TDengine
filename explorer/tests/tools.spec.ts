import { test, expect } from './_utils/test';
import { gotoExplorer } from './_utils/explorerSql';
import { expectAllLinksValid } from './_utils/linkValidator';

test.describe('Tools Page', () => {
  test.beforeEach(async ({ page }) => {
    await gotoExplorer(page);
  });
  test('TDengine CLI tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const cliTool = page.getByRole('heading', { name: 'TDengine CLI', level: 2 }).first();
    await expect(cliTool).toBeVisible();
    await cliTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/TDengine%20CLI/);
    await expect(page.locator('body')).toContainText(/TDengine CLI/i);
    await expectAllLinksValid(page);
  });

  test('taosBenchmark tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const benchmarkTool = page.getByRole('heading', { name: 'taosBenchmark', level: 2 }).first();
    await expect(benchmarkTool).toBeVisible();
    await benchmarkTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/taosBenchmark/);
    await expect(page.locator('body')).toContainText(/taosBenchmark/i);
    await expectAllLinksValid(page);
  });

  test('taosDump tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const dumpTool = page.getByRole('heading', { name: 'taosDump', level: 2 }).first();
    await expect(dumpTool).toBeVisible();
    await dumpTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/taosDump/);
    await expect(page.locator('body')).toContainText(/taosDump/i);
    await expectAllLinksValid(page);
  });

  test('Grafana tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const grafanaTool = page.getByRole('heading', { name: 'Grafana', level: 2 }).first();
    await expect(grafanaTool).toBeVisible();
    await grafanaTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/Grafana/);
    await expect(page.locator('body')).toContainText(/Grafana/i);
    await expectAllLinksValid(page);
  });

  test('PowerBI tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const powerbiTool = page.getByRole('heading', { name: 'PowerBI', level: 2 }).first();
    await expect(powerbiTool).toBeVisible();
    await powerbiTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/PowerBI/);
    await expect(page.locator('body')).toContainText(/PowerBI/i);
    await expectAllLinksValid(page);
  });

  test('Superset tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const supersetTool = page.getByRole('heading', { name: 'Superset', level: 2 }).first();
    await expect(supersetTool).toBeVisible();
    await supersetTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/Superset/);
    await expect(page.locator('body')).toContainText(/Superset/i);
    await expectAllLinksValid(page);
  });

  test('Tableau tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const tableauTool = page.getByRole('heading', { name: 'Tableau', level: 2 }).first();
    await expect(tableauTool).toBeVisible();
    await tableauTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/Tableau/);
    await expect(page.locator('body')).toContainText(/Tableau/i);
    await expectAllLinksValid(page);
  });

  test('Excel tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const excelTool = page.getByRole('heading', { name: 'Excel', level: 2 }).first();
    await expect(excelTool).toBeVisible();
    await excelTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/Excel/);
    await expect(page.locator('body')).toContainText(/Excel/i);
    await expectAllLinksValid(page);
  });

  test('Node-RED tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const noderedTool = page.getByRole('heading', { name: 'Node-RED', level: 2 }).first();
    await expect(noderedTool).toBeVisible();
    await noderedTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/Node-RED/);
    await expect(page.locator('body')).toContainText(/Node-RED/i);
    await expectAllLinksValid(page);
  });

  test('Looker Studio tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const lookerTool = page.getByRole('heading', { name: 'Looker Studio', level: 2 }).first();
    await expect(lookerTool).toBeVisible();
    await lookerTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/Looker%20Studio/);
    await expect(page.locator('body')).toContainText(/Looker Studio/i);
    await expectAllLinksValid(page);
  });

  test('Yonghong BI tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const yonghongTool = page.getByRole('heading', { name: /Yonghong BI/i, level: 2 }).first();
    await expect(yonghongTool).toBeVisible();
    await yonghongTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/YonghongBI/);
    await expect(page.locator('body')).toContainText(/Yonghong BI/i);
    await expectAllLinksValid(page);
  });

  test('Seeq tool page and all links are valid', async ({ page }) => {
    await page.goto('/tools', { waitUntil: 'networkidle' });

    const seeqTool = page.getByRole('heading', { name: 'Seeq', level: 2 }).first();
    await expect(seeqTool).toBeVisible();
    await seeqTool.click();

    await page.waitForURL(/\/tools\/docs\/tool\/Seeq/);
    await expect(page.locator('body')).toContainText(/Seeq/i);
    await expectAllLinksValid(page);
  });
});
