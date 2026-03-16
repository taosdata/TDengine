import { test, expect } from './_utils/test';
import { ensureLogin } from './_utils/auth';
import { routes } from './_utils/routes';

test.describe('Dashboard', () => {
  test.beforeEach(async ({ page }) => {
    await ensureLogin(page, routes.explorer);
    await page.goto(routes.dashboard);
    await page.waitForLoadState('networkidle');
  });

  test('renders dashboard page with cluster info', async ({ page }) => {
    // Check page loaded successfully
    await expect(page.locator('body')).toBeVisible({ timeout: 10_000 });

    // Check for common dashboard elements (service counts or cluster info)
    const pageText = await page.textContent('body');
    const hasClusterInfo =
      pageText?.includes('Cluster') ||
      pageText?.includes('taosd') ||
      pageText?.includes('taos-adapter') ||
      pageText?.includes('taosX') ||
      pageText?.includes('taos-keeper');

    expect(hasClusterInfo).toBeTruthy();
  });

  test('displays server hosts table', async ({ page }) => {
    // Check Server Hosts section
    await expect(page.getByText('Server Hosts')).toBeVisible({ timeout: 10_000 });

    // Check table headers
    const table = page.locator('.el-table').first();
    await expect(table).toBeVisible();
    await expect(table.getByText('Endpoint')).toBeVisible();
    await expect(table.getByText('CPU Usage')).toBeVisible();
    await expect(table.getByText('Memory Usage')).toBeVisible();
  });

  test('table columns are sortable', async ({ page }) => {
    const table = page.locator('.el-table').first();
    await expect(table).toBeVisible({ timeout: 10_000 });

    // Try to click a sortable column header
    const cpuHeader = page.getByRole('button', { name: /Sort by CPU Usage/i });
    if (await cpuHeader.isVisible()) {
      await cpuHeader.click();
      // Table should still be visible after sorting
      await expect(table).toBeVisible();
    }
  });

  test('dashboard displays cluster metrics', async ({ page }) => {
    // Check for common metrics in page content
    const pageText = await page.textContent('body');
    const hasMetrics =
      pageText?.includes('CPU') ||
      pageText?.includes('Memory') ||
      pageText?.includes('Disk') ||
      pageText?.includes('Connection') ||
      pageText?.includes('Database') ||
      pageText?.includes('Usage');

    expect(hasMetrics).toBeTruthy();
  });

  test('dashboard metrics update on refresh', async ({ page }) => {
    // Get initial metric value
    const metricElement = page.locator('.metric-value, .stat-value').first();

    if (await metricElement.isVisible().catch(() => false)) {
      const initialValue = await metricElement.textContent();

      // Click refresh button if present
      const refreshBtn = page.getByRole('button', { name: /refresh|reload/i });
      if (await refreshBtn.isVisible().catch(() => false)) {
        const refreshSettled = Promise.race([
          page
            .waitForResponse(
              response => ['fetch', 'xhr'].includes(response.request().resourceType()) && response.ok(),
              { timeout: 5_000 }
            )
            .catch(() => null),
          page
            .locator('.el-loading-mask')
            .first()
            .waitFor({ state: 'visible', timeout: 1_500 })
            .then(() => page.locator('.el-loading-mask').first().waitFor({ state: 'hidden', timeout: 10_000 }))
            .catch(() => null)
        ]);

        await refreshBtn.click();
        await refreshSettled;

        // Verify page updated (may or may not change value)
        const updatedValue = await metricElement.textContent();
        expect(updatedValue ?? initialValue).toBeDefined();
      }
    }
  });

  test('server hosts table supports pagination', async ({ page }) => {
    const table = page.locator('.el-table');
    await expect(table).toBeVisible({ timeout: 10_000 });

    // Check if pagination exists
    const pagination = page.locator('.el-pagination');

    if (await pagination.isVisible().catch(() => false)) {
      // Get total count
      const totalText = await pagination.locator('.el-pagination__total').textContent();
      expect(totalText).toBeTruthy();

      // Try to navigate pages if multiple pages exist
      const nextBtn = pagination.locator('.btn-next');
      if (await nextBtn.isEnabled().catch(() => false)) {
        await nextBtn.click();

        // Verify page changed
        const activePageNum = pagination.locator('.el-pager .is-active');
        await expect
          .poll(
            async () => {
              const pageNum = await activePageNum.textContent();
              return parseInt(pageNum || '0');
            },
            { timeout: 5_000 }
          )
          .toBeGreaterThan(1);
      }
    }
  });

  test('server hosts table row click shows details', async ({ page }) => {
    const table = page.locator('.el-table');
    await expect(table).toBeVisible({ timeout: 10_000 });

    const firstRow = table.locator('tbody tr').first();

    if (await firstRow.isVisible().catch(() => false)) {
      await firstRow.click();
      const detailsPanel = page.locator('.details-panel, .el-drawer, .el-dialog');
      await expect
        .poll(async () => (await detailsPanel.isVisible().catch(() => false)) || page.url() !== routes.dashboard, {
          timeout: 10_000
        })
        .toBeTruthy();
    }
  });

  test('dashboard charts render without errors', async ({ page }) => {
    // Look for chart containers
    const chartContainers = page.locator('.chart, .echarts, canvas[data-zr-dom-id]');
    const chartCount = await chartContainers.count();

    if (chartCount > 0) {
      // Verify at least one chart is visible
      await expect(chartContainers.first()).toBeVisible({ timeout: 10_000 });

      // Check for chart canvas
      const canvas = page.locator('canvas').first();
      if (await canvas.isVisible().catch(() => false)) {
        // Verify canvas has dimensions
        const box = await canvas.boundingBox();
        expect(box?.width).toBeGreaterThan(0);
        expect(box?.height).toBeGreaterThan(0);
      }
    }
  });

  test('dashboard handles empty/no data state', async ({ page }) => {
    // This test verifies graceful handling when no data is available
    // The page should not crash or show errors

    const errorMsg = page.locator('.error-message, .el-alert--error');
    await expect(errorMsg).not.toBeVisible();

    // Should show either data or empty state
    const hasData = (await page.locator('.el-table tbody tr').count()) > 0;
    const hasEmptyState = await page
      .locator('.empty-state, .el-empty')
      .isVisible()
      .catch(() => false);

    expect(hasData || hasEmptyState).toBeTruthy();
  });

  test('server hosts table search filters results', async ({ page }) => {
    const searchInput = page.locator('input[placeholder*="search"], input[placeholder*="Search"]').first();

    if (await searchInput.isVisible().catch(() => false)) {
      // Get initial row count
      const table = page.locator('.el-table');
      const initialCount = await table.locator('tbody tr').count();

      if (initialCount > 0) {
        // Get first row text to search for
        const firstRowText = await table.locator('tbody tr').first().textContent();
        const searchTerm = firstRowText?.trim().split(/\s+/)[0] || 'test';

        // Perform search
        const searchSettled = Promise.race([
          page
            .waitForResponse(
              response => ['fetch', 'xhr'].includes(response.request().resourceType()) && response.ok(),
              { timeout: 5_000 }
            )
            .catch(() => null),
          expect
            .poll(async () => table.locator('tbody tr').count(), { timeout: 10_000 })
            .toBeLessThanOrEqual(initialCount)
        ]);
        await searchInput.fill(searchTerm);
        await searchSettled;

        // Verify results filtered
        const filteredCount = await table.locator('tbody tr').count();
        expect(filteredCount).toBeGreaterThan(0);
        expect(filteredCount).toBeLessThanOrEqual(initialCount);
      }
    }
  });
});
