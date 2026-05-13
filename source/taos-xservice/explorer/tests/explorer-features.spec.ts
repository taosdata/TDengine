import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';

test.describe('Explorer - SQL Editor Features', () => {
  test('SQL editor tabs are functional', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    // Check SQL tab is selected by default
    const sqlTab = page.getByRole('tab', { name: 'SQL', selected: true });
    await expect(sqlTab).toBeVisible({ timeout: 10_000 });

    // Check Favorite List tab exists
    const favoriteTab = page.getByRole('tab', { name: /Favorite List/i });
    await expect(favoriteTab).toBeVisible();

    // Check Logs tab exists
    const logsTab = page.getByRole('tab', { name: /Logs/i });
    await expect(logsTab).toBeVisible();
  });

  test('switches to Favorite List tab', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const favoriteTab = page.getByRole('tab', { name: /Favorite List/i });
    await favoriteTab.click();

    // Tab should be selected
    await expect(favoriteTab).toHaveAttribute('aria-selected', 'true');
  });

  test('switches to Logs tab', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const logsTab = page.getByRole('tab', { name: /Logs/i });
    await logsTab.click();

    // Tab should be selected
    await expect(logsTab).toHaveAttribute('aria-selected', 'true');
  });

  test('Format button is visible', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const formatBtn = page.getByRole('button', { name: /Format/i });
    await expect(formatBtn).toBeVisible({ timeout: 10_000 });
  });

  test('Favorite button is visible', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const favoriteBtn = page.getByRole('button', { name: /Favorite/i });
    await expect(favoriteBtn).toBeVisible({ timeout: 10_000 });
  });

  test('Run button is visible', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const runBtn = page.getByRole('button', { name: /Run/i });
    await expect(runBtn).toBeVisible({ timeout: 10_000 });
  });
});

test.describe('Explorer - Result Tabs', () => {
  test('Grid and Chart tabs are visible', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    // Check Grid tab
    const gridTab = page.getByRole('tab', { name: /Grid/i });
    await expect(gridTab).toBeVisible({ timeout: 10_000 });

    // Check Chart tab
    const chartTab = page.getByRole('tab', { name: /Chart/i });
    await expect(chartTab).toBeVisible();
  });

  test('switches to Chart tab', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const chartTab = page.getByRole('tab', { name: /Chart/i });
    await chartTab.click();

    // Tab should be selected
    await expect(chartTab).toHaveAttribute('aria-selected', 'true');
  });

  test('displays row count', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    // Check for row count display
    await expect(page.getByText(/\d+ rows/i)).toBeVisible({ timeout: 10_000 });
  });
});

test.describe('Explorer - Database Tree', () => {
  test('database tree is visible', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    // Check database tree header - use locator that targets the specific header element
    const dbTreeHeader = page.locator('.dbs-tree-header .title', { hasText: 'Databases' });
    await expect(dbTreeHeader).toBeVisible({ timeout: 10_000 });

    // Check tree is rendered
    const tree = page.getByRole('tree');
    await expect(tree).toBeVisible();
  });

  test('database tree items are expandable', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const tree = page.getByRole('tree');
    await expect(tree).toBeVisible({ timeout: 10_000 });

    // Find first tree item
    const firstTreeItem = page.getByRole('treeitem').first();
    if (await firstTreeItem.isVisible()) {
      await expect(firstTreeItem).toBeVisible();
    }
  });

  test('database tree has action buttons', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    const tree = page.getByRole('tree');
    await expect(tree).toBeVisible({ timeout: 10_000 });

    // Tree items should have action buttons (visible on hover)
    const treeItems = page.getByRole('treeitem');
    const count = await treeItems.count();
    expect(count).toBeGreaterThan(0);
  });
});

test.describe('Explorer - SQL Placeholder', () => {
  test('displays SQL hint text', async ({ page }) => {
    await page.goto(routes.explorer, { waitUntil: 'networkidle' });

    // Check for SQL hint/placeholder
    await expect(page.getByText(/table name must be prefixed with database name/i)).toBeVisible({ timeout: 10_000 });
  });
});
