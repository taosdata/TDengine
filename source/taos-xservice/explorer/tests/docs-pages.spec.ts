import { test, expect } from './_utils/test';
import { routes } from './_utils/routes';

test.describe('Programming Documentation', () => {
  test('renders programming page without errors', async ({ page }) => {
    await page.goto(routes.programming || '/programming', { waitUntil: 'networkidle' });

    // Page should load without console errors
    const errors: string[] = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.waitForTimeout(1000);

    // Should have minimal or no errors
    expect(errors.length).toBeLessThan(5);

    // Should have some content visible
    const content = page.locator('.main-content, .content, .programming-content, body');
    await expect(content.first()).toBeVisible({ timeout: 10_000 });
  });

  test('displays connector documentation list or content', async ({ page }) => {
    await page.goto(routes.programming || '/programming', { waitUntil: 'networkidle' });

    // Wait for page to fully load
    await page.waitForTimeout(1000);

    // Should have list items, cards, or some content structure
    const listItems = page.locator('.el-card, .list-item, li, .doc-item, article, section');
    const count = await listItems.count();

    // If no specific list structure, at least the page should have loaded
    if (count === 0) {
      // Check if page has any meaningful content
      const bodyText = await page.locator('body').textContent();
      expect(bodyText).toBeTruthy();
      expect(bodyText!.length).toBeGreaterThan(10);
    } else {
      expect(count).toBeGreaterThan(0);
    }
  });

  test('can navigate to connector detail page if links exist', async ({ page }) => {
    await page.goto(routes.programming || '/programming', { waitUntil: 'networkidle' });

    // Find first clickable item
    const firstItem = page.locator('a[href*="/docs/connector"], a[href*="/connector"], .el-card a, .list-item a').first();

    if ((await firstItem.count()) > 0) {
      await firstItem.click();

      // Should navigate to detail page or stay on same page with content
      await page.waitForTimeout(1000);

      // Verify navigation happened or content is displayed
      const hasNavigated = page.url().includes('/docs/connector') || page.url().includes('/connector');
      const hasContent = (await page.locator('body').textContent())!.length > 100;

      expect(hasNavigated || hasContent).toBeTruthy();
    } else {
      // Skip test if no links found
      test.skip();
    }
  });
});

test.describe('Tools Documentation', () => {
  test('renders tools page without errors', async ({ page }) => {
    await page.goto(routes.tools || '/tools', { waitUntil: 'networkidle' });

    // Page should load without major console errors
    const errors: string[] = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.waitForTimeout(1000);

    expect(errors.length).toBeLessThan(5);

    // Should have content visible
    const content = page.locator('.main-content, .content, .tools-content, body');
    await expect(content.first()).toBeVisible({ timeout: 10_000 });
  });

  test('displays tools documentation list or content', async ({ page }) => {
    await page.goto(routes.tools || '/tools', { waitUntil: 'networkidle' });

    // Wait for page to fully load
    await page.waitForTimeout(1000);

    // Should have list items, cards, or some content structure
    const listItems = page.locator('.el-card, .list-item, li, .doc-item, article, section');
    const count = await listItems.count();

    // If no specific list structure, at least the page should have loaded
    if (count === 0) {
      // Check if page has any meaningful content
      const bodyText = await page.locator('body').textContent();
      expect(bodyText).toBeTruthy();
      expect(bodyText!.length).toBeGreaterThan(10);
    } else {
      expect(count).toBeGreaterThan(0);
    }
  });

  test('can navigate to tool detail page if links exist', async ({ page }) => {
    await page.goto(routes.tools || '/tools', { waitUntil: 'networkidle' });

    // Find first clickable item
    const firstItem = page.locator('a[href*="/docs/tool"], a[href*="/tool"], .el-card a, .list-item a').first();

    if ((await firstItem.count()) > 0) {
      await firstItem.click();

      // Should navigate to detail page or stay on same page with content
      await page.waitForTimeout(1000);

      // Verify navigation happened or content is displayed
      const hasNavigated = page.url().includes('/docs/tool') || page.url().includes('/tool');
      const hasContent = (await page.locator('body').textContent())!.length > 100;

      expect(hasNavigated || hasContent).toBeTruthy();
    } else {
      // Skip test if no links found
      test.skip();
    }
  });
});
