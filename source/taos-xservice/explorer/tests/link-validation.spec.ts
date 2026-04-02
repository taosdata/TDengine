import { test, expect } from './_utils/test';
import { ensureAllLinksValid, expectAllLinksValid } from './_utils/linkValidator';

test.describe('Link Validation', () => {
  test('validates all external links on landing page', async ({ page }) => {
    await page.goto('/landing', { waitUntil: 'networkidle' });

    // Skip localhost and example.com links
    await expectAllLinksValid(page, {
      skipPatterns: [/localhost/, /example\.com/, /127\.0\.0\.1/],
      timeout: 10000
    });
  });

  test('validates links in footer', async ({ page }) => {
    await page.goto('/explorer', { waitUntil: 'networkidle' });

    const footer = page.locator('footer');
    const hasFooter = await footer.isVisible().catch(() => false);

    if (hasFooter) {
      const results = await ensureAllLinksValid(footer, {
        skipPatterns: [/localhost/],
        timeout: 10000,
        failFast: false
      });

      // Log results for debugging
      console.log(`Validated ${results.length} links in footer`);
      const failedLinks = results.filter(r => !r.ok);

      if (failedLinks.length > 0) {
        console.log('Failed links:', failedLinks);
      }

      expect(failedLinks).toHaveLength(0);
    } else {
      test.skip();
    }
  });

  test('validates documentation links', async ({ page }) => {
    await page.goto('/explorer', { waitUntil: 'networkidle' });

    // Find all links in help sections or documentation areas
    const docLinks = page.locator('[class*="help"], [class*="doc"]').locator('a[href^="http"]');
    const count = await docLinks.count();

    if (count > 0) {
      const results = await ensureAllLinksValid(page.locator('[class*="help"], [class*="doc"]'), {
        skipPatterns: [/localhost/],
        timeout: 10000,
        failFast: false
      });

      console.log(`Validated ${results.length} documentation links`);

      const failedLinks = results.filter(r => !r.ok);
      expect(failedLinks).toHaveLength(0);
    } else {
      test.skip();
    }
  });
});
