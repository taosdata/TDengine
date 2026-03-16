import { expect, type Page, type Locator } from 'playwright/test';

/**
 * Validates all HTTP/HTTPS links within a page or locator.
 * Checks that links return non-error status codes (not 4xx or 5xx).
 *
 * @param target - Page or Locator to search for links
 * @param options - Configuration options
 * @returns Array of validation results
 */
export async function ensureAllLinksValid(
  target: Page | Locator,
  options: {
    /** Skip links matching these patterns */
    skipPatterns?: RegExp[];
    /** Timeout for each HEAD request in ms */
    timeout?: number;
    /** Whether to throw on first error or collect all errors */
    failFast?: boolean;
  } = {}
): Promise<{ url: string; status: number; ok: boolean }[]> {
  const { skipPatterns = [], timeout = 5000, failFast = false } = options;

  // Get the page object
  const page = 'goto' in target ? target : target.page();

  // Find all links with href starting with http or /
  const links = await (target as any).locator('a[hreaf^="/"],a[hraef^="http"]').all();

  const results: { url: string; status: number; ok: boolean }[] = [];
  const errors: string[] = [];

  for (const link of links) {
    const href = await link.getAttribute('href');
    if (!href) continue;

    // Skip if matches any skip pattern
    if (skipPatterns.some(pattern => pattern.test(href))) {
      continue;
    }

    try {
      // Use page.request to make HEAD request
      const response = await page.request.head(href, { timeout });
      const status = response.status();
      const ok = status < 400;

      results.push({ url: href, status, ok });

      if (!ok) {
        const errorMsg = `Link validation failed: ${href} returned status ${status}`;
        errors.push(errorMsg);

        if (failFast) {
          throw new Error(errorMsg);
        }
      }
    } catch (error) {
      const errorMsg = `Link validation error: ${href} - ${error instanceof Error ? error.message : String(error)}`;
      errors.push(errorMsg);

      results.push({ url: href, status: 0, ok: false });

      if (failFast) {
        throw new Error(errorMsg);
      }
    }
  }

  // If not fail-fast, throw with all errors at the end
  if (errors.length > 0 && !failFast) {
    throw new Error(`Link validation failed:\n${errors.join('\n')}`);
  }

  return results;
}

/**
 * Asserts that all HTTP/HTTPS links within a page or locator are valid.
 * This is a convenience wrapper around ensureAllLinksValid that uses Playwright's expect.
 *
 * @param target - Page or Locator to search for links
 * @param options - Configuration options
 */
export async function expectAllLinksValid(
  target: Page | Locator,
  options: {
    skipPatterns?: RegExp[];
    timeout?: number;
  } = {}
): Promise<void> {
  const results = await ensureAllLinksValid(target, { ...options, failFast: false });

  const failedLinks = results.filter(r => !r.ok);

  expect(failedLinks, `Expected all links to be valid, but ${failedLinks.length} failed`).toHaveLength(0);
}
