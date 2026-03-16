import { test, expect } from './_utils/test';
import { expectAllLinksValid } from './_utils/linkValidator';

test.describe('Programming Page', () => {
  test('renders programming connectors list', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    // Check main description is visible
    await expect(page.getByText(/Use the programming language/i)).toBeVisible({ timeout: 10_000 });

    // Check connector cards are displayed
    await expect(page.getByRole('heading', { name: 'Java', level: 2 })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Python', level: 2 })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Go', level: 2 })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Node.js', level: 2 })).toBeVisible();
  });

  test('connector links are clickable', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    // Find Java connector link
    const javaLink = page.getByRole('link', { name: /Java.*taos-jdbc/i });
    await expect(javaLink).toBeVisible({ timeout: 10_000 });

    // Verify link has correct href
    const href = await javaLink.getAttribute('href');
    expect(href).toContain('/programming/docs/connector/Java');
  });

  test('displays multiple connector types', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    // Check for various connectors
    const connectors = ['Java', 'Go', 'Python', 'Node.js', 'C#', 'Rust', 'R', 'REST API'];

    for (const connector of connectors) {
      await expect(page.getByRole('heading', { name: connector, level: 2 }).first()).toBeVisible();
    }
  });

  test('REST API connector is displayed and all links are valid', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    const restApi = page.getByRole('heading', { name: 'REST API', level: 2 }).first();
    await expect(restApi).toBeVisible();
    await restApi.click();

    await page.waitForURL(/\/programming\/docs\/connector\/REST/);
    await expect(page.getByText('In this section we will')).toBeVisible();
    await expectAllLinksValid(page);
  });

  test('Java connector is displayed and all links are valid', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    const javaConnector = page.getByRole('heading', { name: 'Java', level: 2 }).first();
    await expect(javaConnector).toBeVisible();
    await javaConnector.click();

    await page.waitForURL(/\/programming\/docs\/connector\/Java/);
    await expect(page.locator('body')).toContainText(/Java/i);
    await expectAllLinksValid(page);
  });

  test('Go connector is displayed and all links are valid', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    const goConnector = page.getByRole('heading', { name: 'Go', level: 2 }).first();
    await expect(goConnector).toBeVisible();
    await goConnector.click();

    await page.waitForURL(/\/programming\/docs\/connector\/Go/);
    await expect(page.locator('body')).toContainText(/Go/i);
    await expectAllLinksValid(page);
  });

  test('Python connector is displayed and all links are valid', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    const pythonConnector = page.getByRole('heading', { name: 'Python', level: 2 }).first();
    await expect(pythonConnector).toBeVisible();
    await pythonConnector.click();

    await page.waitForURL(/\/programming\/docs\/connector\/Python/);
    await expect(page.locator('body')).toContainText(/Python/i);
    await expectAllLinksValid(page);
  });

  test('Node.js connector is displayed and all links are valid', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    const nodeConnector = page.getByRole('heading', { name: 'Node.js', level: 2 }).first();
    await expect(nodeConnector).toBeVisible();
    await nodeConnector.click();

    await page.waitForURL(/\/programming\/docs\/connector\/Node.js/);
    await expect(page.locator('body')).toContainText(/Node\.js/i);
    await expectAllLinksValid(page);
  });

  test('C# connector is displayed and all links are valid', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    const csharpConnector = page.getByRole('heading', { name: 'C#', level: 2 }).first();
    await expect(csharpConnector).toBeVisible();
    await csharpConnector.click();

    await page.waitForURL(/\/programming\/docs\/connector\/C%23/);
    await expect(page.locator('body')).toContainText(/C#/i);
    await expectAllLinksValid(page);
  });

  test('Rust connector is displayed and all links are valid', async ({ page }) => {
    await page.goto('/programming', { waitUntil: 'networkidle' });

    const rustConnector = page.getByRole('heading', { name: 'Rust', level: 2 }).first();
    await expect(rustConnector).toBeVisible();
    await rustConnector.click();

    await page.waitForURL(/\/programming\/docs\/connector\/Rust/);
    await expect(page.locator('body')).toContainText(/Cargo.toml/i);
    await expectAllLinksValid(page);
  });
});
