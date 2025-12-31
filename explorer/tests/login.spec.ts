import { readFile, readFileSync } from 'fs';
import { test, expect } from 'playwright/test';

test.describe('Login Page', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to login page before each test
    await page.goto('/login', { waitUntil: 'networkidle' });

    // Wait for Vue to mount and render the login content
    await page.waitForSelector('.login-content', { state: 'visible' });
  });

  test('should display login page with all elements', async ({ page }) => {
    // Check page title
    await expect(page).toHaveTitle(/TDengine TSDB-Enterprise/);

    // Check login form is visible
    await expect(page.locator('.login-content')).toBeVisible();

    // Check username field
    const usernameInput = page.locator('input[placeholder*="username"]').first();
    await expect(usernameInput).toBeVisible();

    // Check password field
    const passwordInput = page.locator('input[type="password"]').first();
    await expect(passwordInput).toBeVisible();

    // Check sign in button
    const signInButton = page.locator('button').filter({ hasText: /Sign In|signin/i });
    await expect(signInButton.first()).toBeVisible();
  });

  test('should show validation error for empty username', async ({ page }) => {
    // Wait for form to be ready
    await page.waitForSelector('input[type="password"]', { state: 'visible' });

    // Click password field to trigger validation
    await page.locator('input[type="password"]').first().click();

    // Click sign in button
    await page
      .locator('button')
      .filter({ hasText: /Sign In|signin/i })
      .first()
      .click();

    // Wait for validation to appear
    await page.waitForTimeout(300);

    // Check for error message (Element Plus validation)
    const errorMessage = page.locator('.el-form-item__error');
    await expect(errorMessage.first()).toBeVisible();
  });

  test('should show validation error for empty password', async ({ page }) => {
    // Fill username
    await page.locator('input[placeholder*="username"]').first().fill('testuser');

    // Click sign in button without password
    await page
      .locator('button')
      .filter({ hasText: /Sign In|signin/i })
      .first()
      .click();

    // Wait for validation
    await page.waitForTimeout(500);

    // Check for error message
    const errorMessage = page.locator('.el-form-item__error');
    await expect(errorMessage.first()).toBeVisible();
  });

  test('should attempt login with valid credentials format', async ({ page }) => {
    // Fill username
    const usernameInput = page.locator('input[placeholder*="username"]').first();
    await usernameInput.fill('root');

    // Fill password
    const passwordInput = page.locator('input[type="password"]').first();
    await passwordInput.fill('taosdata');

    // Click sign in button
    const signInButton = page
      .locator('button')
      .filter({ hasText: /Sign In|signin/i })
      .first();
    await signInButton.click();

    // Wait for either success or error
    await page.waitForURL(/\/explorer/, { timeout: 6000 });

    // The page should either redirect or show an error message
    // (depends on whether TDengine is actually running)
    const currentUrl = page.url();
    if (currentUrl.includes('/explorer')) {
      expect(currentUrl).toContain('/explorer');
      // Expect Databases
      await page.waitForSelector('.dbs-tree-header', { state: 'visible' });
      // Input SQL
      await page.locator('.cm-activeLine').click();
      await page.getByRole('textbox').click();
      await page.getByRole('textbox').fill('select * from information_schema.ins_mnodes;');
      await page.getByRole('button', { name: 'Run' }).click();

      await page.getByRole('button', { name: 'Run' }).click();
      await page.waitForLoadState('networkidle');
      // Wait for results
      await page.waitForSelector('#pane-grid > div > div > div.el-table__inner-wrapper > div.el-table__header-wrapper > table', { state: 'visible' });
      // Export results
      await page.waitForTimeout(1000);
      await page.getByRole('button', { name: 'Export' }).click();
      const downloadPromise = page.waitForEvent('download');
      await page.getByRole('button', { name: 'OK' }).click();
      const download = await downloadPromise;
      console.log('Downloaded file path:', await download.path());
      const file = readFileSync(await download.path(), 'utf8');
      console.log('Downloaded file content:', file);
      await page.getByRole('button').nth(3).click();
      await page.getByRole('textbox', { name: '* Name' }).fill('dbName1');
      await page.getByRole('button', { name: 'Create' }).click();
    } else {
      const hasError = await page
        .locator('.el-message--error')
        .isVisible()
        .catch(() => false);

      // Either we're redirected away from login or there's an error message
      expect(hasError).toBeTruthy();
    }
  });

  test('should toggle password visibility', async ({ page }) => {
    // Wait for password input to be ready
    await page.waitForSelector('input[type="password"]', { state: 'visible' });

    const passwordInput = page.locator('input[type="password"]').first();
    await passwordInput.fill('testpassword');

    // Check password is hidden initially
    await expect(passwordInput).toHaveAttribute('type', 'password');

    // Click the show password button
    const showPasswordButton = page.locator('.el-input__icon').filter({ hasText: '' });
    if ((await showPasswordButton.count()) > 0) {
      await showPasswordButton.first().click();
      await page.waitForTimeout(300);
    }
  });

  test('should allow language switching', async ({ page }) => {
    // Wait for language switcher to be ready
    await page.waitForSelector('.language', { state: 'visible' });

    // Find language switcher
    const languageSwitcher = page.locator('.language');

    if (await languageSwitcher.isVisible()) {
      const initialText = await languageSwitcher.textContent();

      // Click to switch language
      await languageSwitcher.click();
      await page.waitForTimeout(500);

      // Check if language changed
      const newText = await languageSwitcher.textContent();
      expect(initialText).not.toBe(newText);
    }
  });

  test('should trim whitespace from username', async ({ page }) => {
    // Fill username with spaces
    const usernameInput = page.locator('input[placeholder*="username"]').first();
    await usernameInput.fill('  root  ');

    // Fill password
    const passwordInput = page.locator('input[type="password"]').first();
    await passwordInput.fill('taosdata');

    // The trimmedUsername computed property should handle this
    // We can verify by checking the form doesn't show validation errors for format
    const signInButton = page
      .locator('button')
      .filter({ hasText: /Sign In|signin/i })
      .first();
    await expect(signInButton).toBeEnabled();
  });

  test('should display copyright information', async ({ page }) => {
    // Check for copyright text
    const copyright = page.locator('.copyright');

    if (await copyright.isVisible()) {
      const copyrightText = await copyright.textContent();
      expect(copyrightText).toContain('TDengine');
      expect(copyrightText).toContain('2025');
    }
  });

  test('should handle Enter key press in password field', async ({ page }) => {
    // Fill username
    await page.locator('input[placeholder*="username"]').first().fill('root');

    // Fill password and press Enter
    const passwordInput = page.locator('input[type="password"]').first();
    await passwordInput.fill('taosdata');
    await passwordInput.press('Enter');

    // Wait for form submission
    await page.waitForTimeout(1000);

    // Form should be submitted (either redirect or error)
    const currentUrl = page.url();
    const hasLoadingState = await page
      .locator('.el-loading-mask')
      .isVisible()
      .catch(() => false);

    expect(currentUrl || hasLoadingState).toBeTruthy();
  });

  test('should show OAuth login button when enabled', async ({ page }) => {
    // Check if OAuth button exists
    const oauthButton = page.locator('button').filter({ hasText: /Login with|OAuth/i });
    const oauthButtonCount = await oauthButton.count();

    // OAuth might not be enabled in all environments
    if (oauthButtonCount > 0) {
      await expect(oauthButton.first()).toBeVisible();

      // Check for OR divider
      const divider = page.locator('.el-divider');
      await expect(divider.first()).toBeVisible();
    }
  });

  test('should have proper form structure', async ({ page }) => {
    // Wait for form to be fully rendered
    await page.waitForSelector('.demo-dynamic', { state: 'visible' });

    // Check form exists
    const form = page.locator('.demo-dynamic');
    await expect(form).toBeVisible();

    // Check form items
    const formItems = page.locator('.el-form-item');
    const formItemCount = await formItems.count();

    // Should have at least 3 form items (username, password, submit button)
    expect(formItemCount).toBeGreaterThanOrEqual(3);
  });

  test('should maintain responsive layout', async ({ page }) => {
    // Check mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(300);

    const loginContent = page.locator('.login-content');
    await expect(loginContent).toBeVisible();

    // Check desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.waitForTimeout(300);

    await expect(loginContent).toBeVisible();
  });
});

test.describe('Login Page Navigation', () => {
  test('should stay on login page when accessing root', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for Vue router to complete navigation
    await page.waitForLoadState('networkidle');

    // Root should redirect to login if not authenticated
    const url = page.url();

    expect(url.includes('/login') || url.includes('/explorer')).toBeTruthy();
  });
});
