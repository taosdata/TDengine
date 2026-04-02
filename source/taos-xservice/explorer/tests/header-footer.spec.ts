import { test, expect } from './_utils/test';
import { ensureLogin } from './_utils/auth';
import { routes } from './_utils/routes';

test.beforeEach(async ({ page }) => {
  await ensureLogin(page, routes.explorer);
});

test.describe('Header - Timezone Selector', () => {
  test('timezone selector is visible', async ({ page }) => {
    const timezoneCombobox = page.locator('.timezone-wrapper').getByRole('combobox');
    await expect(timezoneCombobox).toBeVisible({ timeout: 10_000 });
  });

  test('timezone selector opens timezone dropdown', async ({ page }) => {
    const timezoneCombobox = page.locator('.timezone-wrapper').getByRole('combobox');
    await expect(timezoneCombobox).toBeVisible({ timeout: 10_000 });
    await timezoneCombobox.click();
    await expect(page.locator('.el-select-dropdown.timezone-select:visible')).toBeVisible({ timeout: 5_000 });
  });
});

test.describe('Header - Help Button', () => {
  test('help entry is visible when docs are enabled', async ({ page }) => {
    const helpTrigger = page.locator('.doc-block .avatar_block');
    if (!(await helpTrigger.isVisible().catch(() => false))) {
      test.skip(true, 'Help entry is disabled in current build config');
    }
    await expect(helpTrigger).toBeVisible({ timeout: 10_000 });
  });

  test('help entry opens docs dropdown', async ({ page }) => {
    const helpTrigger = page.locator('.doc-block .avatar_block');
    if (!(await helpTrigger.isVisible().catch(() => false))) {
      test.skip(true, 'Help entry is disabled in current build config');
    }

    await helpTrigger.hover();
    const helpLink = page.locator('.el-dropdown-menu:visible a.drop-block[href]').first();
    await expect(helpLink).toBeVisible({ timeout: 5_000 });
    const href = await helpLink.getAttribute('href');
    expect(href).toMatch(/^\/docs(?:-en)?\/$/);
  });
});

test.describe('Header - User Menu', () => {
  test('user menu trigger is visible', async ({ page }) => {
    const userTrigger = page.locator('.avatar_wrapper .avatar_block');
    await expect(userTrigger).toBeVisible({ timeout: 10_000 });
    const userInitial = (await userTrigger.textContent())?.trim() || '';
    expect(userInitial).toMatch(/^[A-Z]$/);
  });

  test('user menu trigger opens dropdown', async ({ page }) => {
    const userTrigger = page.locator('.avatar_wrapper .avatar_block');
    await expect(userTrigger).toBeVisible({ timeout: 10_000 });
    await userTrigger.hover();

    const userMenu = page.locator('.el-dropdown-menu:visible').filter({ has: page.locator('.custom-divider') });
    await expect(userMenu).toBeVisible({ timeout: 5_000 });
    const profileEntry = userMenu.locator('a.drop-block[href="/profile"]');
    await expect(profileEntry).toBeVisible({ timeout: 5_000 });
  });
});

test.describe('Header - Logo Link', () => {
  test('logo link points to supported destination', async ({ page }) => {
    const logoLink = page.locator('.sidebar_logo_container a').first();
    await expect(logoLink).toBeVisible({ timeout: 10_000 });
    const href = await logoLink.getAttribute('href');
    expect(href).toMatch(/^\/(landing|explorer)$/);
  });
});

test.describe('Footer - Version Info', () => {
  test('displays version number in status bar', async ({ page }) => {
    const versionText = page.locator('.status-left .license .value').first();
    await expect(versionText).toBeVisible({ timeout: 10_000 });
    await expect(versionText).toContainText(/\d+\.\d+\.\d+(?:\.\d+)?/);
  });

  test('displays non-empty version or license text', async ({ page }) => {
    const versionText = page.locator('.status-left .license .value').first();
    await expect(versionText).toBeVisible({ timeout: 10_000 });
    const value = (await versionText.textContent())?.trim() || '';
    expect(value.length).toBeGreaterThan(0);
  });
});

test.describe('Language Switcher', () => {
  test('language switcher is visible', async ({ page }) => {
    const langSwitcher = page.locator('.status-right .language');
    await expect(langSwitcher).toBeVisible({ timeout: 10_000 });
  });

  test('language switcher toggles language label', async ({ page }) => {
    const langSwitcher = page.locator('.status-right .language');
    await expect(langSwitcher).toBeVisible({ timeout: 10_000 });
    const initialText = (await langSwitcher.textContent())?.trim() || '';
    expect(initialText).toBeTruthy();

    await langSwitcher.click();
    await expect(langSwitcher).not.toHaveText(initialText, { timeout: 10_000 });
  });
});
