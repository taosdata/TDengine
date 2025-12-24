# Explorer UI Tests

This directory contains end-to-end UI tests for the taosX Explorer application using Playwright.

## Test Files

- **login.spec.ts** - Comprehensive login page UI tests

## Prerequisites

1. Node.js and pnpm installed
2. Playwright installed (should be in devDependencies)
3. Explorer application running (the tests expect it to be available at `http://localhost:6060`)

## Running Tests

### Install dependencies

```bash
pnpm install
pnpm exec playwright install-deps
pnpm exec playwright install
```

### Run all tests
```bash
pnpm exec playwright test
```

### Run only login tests
```bash
pnpm exec playwright test login.spec.ts
```

### Run tests in headed mode (see browser)
```bash
pnpm exec playwright test --headed
```

### Run tests with UI mode (interactive)
```bash
pnpm exec playwright test --ui
```

### Run a single test by name
```bash
# Match by full or partial test name
pnpm exec playwright test -g "should display login page"

# Run with headed mode to see the browser
pnpm exec playwright test -g "should display login page" --headed

# Run with debug mode (opens inspector)
pnpm exec playwright test -g "should display login page" --debug
```

### Run by line number
```bash
# Run test at specific line in file
pnpm exec playwright test login.spec.ts:29
```

### Run by describe block
```bash
# Run all tests in a describe block
pnpm exec playwright test -g "Login Page"
pnpm exec playwright test -g "Login Page Navigation"
```

### Run tests and generate HTML report
```bash
pnpm exec playwright test
pnpm exec playwright show-report
```

## Quick Reference - Running Single Tests

Here are common commands for running single tests from the login suite:

```bash
# Validation tests
pnpm exec playwright test -g "empty username"
pnpm exec playwright test -g "empty password"

# Interaction tests
pnpm exec playwright test -g "toggle password"
pnpm exec playwright test -g "language switching"
pnpm exec playwright test -g "Enter key"

# OAuth test
pnpm exec playwright test -g "OAuth"

# Navigation tests
pnpm exec playwright test -g "register to login"
pnpm exec playwright test -g "accessing root"

# Responsive layout test
pnpm exec playwright test -g "responsive layout"
```

## Login Test Coverage

The `login.spec.ts` file includes comprehensive tests for:

### Basic UI Elements
- ✓ Display of login form with username and password fields
- ✓ Visibility of sign-in button
- ✓ Display of welcome content and article
- ✓ Copyright information display

### Form Validation
- ✓ Empty username validation error
- ✓ Empty password validation error
- ✓ Proper form structure with all required elements

### User Interactions
- ✓ Login attempt with valid credential format
- ✓ Password visibility toggle
- ✓ Enter key press in password field
- ✓ Username whitespace trimming

### Additional Features
- ✓ Language switching functionality
- ✓ OAuth login button display (when enabled)
- ✓ Responsive layout on different screen sizes

### Navigation
- ✓ Register page redirect to login
- ✓ Root path behavior

## Test Configuration

The Playwright configuration is defined in `playwright.config.ts` at the project root:

- Base URL: `http://localhost:6060`
- Browser: Chrome (Desktop)
- Parallel execution: Disabled
- Reporter: HTML
- Web server: Starts dev server automatically on port 8080

## Writing New Tests

To add new tests:

1. Create a new `.spec.ts` file in the `tests/` directory
2. Import test utilities:
   ```typescript
   import { test, expect } from 'playwright/test';
   ```
3. Write your test cases using `test.describe()` and `test()` blocks
4. Use Playwright's locator API to interact with elements
5. Run the tests to verify they work

### Waiting for Vue to Mount

Since Explorer is a Vue 3 application, tests must wait for Vue to mount and render components before interacting with them. Here are the recommended approaches:

#### 1. Wait for Network Idle (Recommended)
```typescript
test.beforeEach(async ({ page }) => {
  await page.goto('/login', { waitUntil: 'networkidle' });
  
  // Wait for Vue to render key components
  await page.waitForSelector('.login-content', { state: 'visible' });
});
```

#### 2. Wait for Specific Elements
```typescript
test('should wait for Vue components', async ({ page }) => {
  await page.goto('/login');
  
  // Wait for Vue-rendered element
  await page.waitForSelector('.login-content', { state: 'visible' });
  
  // Now safe to interact
  const usernameInput = page.locator('input[placeholder*="username"]');
  await expect(usernameInput).toBeVisible();
});
```

#### 3. Wait for Load State
```typescript
// Wait for Vue router navigation to complete
await page.waitForLoadState('networkidle');

// Wait for DOM to be ready
await page.waitForLoadState('domcontentloaded');
```

#### 4. Combined Approach (Most Reliable)
```typescript
test('robust Vue mounting wait', async ({ page }) => {
  // Navigate with network idle
  await page.goto('/your-page', { waitUntil: 'networkidle' });
  
  // Wait for Vue app element
  await page.waitForSelector('#app', { state: 'attached' });
  
  // Wait for specific component
  await page.waitForSelector('.your-component', { state: 'visible' });
  
  // Small buffer for animations
  await page.waitForTimeout(300);
});
```

### Example Test Structure

```typescript
import { test, expect } from 'playwright/test';

test.describe('Feature Name', () => {
  test.beforeEach(async ({ page }) => {
    // Setup before each test with Vue mounting wait
    await page.goto('/your-page', { waitUntil: 'networkidle' });
    
    // Wait for Vue to render main content
    await page.waitForSelector('.main-content', { state: 'visible' });
  });

  test('should do something', async ({ page }) => {
    // Your test logic here
    const element = page.locator('.some-class');
    await expect(element).toBeVisible();
  });
});
```

## Debugging Tests

### Run in debug mode
```bash
pnpm exec playwright test --debug
```

### Run with trace
```bash
pnpm exec playwright test --trace on
```

### View trace file
```bash
pnpm exec playwright show-trace trace.zip
```

## CI/CD Integration

The tests are configured to work in CI environments:
- Retries: 2 times on CI
- Workers: 1 (no parallel execution on CI)
- `forbidOnly`: Prevents accidental `.only()` calls

## Troubleshooting

### Tests fail with "Cannot find package 'playwright'"
Run `pnpm install` to ensure all dependencies are installed.

### Tests timeout or fail to connect
Ensure the Explorer dev server is running on port 8080 or configure the `webServer` in `playwright.config.ts`.

### Element not found errors
- Check if the element selector has changed in the UI
- Use Playwright Inspector (`--debug` flag) to inspect elements
- Verify the page has fully loaded before interacting
- **Add proper waits for Vue mounting** - see "Waiting for Vue to Mount" section above
- Use `waitForSelector()` before interacting with Vue-rendered elements

### Browser doesn't close
Check for hanging promises or missing `await` statements in your tests.

### Tests are flaky or intermittently fail
- Increase timeouts for network operations: `{ timeout: 10000 }`
- Always wait for Vue components to mount using `waitForSelector()`
- Use `waitUntil: 'networkidle'` when navigating to pages
- Avoid using `waitForTimeout()` - prefer `waitForSelector()` or `waitForLoadState()`

## Resources

- [Playwright Documentation](https://playwright.dev/)
- [Playwright Best Practices](https://playwright.dev/docs/best-practices)
- [Playwright Test API](https://playwright.dev/docs/api/class-test)
