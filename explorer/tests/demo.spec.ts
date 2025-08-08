import { test, expect } from 'playwright/test';

test.describe('registerForward', () => {
	test('my test', async ({ page }) => {
		// ...
		await page.goto('http://localhost:6060/register');
		console.log('Page URL:', page.url());
		expect(page.url()).toBe('http://localhost:6060/login');
	});
});
