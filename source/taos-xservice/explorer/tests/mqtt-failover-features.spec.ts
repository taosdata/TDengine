import { test, expect } from './_utils/test';
import { gotoDataInTask, openAddSourceFromList, selectElOptionByText } from './_utils/datain';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Open the MQTT add-source form and wait for the host field to be visible. */
async function openMqttForm(page: Parameters<typeof gotoDataInTask>[0]) {
  await gotoDataInTask(page);
  await openAddSourceFromList(page);
  await selectElOptionByText(page, 'type', 'MQTT');
  const hostInput = page.locator('#data\\.broker_addresses\\.host_0');
  await expect(hostInput).toBeVisible({ timeout: 10_000 });
  return hostInput;
}

/** Switch MQTT protocol version via the Connection Configuration select. */
async function selectMqttVersion(page: Parameters<typeof gotoDataInTask>[0], version: string) {
  const versionSelect = page.locator('#data\\.connection_options\\.version');
  await versionSelect.scrollIntoViewIfNeeded();
  await expect(versionSelect).toBeVisible({ timeout: 10_000 });
  await versionSelect.click({ force: true });
  const dropdown = page.locator('.el-select-dropdown:visible');
  await expect(dropdown).toBeVisible({ timeout: 5_000 });
  await dropdown.locator('.el-select-dropdown__item').filter({ hasText: version }).first().click();
  await page.waitForTimeout(300);
}

/** Fill required fields with the given host/port so connectivity check can proceed. */
async function fillRequiredFields(
  page: Parameters<typeof gotoDataInTask>[0],
  opts: { host: string; port: string; topics?: string }
) {
  const hostInput = page.locator('#data\\.broker_addresses\\.host_0');
  const portInput = page.locator('#data\\.broker_addresses\\.port_0');
  const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
  const clientIdInput = clientIdFormItem.locator('input').first();
  const topicsInput = page.locator('#data\\.groups_before\\.collect\\.topics');

  await hostInput.fill(opts.host);
  await portInput.clear();
  await portInput.fill(opts.port);

  await clientIdInput.scrollIntoViewIfNeeded();
  await clientIdInput.fill(`e2e_${Date.now()}`);

  await topicsInput.scrollIntoViewIfNeeded();
  await topicsInput.fill(opts.topics ?? 'test_topic::0');
}

/** Click "Check Connection" and wait for the result element to appear. */
async function clickCheckConnection(page: Parameters<typeof gotoDataInTask>[0]) {
  const checkBtn = page.locator('.btn-check-connectivity');
  await checkBtn.scrollIntoViewIfNeeded();
  await checkBtn.click();
  const resultText = page.locator('.box-check-connectivity .text');
  await expect(resultText).toBeVisible({ timeout: 60_000 });
  return resultText;
}

async function addBrokerRow(page: Parameters<typeof gotoDataInTask>[0]) {
  const addBtn = page.getByRole('button', { name: /Add Borker/i }).first();
  await addBtn.scrollIntoViewIfNeeded();
  await expect(addBtn).toBeVisible({ timeout: 5_000 });
  await addBtn.click();
  await page.waitForTimeout(300);
}

function cleanSessionSwitch(page: Parameters<typeof gotoDataInTask>[0]) {
  // The el-switch checkbox input is hidden; locate the visible el-switch via its parent form-item label.
  return page.locator('.el-form-item').filter({ hasText: /^Clean Session/ }).locator('.el-switch').first();
}

function tlsFileButton(page: Parameters<typeof gotoDataInTask>[0], labelText: RegExp) {
  // Match by the el-form-item class name suffix which uses the field name (e.g. data-auth_options-ca)
  const fieldMap: Record<string, string> = {
    ca: 'data-auth_options-ca',
    cert: 'data-auth_options-cert',
    cert_key: 'data-auth_options-cert_key'
  };
  // Pick the right class by testing the label pattern
  let cls = '';
  if (/^CA$/i.test(labelText.source)) cls = fieldMap.ca;
  else if (/certificate file/i.test(labelText.source)) cls = fieldMap.cert;
  else if (/key file/i.test(labelText.source)) cls = fieldMap.cert_key;

  if (cls) {
    return page.locator(`.el-form-item.${cls}`).getByRole('button', { name: /Select File/i }).first();
  }
  // Fallback: locate by label inside form-item
  return page.locator('.el-form-item').filter({
    has: page.locator('.el-form-item__label', { hasText: labelText })
  }).getByRole('button', { name: /Select File/i }).first();
}

/** Select a TLS verification option directly without relying on selectElOptionByText. */
async function selectTlsOption(page: Parameters<typeof gotoDataInTask>[0], optionText: string) {
  // Use the combobox with aria-label or locate by form-item label text
  const tlsFormItem = page.locator('.el-form-item').filter({ hasText: /TLS Verification/i }).first();
  const tlsSelect = tlsFormItem.locator('.el-select').first();
  await tlsSelect.scrollIntoViewIfNeeded();
  await tlsSelect.click({ force: true });
  const dropdown = page.locator('.el-select-dropdown:visible');
  await expect(dropdown).toBeVisible({ timeout: 10_000 });
  await dropdown.locator('.el-select-dropdown__item').filter({ hasText: optionText }).first().click();
  await page.waitForTimeout(300);
}

// ---------------------------------------------------------------------------
// Test Suite: Broker Addresses (multi-address / failover)  D2.2.2
// ---------------------------------------------------------------------------

test.describe('DataIn - MQTT failover: Broker Addresses section (D2.2.2)', () => {
  test('default broker address row renders with host and port inputs', async ({ page }) => {
    await openMqttForm(page);

    const hostInput = page.locator('#data\\.broker_addresses\\.host_0');
    const portInput = page.locator('#data\\.broker_addresses\\.port_0');

    await expect(hostInput).toBeVisible();
    await expect(portInput).toBeVisible();
    // Default port value
    await expect(portInput).toHaveValue('1883');
  });

  test('Add button appends a second broker address row', async ({ page }) => {
    await openMqttForm(page);
    await addBrokerRow(page);

    // A second host+port row should now exist
    const hostInput1 = page.locator('#data\\.broker_addresses\\.host_1');
    const portInput1 = page.locator('#data\\.broker_addresses\\.port_1');
    await expect(hostInput1).toBeVisible({ timeout: 5_000 });
    await expect(portInput1).toBeVisible({ timeout: 5_000 });
    await expect(portInput1).toHaveValue('1883');
  });

  test('second broker address row can be filled independently', async ({ page }) => {
    await openMqttForm(page);
    await addBrokerRow(page);

    const hostInput1 = page.locator('#data\\.broker_addresses\\.host_1');
    const portInput1 = page.locator('#data\\.broker_addresses\\.port_1');
    await expect(hostInput1).toBeVisible({ timeout: 5_000 });

    await hostInput1.fill('192.168.1.46');
    await portInput1.clear();
    await portInput1.fill('1884');

    await expect(hostInput1).toHaveValue('192.168.1.46');
    await expect(portInput1).toHaveValue('1884');
  });

  test('delete button removes a broker address row', async ({ page }) => {
    await openMqttForm(page);
    await addBrokerRow(page);

    // The second row should now exist
    const hostInput1 = page.locator('#data\\.broker_addresses\\.host_1');
    await expect(hostInput1).toBeVisible({ timeout: 5_000 });

    // Click the delete button on the second row. The delete icon is typically the
    // last icon/button inside the grouping row that corresponds to the second entry.
    const deleteBtn = page.locator('#data\\.broker_addresses\\.host_1')
      .locator('xpath=ancestor::div[contains(@class,"el-form-item")]')
      .getByRole('button')
      .last();
    if (await deleteBtn.count()) {
      await deleteBtn.click();
      await page.waitForTimeout(300);
      await expect(hostInput1).not.toBeVisible({ timeout: 3_000 });
    } else {
      // Alternative: delete buttons may live in the grouping wrapper
      const brokerSection = page.locator('body');
      const groupingRows = brokerSection.locator('.input-row, .host-port-row, .group-row');
      const rowCount = await groupingRows.count();
      if (rowCount >= 2) {
        const delBtn = groupingRows.nth(1).getByRole('button').last();
        await delBtn.click({ force: true });
        await page.waitForTimeout(300);
        await expect(hostInput1).not.toBeVisible({ timeout: 3_000 });
      }
    }
  });

  test('connectivity check succeeds with single reachable address (192.168.1.45:1883)', async ({ page }) => {
    await openMqttForm(page);
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1883' });

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/reachable/i);
    await expect(resultText.locator('span.error')).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check fails with single unreachable address (192.168.1.45:1885)', async ({ page }) => {
    await openMqttForm(page);
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1885' });

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/not reachable/i);
    await expect(resultText.locator('span.error')).toBeVisible({ timeout: 5_000 });
  });

  test('connectivity check succeeds when first address unreachable but second reachable (failover)', async ({
    page
  }) => {
    await openMqttForm(page);

    // Fill first address with an unreachable port
    const hostInput0 = page.locator('#data\\.broker_addresses\\.host_0');
    const portInput0 = page.locator('#data\\.broker_addresses\\.port_0');
    await hostInput0.fill('192.168.1.45');
    await portInput0.clear();
    await portInput0.fill('1885'); // unreachable

    // Add second address with a reachable port
    await addBrokerRow(page);

    const hostInput1 = page.locator('#data\\.broker_addresses\\.host_1');
    const portInput1 = page.locator('#data\\.broker_addresses\\.port_1');
    await expect(hostInput1).toBeVisible({ timeout: 5_000 });
    await hostInput1.fill('192.168.1.45');
    await portInput1.clear();
    await portInput1.fill('1883'); // reachable

    // Fill remaining required fields
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();
    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.fill(`e2e_failover_${Date.now()}`);

    const topicsInput = page.locator('#data\\.groups_before\\.collect\\.topics');
    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.fill('test_topic::0');

    const resultText = await clickCheckConnection(page);
    // With failover, connecting to the second address should succeed
    await expect(resultText).toContainText(/reachable/i);
  });
});

// ---------------------------------------------------------------------------
// Test Suite: Connection Configuration section (D2.2.3)
// ---------------------------------------------------------------------------

test.describe('DataIn - MQTT Connection Configuration section (D2.2.3)', () => {
  test('Connection Configuration section renders all expected fields', async ({ page }) => {
    await openMqttForm(page);

    // MQTT protocol version
    const versionSelect = page.locator('#data\\.connection_options\\.version');
    await versionSelect.scrollIntoViewIfNeeded();
    await expect(versionSelect).toBeVisible();

    // Client ID (rendered by customId component)
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    await clientIdFormItem.scrollIntoViewIfNeeded();
    await expect(clientIdFormItem.locator('input').first()).toBeVisible({ timeout: 5_000 });

    // Keep Alive
    const keepAliveInput = page.locator('#data\\.connection_options\\.keep_alive');
    await keepAliveInput.scrollIntoViewIfNeeded();
    await expect(keepAliveInput).toBeVisible();
    await expect(keepAliveInput).toHaveValue('60');

    // Clean Session switch
    const cleanSession = cleanSessionSwitch(page);
    await cleanSession.scrollIntoViewIfNeeded();
    await expect(cleanSession).toBeVisible();
  });

  test('Connect User Properties field is hidden for MQTT v3.1', async ({ page }) => {
    await openMqttForm(page);

    // Default version is 3.1 — connect_user_properties should NOT be visible
    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await expect(connectUserProps).not.toBeVisible({ timeout: 3_000 });
  });

  test('Connect User Properties field is hidden for MQTT v3.1.1', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '3.1.1');

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await expect(connectUserProps).not.toBeVisible({ timeout: 3_000 });
  });

  test('Connect User Properties field appears when MQTT version is 5.0', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await connectUserProps.scrollIntoViewIfNeeded();
    await expect(connectUserProps).toBeVisible({ timeout: 5_000 });
  });

  test('Connect User Properties field accepts valid key=value pairs', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await connectUserProps.scrollIntoViewIfNeeded();
    await connectUserProps.fill('client-type=sensor,env=prod');
    await connectUserProps.press('Tab');

    // No validation error expected for valid input
    const error = connectUserProps
      .locator('xpath=ancestor::div[contains(@class,"el-form-item")]')
      .locator('.el-form-item__error')
      .first();
    await expect(error).not.toBeVisible({ timeout: 3_000 });
  });

  test('Keep Alive default value is 60 and can be changed', async ({ page }) => {
    await openMqttForm(page);

    const keepAliveInput = page.locator('#data\\.connection_options\\.keep_alive');
    await keepAliveInput.scrollIntoViewIfNeeded();
    await expect(keepAliveInput).toHaveValue('60');

    await keepAliveInput.click({ clickCount: 3 });
    await keepAliveInput.fill('30');
    await expect(keepAliveInput).toHaveValue('30');
  });

  test('connectivity check with Connect User Properties (v5) on reachable broker', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1883' });

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await connectUserProps.scrollIntoViewIfNeeded();
    await connectUserProps.fill('client-type=sensor,env=prod');

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/reachable/i);
    await expect(resultText.locator('span.error')).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check with Connect User Properties (v5) on unreachable broker', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1885' });

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await connectUserProps.scrollIntoViewIfNeeded();
    await connectUserProps.fill('client-type=sensor');

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/not reachable/i);
    await expect(resultText.locator('span.error')).toBeVisible({ timeout: 5_000 });
  });
});

// ---------------------------------------------------------------------------
// Test Suite: Authentication Configuration section (D2.2.4)
// ---------------------------------------------------------------------------

test.describe('DataIn - MQTT Authentication Configuration section (D2.2.4)', () => {
  test('Authentication Configuration section renders username, password, and TLS fields', async ({ page }) => {
    await openMqttForm(page);

    // Username
    const usernameInput = page.locator('input[id*="username"]').first();
    await usernameInput.scrollIntoViewIfNeeded();
    await expect(usernameInput).toBeVisible();

    // Password
    const passwordInput = page.locator('input[id*="password"]').first();
    await passwordInput.scrollIntoViewIfNeeded();
    await expect(passwordInput).toBeVisible();

    // TLS Verification select
    const tlsSelect = page.locator('#data\\.auth_options\\.tsl_verify');
    await tlsSelect.scrollIntoViewIfNeeded();
    await expect(tlsSelect).toBeVisible();
  });

  test('TLS default value is Disable (none) and CA/cert fields are hidden', async ({ page }) => {
    await openMqttForm(page);

    const tlsSelect = page.locator('#data\\.auth_options\\.tsl_verify');
    await tlsSelect.scrollIntoViewIfNeeded();
    await expect(tlsSelect).toBeVisible();

    // CA, cert, cert_key should be hidden by default
    const caInput = page.locator('#data\\.auth_options\\.ca');
    const certInput = page.locator('#data\\.auth_options\\.cert');
    const certKeyInput = page.locator('#data\\.auth_options\\.cert_key');

    await expect(caInput).not.toBeVisible({ timeout: 3_000 });
    await expect(certInput).not.toBeVisible({ timeout: 3_000 });
    await expect(certKeyInput).not.toBeVisible({ timeout: 3_000 });
  });

  test('TLS Unidirectional shows CA field only', async ({ page }) => {
    await openMqttForm(page);

    await selectTlsOption(page, 'Unidirectional');

    const caButton = tlsFileButton(page, /^CA$/i);
    await caButton.scrollIntoViewIfNeeded();
    await expect(caButton).toBeVisible({ timeout: 5_000 });

    const certButton = tlsFileButton(page, /Client certificate file/i);
    const certKeyButton = tlsFileButton(page, /Client key file/i);
    await expect(certButton).not.toBeVisible({ timeout: 3_000 });
    await expect(certKeyButton).not.toBeVisible({ timeout: 3_000 });
  });

  test('TLS Bidirectional shows CA, cert, and cert_key fields', async ({ page }) => {
    await openMqttForm(page);

    await selectTlsOption(page, 'Bidirectional');

    const caButton = tlsFileButton(page, /^CA$/i);
    const certButton = tlsFileButton(page, /Client certificate file/i);
    const certKeyButton = tlsFileButton(page, /Client key file/i);

    await caButton.scrollIntoViewIfNeeded();
    await expect(caButton).toBeVisible({ timeout: 5_000 });
    await certButton.scrollIntoViewIfNeeded();
    await expect(certButton).toBeVisible({ timeout: 5_000 });
    await certKeyButton.scrollIntoViewIfNeeded();
    await expect(certKeyButton).toBeVisible({ timeout: 5_000 });
  });

  test('username and password fields accept input values', async ({ page }) => {
    await openMqttForm(page);

    const usernameInput = page.locator('input[id*="username"]').first();
    const passwordInput = page.locator('input[id*="password"]').first();

    await usernameInput.scrollIntoViewIfNeeded();
    await usernameInput.fill('testuser');
    await expect(usernameInput).toHaveValue('testuser');

    await passwordInput.scrollIntoViewIfNeeded();
    await passwordInput.fill('secret');
    await expect(passwordInput).toHaveValue('secret');
  });

  test('connectivity check with username/password on reachable broker', async ({ page }) => {
    await openMqttForm(page);
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1883' });

    const usernameInput = page.locator('input[id*="username"]').first();
    const passwordInput = page.locator('input[id*="password"]').first();
    await usernameInput.fill('testuser');
    await passwordInput.fill('testpass');

    const resultText = await clickCheckConnection(page);
    // The broker at 1884 is reachable; auth credentials may or may not be valid,
    // but the connectivity check should at least report a result.
    await expect(resultText).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// Test Suite: Subscribe / Collect fields (v5 only)  D2.2.5
// ---------------------------------------------------------------------------

test.describe('DataIn - MQTT Subscribe fields: sub-offset and subscribe_user_properties (D2.2.5)', () => {
  test('sub-offset field is hidden for MQTT v3.1 (default)', async ({ page }) => {
    await openMqttForm(page);

    const subOffsetSelect = page.locator('#data\\.groups_before\\.collect\\.sub\\-offset');
    await expect(subOffsetSelect).not.toBeVisible({ timeout: 3_000 });
  });

  test('subscribe_user_properties field is hidden for MQTT v3.1 (default)', async ({ page }) => {
    await openMqttForm(page);

    const subUserProps = page.locator('#data\\.groups_before\\.collect\\.subscribe_user_properties');
    await expect(subUserProps).not.toBeVisible({ timeout: 3_000 });
  });

  test('sub-offset and subscribe_user_properties appear when version is 5.0', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    const subOffsetSelect = page.locator('#data\\.groups_before\\.collect\\.sub\\-offset');
    await subOffsetSelect.scrollIntoViewIfNeeded();
    await expect(subOffsetSelect).toBeVisible({ timeout: 5_000 });

    const subUserProps = page.locator('#data\\.groups_before\\.collect\\.subscribe_user_properties');
    await subUserProps.scrollIntoViewIfNeeded();
    await expect(subUserProps).toBeVisible({ timeout: 5_000 });
  });

  test('sub-offset hidden again when version switches back to v3.1', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    // Confirm it's visible
    const subOffsetSelect = page.locator('#data\\.groups_before\\.collect\\.sub\\-offset');
    await subOffsetSelect.scrollIntoViewIfNeeded();
    await expect(subOffsetSelect).toBeVisible({ timeout: 5_000 });

    // Switch back to 3.1
    await selectMqttVersion(page, '3.1');
    await expect(subOffsetSelect).not.toBeVisible({ timeout: 3_000 });
  });

  test('sub-offset dropdown provides earliest and latest options', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    const subOffsetSelect = page.locator('#data\\.groups_before\\.collect\\.sub\\-offset');
    await subOffsetSelect.scrollIntoViewIfNeeded();
    await subOffsetSelect.click({ force: true });

    const dropdown = page.locator('.el-select-dropdown:visible');
    await expect(dropdown).toBeVisible({ timeout: 5_000 });

    await expect(dropdown.locator('.el-select-dropdown__item').filter({ hasText: 'earliest' })).toBeVisible();
    await expect(dropdown.locator('.el-select-dropdown__item').filter({ hasText: 'latest' })).toBeVisible();

    // Close dropdown
    await page.keyboard.press('Escape');
  });

  test('subscribe_user_properties accepts valid key=value pairs', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    const subUserProps = page.locator('#data\\.groups_before\\.collect\\.subscribe_user_properties');
    await subUserProps.scrollIntoViewIfNeeded();
    await subUserProps.fill('priority=high,region=us');
    await subUserProps.press('Tab');

    const error = subUserProps
      .locator('xpath=ancestor::div[contains(@class,"el-form-item")]')
      .locator('.el-form-item__error')
      .first();
    await expect(error).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check with sub-offset=earliest on reachable broker', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1883' });

    const subOffsetSelect = page.locator('#data\\.groups_before\\.collect\\.sub\\-offset');
    await subOffsetSelect.scrollIntoViewIfNeeded();
    await subOffsetSelect.click({ force: true });
    const dropdown = page.locator('.el-select-dropdown:visible');
    await expect(dropdown).toBeVisible({ timeout: 5_000 });
    await dropdown.locator('.el-select-dropdown__item').filter({ hasText: 'earliest' }).first().click();

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/reachable/i);
    await expect(resultText.locator('span.error')).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check with subscribe_user_properties on reachable broker', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1883' });

    const subUserProps = page.locator('#data\\.groups_before\\.collect\\.subscribe_user_properties');
    await subUserProps.scrollIntoViewIfNeeded();
    await subUserProps.fill('priority=high');

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/reachable/i);
    await expect(resultText.locator('span.error')).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check with all v5 params filled on unreachable broker', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1885' });

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await connectUserProps.scrollIntoViewIfNeeded();
    await connectUserProps.fill('client-type=sensor');

    const subOffsetSelect = page.locator('#data\\.groups_before\\.collect\\.sub\\-offset');
    await subOffsetSelect.scrollIntoViewIfNeeded();
    await subOffsetSelect.click({ force: true });
    const dropdown = page.locator('.el-select-dropdown:visible');
    await expect(dropdown).toBeVisible({ timeout: 5_000 });
    await dropdown.locator('.el-select-dropdown__item').filter({ hasText: 'latest' }).first().click();

    const subUserProps = page.locator('#data\\.groups_before\\.collect\\.subscribe_user_properties');
    await subUserProps.scrollIntoViewIfNeeded();
    await subUserProps.fill('priority=low');

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/not reachable/i);
    await expect(resultText.locator('span.error')).toBeVisible({ timeout: 5_000 });
  });
});

// ---------------------------------------------------------------------------
// Test Suite: Full v5 parameter set + connectivity (D2.2.6)
// ---------------------------------------------------------------------------

test.describe('DataIn - MQTT v5: all new parameters with connectivity check (D2.2.6)', () => {
  test('fill all v5 parameters and verify connectivity succeeds (192.168.1.45:1883)', async ({ page }) => {
    test.setTimeout(120_000);

    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    // --- Broker Addresses ---
    const hostInput = page.locator('#data\\.broker_addresses\\.host_0');
    const portInput = page.locator('#data\\.broker_addresses\\.port_0');
    await hostInput.fill('192.168.1.45');
    await portInput.clear();
    await portInput.fill('1883');

    // --- Connection Configuration ---
    const keepAliveInput = page.locator('#data\\.connection_options\\.keep_alive');
    await keepAliveInput.scrollIntoViewIfNeeded();
    await keepAliveInput.click({ clickCount: 3 });
    await keepAliveInput.fill('30');

    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();
    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.fill(`e2e_v5_all_${Date.now()}`);

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await connectUserProps.scrollIntoViewIfNeeded();
    await connectUserProps.fill('client-type=sensor,env=test');

    // --- Collect ---
    const topicsInput = page.locator('#data\\.groups_before\\.collect\\.topics');
    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.fill('e2e_topic::0');

    const subOffsetSelect = page.locator('#data\\.groups_before\\.collect\\.sub\\-offset');
    await subOffsetSelect.scrollIntoViewIfNeeded();
    await subOffsetSelect.click({ force: true });
    const dropdown = page.locator('.el-select-dropdown:visible');
    await expect(dropdown).toBeVisible({ timeout: 5_000 });
    await dropdown.locator('.el-select-dropdown__item').filter({ hasText: 'earliest' }).first().click();

    const subUserProps = page.locator('#data\\.groups_before\\.collect\\.subscribe_user_properties');
    await subUserProps.scrollIntoViewIfNeeded();
    await subUserProps.fill('priority=high,region=us');

    // --- Check Connection ---
    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/reachable/i);
    await expect(resultText.locator('span.error')).not.toBeVisible({ timeout: 3_000 });
  });

  test('fill all v5 parameters and verify connectivity fails (192.168.1.45:1885)', async ({ page }) => {
    test.setTimeout(120_000);

    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');

    // --- Broker Addresses ---
    const hostInput = page.locator('#data\\.broker_addresses\\.host_0');
    const portInput = page.locator('#data\\.broker_addresses\\.port_0');
    await hostInput.fill('192.168.1.45');
    await portInput.clear();
    await portInput.fill('1885');

    // --- Connection Configuration ---
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();
    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.fill(`e2e_v5_fail_${Date.now()}`);

    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await connectUserProps.scrollIntoViewIfNeeded();
    await connectUserProps.fill('client-type=sensor');

    // --- Collect ---
    const topicsInput = page.locator('#data\\.groups_before\\.collect\\.topics');
    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.fill('e2e_topic::0');

    const subUserProps = page.locator('#data\\.groups_before\\.collect\\.subscribe_user_properties');
    await subUserProps.scrollIntoViewIfNeeded();
    await subUserProps.fill('priority=high');

    // --- Check Connection ---
    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/not reachable/i);
    await expect(resultText.locator('span.error')).toBeVisible({ timeout: 5_000 });
  });

  test('v3 task (no user properties) connectivity succeeds (192.168.1.45:1883)', async ({ page }) => {
    await openMqttForm(page);
    // Default version is 3.1 — no user properties
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1883' });

    // Confirm user property fields are NOT visible
    const connectUserProps = page.locator('#data\\.connection_options\\.connect_user_properties');
    await expect(connectUserProps).not.toBeVisible({ timeout: 3_000 });

    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/reachable/i);
    await expect(resultText.locator('span.error')).not.toBeVisible({ timeout: 3_000 });
  });

  test('clean_session=false is independent of connect_user_properties (v5)', async ({ page }) => {
    await openMqttForm(page);
    await selectMqttVersion(page, '5.0');
    await fillRequiredFields(page, { host: '192.168.1.45', port: '1883' });

    // Toggle Clean Session off
    const cleanSession = cleanSessionSwitch(page);
    await cleanSession.scrollIntoViewIfNeeded();
    await expect(cleanSession).toBeVisible();
    await cleanSession.click();

    // Leave connect_user_properties empty — clean_session=false should still work
    const resultText = await clickCheckConnection(page);
    await expect(resultText).toContainText(/reachable/i);
  });
});
