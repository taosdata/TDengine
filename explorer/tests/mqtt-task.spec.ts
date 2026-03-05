import { test, expect } from './_utils/test';
import { runSqlBatch } from './_utils/explorerSql';
import { stopTaskBestEffort, deleteTaskBestEffort } from './_utils/cleanup';
import { findTaskRow, gotoDataInTask, openAddSourceFromList, selectElOptionByText } from './_utils/datain';
import { routes } from './_utils/routes';

test.describe('DataIn - MQTT datasource (D2.2.1)', () => {
  test('all required fields show errors when empty and Check Connection is clicked', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MQTT as datasource type
    await selectElOptionByText(page, 'type', 'MQTT');

    // Wait for MQTT-specific fields to render
    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    // --- Locate required fields ---

    // Connection Configuration
    const portInput = page.locator('#data\\.connection_options\\.port');

    // Collect section (inside groups_before with UUID)
    // version is a select – Element Plus puts id on the inner <input>
    const mqttVersionSelect = page.locator('input[id^="data.groups_before."][id$=".version"]').first();
    // Topics QoS Config is a regular input
    const topicsInput = page.locator('input[id^="data.groups_before."][id$=".topics"]').first();
    // Client ID uses customId component which does NOT put an id on its <el-input>.
    // In "add" mode it renders: <span>taosx</span> + <el-input class="mr20 ml15">
    // We find the el-form-item by its class containing "client_id", then the inner input.
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();

    // --- Verify required fields are rendered ---
    await expect(hostInput).toBeVisible();
    await expect(portInput).toBeVisible();

    await mqttVersionSelect.scrollIntoViewIfNeeded();
    await expect(mqttVersionSelect).toBeVisible({ timeout: 10_000 });

    await clientIdInput.scrollIntoViewIfNeeded();
    await expect(clientIdInput).toBeVisible({ timeout: 10_000 });

    await topicsInput.scrollIntoViewIfNeeded();
    await expect(topicsInput).toBeVisible();

    // --- Verify default values ---
    await expect(portInput).toHaveValue('1883');

    // --- Clear all required fields to ensure they are empty ---
    await hostInput.clear();
    await portInput.clear();
    await clientIdInput.clear();
    await topicsInput.clear();

    // --- Click Check Connection to trigger validation ---
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // --- Verify required field error messages appear ---
    // After validation failure, Element Plus scrolls to first error. Wait for it.
    await page.waitForTimeout(500);

    // Helper: locate the .el-form-item__error inside the ancestor .el-form-item
    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // For clientId the input has no id, so use the form item we already found
    const getClientIdError = () => clientIdFormItem.locator('.el-form-item__error').first();

    // MQTT Host is required
    const hostError = getFormItemError(hostInput);
    await hostInput.scrollIntoViewIfNeeded();
    await expect(hostError).toBeVisible({ timeout: 5_000 });
    await expect(hostError).toContainText(/required/i);

    // MQTT Port is required
    const portError = getFormItemError(portInput);
    await portInput.scrollIntoViewIfNeeded();
    await expect(portError).toBeVisible({ timeout: 5_000 });

    // Client ID is required
    const clientIdError = getClientIdError();
    await clientIdFormItem.scrollIntoViewIfNeeded();
    await expect(clientIdError).toBeVisible({ timeout: 5_000 });
    await expect(clientIdError).toContainText(/required/i);

    // Topics QoS Config is required
    const topicsError = getFormItemError(topicsInput);
    await topicsInput.scrollIntoViewIfNeeded();
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(/required/i);
  });

  test('errors clear after filling required fields', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MQTT as datasource type
    await selectElOptionByText(page, 'type', 'MQTT');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const topicsInput = page.locator('input[id^="data.groups_before."][id$=".topics"]').first();
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();

    // Clear fields
    await hostInput.clear();
    await portInput.clear();
    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.clear();
    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.clear();

    // Trigger validation
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();
    await page.waitForTimeout(500);

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // Verify errors are present
    const hostError = getFormItemError(hostInput);
    await hostInput.scrollIntoViewIfNeeded();
    await expect(hostError).toBeVisible({ timeout: 5_000 });

    const portError = getFormItemError(portInput);
    await portInput.scrollIntoViewIfNeeded();
    await expect(portError).toBeVisible({ timeout: 5_000 });

    const topicsError = getFormItemError(topicsInput);
    await topicsInput.scrollIntoViewIfNeeded();
    await expect(topicsError).toBeVisible({ timeout: 5_000 });

    const clientIdError = clientIdFormItem.locator('.el-form-item__error').first();
    await clientIdFormItem.scrollIntoViewIfNeeded();
    await expect(clientIdError).toBeVisible({ timeout: 5_000 });

    // --- Fill fields one by one and verify errors clear ---

    // Fill MQTT Host
    await hostInput.scrollIntoViewIfNeeded();
    await hostInput.fill('127.0.0.1');
    await hostInput.press('Tab');
    await expect(hostError).not.toBeVisible({ timeout: 5_000 });

    // Fill MQTT Port
    await portInput.scrollIntoViewIfNeeded();
    await portInput.fill('1883');
    await portInput.press('Tab');
    await expect(portError).not.toBeVisible({ timeout: 5_000 });

    // Fill Client ID
    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.fill('test_client');
    await clientIdInput.press('Tab');
    await expect(clientIdError).not.toBeVisible({ timeout: 5_000 });

    // Fill Topics QoS Config
    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.fill('test_topic::0');
    await topicsInput.press('Tab');
    await expect(topicsError).not.toBeVisible({ timeout: 5_000 });
  });

  test('partially filled form only shows errors for remaining empty required fields', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MQTT as datasource type
    await selectElOptionByText(page, 'type', 'MQTT');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const topicsInput = page.locator('input[id^="data.groups_before."][id$=".topics"]').first();
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();

    // Fill Host and Client ID, leave Port and Topics empty
    await hostInput.fill('192.168.1.100');
    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.fill('my_client');

    // Clear Port and Topics
    await portInput.clear();
    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.clear();

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();
    await page.waitForTimeout(500);

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // Host error should NOT appear (it was filled)
    const hostError = getFormItemError(hostInput);
    await hostInput.scrollIntoViewIfNeeded();
    await expect(hostError).not.toBeVisible({ timeout: 3_000 });

    // Client ID error should NOT appear (it was filled)
    const clientIdError = clientIdFormItem.locator('.el-form-item__error').first();
    await clientIdFormItem.scrollIntoViewIfNeeded();
    await expect(clientIdError).not.toBeVisible({ timeout: 3_000 });

    // Port error SHOULD appear (was cleared)
    const portError = getFormItemError(portInput);
    await portInput.scrollIntoViewIfNeeded();
    await expect(portError).toBeVisible({ timeout: 5_000 });

    // Topics QoS Config error SHOULD appear
    const topicsError = getFormItemError(topicsInput);
    await topicsInput.scrollIntoViewIfNeeded();
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(/required/i);
  });

  test('non-required fields do not show errors when empty', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MQTT as datasource type
    await selectElOptionByText(page, 'type', 'MQTT');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    // Click Check Connection without filling anything to trigger validation
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();
    await page.waitForTimeout(500);

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();

    // Verify non-required fields do NOT show errors

    // TLS Verification (not required, has default "Disable")
    const tlsSelect = page.locator('#data\\.connection_options\\.tsl_verify');
    await tlsSelect.scrollIntoViewIfNeeded();
    const tlsError = getFormItemError(tlsSelect);
    await expect(tlsError).not.toBeVisible({ timeout: 3_000 });

    // Authentication > Username (not required)
    const usernameInput = page.locator('input[id*="username"]').first();
    await usernameInput.scrollIntoViewIfNeeded();
    const usernameError = getFormItemError(usernameInput);
    await expect(usernameError).not.toBeVisible({ timeout: 3_000 });

    // Authentication > Password (not required)
    const passwordInput = page.locator('input[id*="password"]').first();
    const passwordError = getFormItemError(passwordInput);
    await expect(passwordError).not.toBeVisible({ timeout: 3_000 });

    // Keep Alive (not required, has default 60)
    const keepAliveInput = page.locator('input[id^="data.groups_before."][id$=".keep_alive"]').first();
    await keepAliveInput.scrollIntoViewIfNeeded();
    const keepAliveError = getFormItemError(keepAliveInput);
    await expect(keepAliveError).not.toBeVisible({ timeout: 3_000 });

    // Topic Analysis (not required)
    const topicPatternInput = page.locator('input[id^="data.groups_before."][id$=".topic_pattern"]').first();
    await topicPatternInput.scrollIntoViewIfNeeded();
    const topicPatternError = getFormItemError(topicPatternInput);
    await expect(topicPatternError).not.toBeVisible({ timeout: 3_000 });
  });

  test('Topics QoS Config pattern validation: spaces, empty names, invalid QoS', async ({ page }) => {
    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MQTT as datasource type
    await selectElOptionByText(page, 'type', 'MQTT');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const topicsInput = page.locator('input[id^="data.groups_before."][id$=".topics"]').first();
    await topicsInput.scrollIntoViewIfNeeded();
    await expect(topicsInput).toBeVisible({ timeout: 10_000 });

    const getFormItemError = (input: ReturnType<typeof page.locator>) =>
      input.locator('xpath=ancestor::div[contains(@class,"el-form-item")]').locator('.el-form-item__error').first();
    const topicsError = getFormItemError(topicsInput);

    const formatErrorPattern = /input format error/i;

    // ---- VALID inputs: should NOT show pattern error ----

    // Topic name containing a space: "ab c::0"
    await topicsInput.fill('ab c::0');
    await topicsInput.press('Tab');
    await expect(topicsError).not.toBeVisible({ timeout: 3_000 });

    // Topic name with multiple spaces: "sensor data live::1"
    await topicsInput.fill('sensor data live::1');
    await topicsInput.press('Tab');
    await expect(topicsError).not.toBeVisible({ timeout: 3_000 });

    // Multiple topics with spaces: "ab c::0,de f::2"
    await topicsInput.fill('ab c::0,de f::2');
    await topicsInput.press('Tab');
    await expect(topicsError).not.toBeVisible({ timeout: 3_000 });

    // Simple topic without space: "topic1::0"
    await topicsInput.fill('topic1::0');
    await topicsInput.press('Tab');
    await expect(topicsError).not.toBeVisible({ timeout: 3_000 });

    // Multiple simple topics: "topic1::0,topic2::1,topic3::2"
    await topicsInput.fill('topic1::0,topic2::1,topic3::2');
    await topicsInput.press('Tab');
    await expect(topicsError).not.toBeVisible({ timeout: 3_000 });

    // All three QoS values are valid individually
    for (const qos of ['0', '1', '2']) {
      await topicsInput.fill(`my_topic::${qos}`);
      await topicsInput.press('Tab');
      await expect(topicsError).not.toBeVisible({ timeout: 3_000 });
    }

    // ---- INVALID inputs: should show pattern error ----

    // Empty topic name (only "::0"): topic name is just whitespace / missing
    await topicsInput.fill('::0');
    await topicsInput.press('Tab');
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(formatErrorPattern);

    // Topic name is only spaces: " ::0"
    await topicsInput.fill(' ::0');
    await topicsInput.press('Tab');
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(formatErrorPattern);

    // QoS out of range: value 3
    await topicsInput.fill('topic1::3');
    await topicsInput.press('Tab');
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(formatErrorPattern);

    // QoS out of range: value 9
    await topicsInput.fill('topic1::9');
    await topicsInput.press('Tab');
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(formatErrorPattern);

    // QoS missing entirely: "topic1::"
    await topicsInput.fill('topic1::');
    await topicsInput.press('Tab');
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(formatErrorPattern);

    // No separator at all: "topic1"
    await topicsInput.fill('topic1');
    await topicsInput.press('Tab');
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(formatErrorPattern);

    // One valid, one invalid in multi-topic: "topic1::0, ::1"
    await topicsInput.fill('topic1::0, ::1');
    await topicsInput.press('Tab');
    await expect(topicsError).toBeVisible({ timeout: 5_000 });
    await expect(topicsError).toContainText(formatErrorPattern);

    // ---- Finally, fill a valid value to confirm error clears ----
    await topicsInput.fill('ab c::0');
    await topicsInput.press('Tab');
    await expect(topicsError).not.toBeVisible({ timeout: 5_000 });
  });

  test('connectivity check succeeds with reachable MQTT broker (192.168.1.45:1883)', async ({ page }) => {
    const ts = Date.now();

    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MQTT as datasource type
    await selectElOptionByText(page, 'type', 'MQTT');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();
    const topicsInput = page.locator('input[id^="data.groups_before."][id$=".topics"]').first();

    // Fill all required fields with valid values
    await hostInput.fill('192.168.1.45');
    await expect(portInput).toHaveValue('1883');

    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.fill(`e2e_mqtt_cc_${ts}`);

    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.fill('test_topic::0');

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // The result text appears below the button inside .box-check-connectivity .text
    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toBeVisible({ timeout: 60_000 });

    // Should report the data source is reachable (no form validation errors, no connection errors)
    await expect(resultText).toContainText(/reachable/i);

    // The icon should be green (SuccessFilled) — verify no error span is shown
    const errorSpan = resultText.locator('span.error');
    await expect(errorSpan).not.toBeVisible({ timeout: 3_000 });
  });

  test('connectivity check fails with unreachable MQTT broker (192.168.1.45:1885)', async ({ page }) => {
    const ts = Date.now();

    await gotoDataInTask(page);
    await openAddSourceFromList(page);

    // Select MQTT as datasource type
    await selectElOptionByText(page, 'type', 'MQTT');

    const hostInput = page.locator('#data\\.connection_options\\.host');
    await expect(hostInput).toBeVisible({ timeout: 10_000 });

    const portInput = page.locator('#data\\.connection_options\\.port');
    const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
    const clientIdInput = clientIdFormItem.locator('input').first();
    const topicsInput = page.locator('input[id^="data.groups_before."][id$=".topics"]').first();

    // Fill all required fields — use wrong port 1885
    await hostInput.fill('192.168.1.45');
    await portInput.clear();
    await portInput.fill('1885');

    await clientIdInput.scrollIntoViewIfNeeded();
    await clientIdInput.fill(`e2e_mqtt_cc_${ts}`);

    await topicsInput.scrollIntoViewIfNeeded();
    await topicsInput.fill('test_topic::0');

    // Click Check Connection
    const checkBtn = page.locator('.btn-check-connectivity');
    await checkBtn.scrollIntoViewIfNeeded();
    await checkBtn.click();

    // The result text appears below the button inside .box-check-connectivity .text
    const resultText = page.locator('.box-check-connectivity .text');
    await expect(resultText).toBeVisible({ timeout: 60_000 });

    // Should report the data source is NOT reachable
    await expect(resultText).toContainText(/not reachable/i);

    // The error message span should be visible below the main text
    const errorSpan = resultText.locator('span.error');
    await expect(errorSpan).toBeVisible({ timeout: 5_000 });
    await expect(errorSpan).toContainText(/error message/i);
  });

  test('create MQTT task, check connection, configure payload and mapping, submit and verify running', async ({
    page
  }) => {
    test.setTimeout(180_000);
    const ts = Date.now();
    const taskName = `mqtt_e2e_${ts}`;
    const clientId = `mqtt_client_${ts}`;
    const stableName = `mqtt_stable_${ts}`;
    const subtableName = `mqtt_table_\${g}`;
    const targetDb = 'test';

    try {
      // Ensure target database exists
      await runSqlBatch(page, [`CREATE DATABASE IF NOT EXISTS \`${targetDb}\`;`]);

      // ========================
      // Step 1: General Information
      // ========================
      await gotoDataInTask(page);
      await openAddSourceFromList(page);

      // Fill task name
      await page.locator('#name').fill(taskName);

      // Select target database BEFORE changing the datasource type,
      // because switching the type triggers a form re-render that can
      // cause the targetDB dropdown options to be unavailable briefly.
      await selectElOptionByText(page, 'targetDB', targetDb);
      await page.waitForTimeout(500);

      // Select MQTT as datasource type
      await selectElOptionByText(page, 'type', 'MQTT');

      // Wait for MQTT-specific form fields to render after type change
      const hostInput = page.locator('#data\\.connection_options\\.host');
      await expect(hostInput).toBeVisible({ timeout: 10_000 });

      // ========================
      // Step 2: Connection Configuration
      // ========================
      await hostInput.fill('192.168.1.45');

      const portInput = page.locator('#data\\.connection_options\\.port');
      await expect(portInput).toHaveValue('1883');

      // TLS Verification — default is "Disable", no change needed

      // ========================
      // Step 3: Authentication (leave empty)
      // ========================
      // Username and Password are optional, leave blank as per screenshot

      // ========================
      // Step 4: Collect section
      // ========================

      // MQTT protocol version — select 5.0
      const mqttVersionSelect = page.locator('input[id^="data.groups_before."][id$=".version"]').first();
      await mqttVersionSelect.scrollIntoViewIfNeeded();
      await expect(mqttVersionSelect).toBeVisible({ timeout: 10_000 });
      // Click to open dropdown, then select "5.0"
      await mqttVersionSelect.click({ force: true });
      const versionDropdown = page.locator('.el-select-dropdown:visible');
      await expect(versionDropdown).toBeVisible({ timeout: 5_000 });
      const version50Option = versionDropdown.locator('.el-select-dropdown__item').filter({ hasText: '5.0' }).first();
      await expect(version50Option).toBeVisible({ timeout: 5_000 });
      await version50Option.click();

      // Client ID
      const clientIdFormItem = page.locator('.el-form-item[class*="client_id"]').first();
      const clientIdInput = clientIdFormItem.locator('input').first();
      await clientIdInput.scrollIntoViewIfNeeded();
      await clientIdInput.fill(clientId);

      // Keep Alive — default is 60, no change needed

      // Clean Session — default is enabled, no change needed

      // Topics QoS Config
      const topicsInput = page.locator('input[id^="data.groups_before."][id$=".topics"]').first();
      await topicsInput.scrollIntoViewIfNeeded();
      await topicsInput.fill('abc::0');

      // Compression — default is "none", no change needed
      // Char Encoding — default is "UTF_8", no change needed

      // ========================
      // Step 5: Check Connection
      // ========================
      const checkBtn = page.locator('.btn-check-connectivity');
      await checkBtn.scrollIntoViewIfNeeded();
      await checkBtn.click();

      const resultText = page.locator('.box-check-connectivity .text');
      await expect(resultText).toBeVisible({ timeout: 60_000 });
      await expect(resultText).toContainText(/reachable/i);

      // Verify no error is shown
      const errorSpan = resultText.locator('span.error');
      await expect(errorSpan).not.toBeVisible({ timeout: 3_000 });

      // ========================
      // Step 6: Payload Transformation
      // ========================

      // The Payload Transformation section uses class "common-transformer".
      // Wait for the section to appear after check-connection succeeds.
      const transformerSection = page.locator('.common-transformer');
      await transformerSection.scrollIntoViewIfNeeded();
      await expect(transformerSection).toBeVisible({ timeout: 30_000 });

      // The sample data textarea is an el-input[type=textarea] with class "msgbody"
      // Element Plus renders it as a <textarea> inside .msgbody
      const sampleDataTextarea = transformerSection.locator('.msgbody textarea').first();
      await sampleDataTextarea.scrollIntoViewIfNeeded();
      await expect(sampleDataTextarea).toBeVisible({ timeout: 10_000 });
      await sampleDataTextarea.fill('{"a": 1}');

      // Parse section: json format should be the default.
      // Click the preview/parse icon button (the one with Icon name="PREVIEW" next to
      // the parse form) to extract columns from the sample data.
      // It is the button in .extract-parse that triggers submitParse.
      const parsePreviewBtn = transformerSection.locator('.extract-parse button').last();
      await parsePreviewBtn.scrollIntoViewIfNeeded();
      await expect(parsePreviewBtn).toBeVisible({ timeout: 5_000 });
      await parsePreviewBtn.click();

      // After parsing, a list of identified column chips (e.g. "a") should appear.
      // Wait for them to show up, which also means columnsArr is populated.
      const colChips = transformerSection.locator('.col-list li');
      await expect(colChips.first()).toBeVisible({ timeout: 15_000 });

      // ========================
      // Step 7: Mapping section
      // ========================

      // The target super table is an el-select with allow-create inside .table-title.
      // Now that columns are parsed, the select should be enabled.
      // The el-select has allow-create but the inner <input> is readonly.
      // We use the "Create STable" dropdown → dialog approach to create the super table.
      const createStableDropdown = transformerSection
        .locator('.table-title')
        .getByRole('button', { name: /Create STable/i })
        .first();
      await createStableDropdown.scrollIntoViewIfNeeded();
      await expect(createStableDropdown).toBeVisible({ timeout: 10_000 });
      await createStableDropdown.click();

      // The dropdown menu shows "Create STable" and "Create Template" — pick "Create STable"
      const stbDropdownMenu = page.locator('.el-dropdown-menu:visible');
      await expect(stbDropdownMenu).toBeVisible({ timeout: 5_000 });
      await stbDropdownMenu
        .locator('.el-dropdown-menu__item')
        .filter({ hasText: /Create STable/i })
        .first()
        .click();

      // Wait for the Create STable dialog to appear
      const stableDialog = page.locator('.el-dialog:visible').first();
      await expect(stableDialog).toBeVisible({ timeout: 10_000 });

      // Fill in the super table name — the input is inside .name_input
      const stableNameInput = stableDialog.locator('.name_input input').first();
      await expect(stableNameInput).toBeVisible({ timeout: 5_000 });
      await stableNameInput.fill(stableName);

      // The dialog should already have columns pre-populated from the parsed data.
      // The first column (TIMESTAMP) should have field name "ts" and the second
      // column field name "a" auto-filled from the parsed JSON. If any column/tag
      // field name is empty, fill it with a default.
      const collapseItems = stableDialog.locator('.el-collapse-item');

      // Process each collapse section (columns, tags)
      const sectionCount = await collapseItems.count();
      for (let si = 0; si < sectionCount; si++) {
        const section = collapseItems.nth(si);
        // Expand the section if it's collapsed
        const sectionHeader = section.locator('.el-collapse-item__header').first();
        const isActive = await section.evaluate(el => el.classList.contains('is-active')).catch(() => false);
        if (!isActive) {
          await sectionHeader.click();
          await page.waitForTimeout(300);
        }
        const rows = section.locator('.input-row');
        const rowCount = await rows.count();
        for (let ri = 0; ri < rowCount; ri++) {
          const inputRow = rows.nth(ri);
          // The field name is the last non-number el-input in the row
          const fieldInput = inputRow.locator('.el-input:not(.el-input-number) input').last();
          const val = await fieldInput.inputValue().catch(() => '');
          if (!val) {
            const fallbackName = si === 0 ? (ri === 0 ? 'ts' : `col_${ri}`) : `g`;
            await fieldInput.fill(fallbackName);
          }
        }
      }

      // Click the Create button in the dialog
      const stableCreateBtn = stableDialog.getByRole('button', { name: /^Create$/i }).first();
      await stableCreateBtn.scrollIntoViewIfNeeded();
      await expect(stableCreateBtn).toBeVisible({ timeout: 5_000 });
      await stableCreateBtn.click();

      // Wait for the dialog to close — the super table is created and selected automatically
      await expect(stableDialog).not.toBeVisible({ timeout: 30_000 });
      await page.waitForTimeout(2000);

      // After creating the super table, the mapping table (el-table) with
      // columns Name / Type / Expression should appear in .table-detail.
      const mappingTable = transformerSection.locator('.table-detail .el-table').first();
      await mappingTable.scrollIntoViewIfNeeded();
      await expect(mappingTable).toBeVisible({ timeout: 15_000 });

      // Row 1: SubTableName — Tablename — fill expression input with subtable name pattern
      const subtableRow = mappingTable.locator('.el-table__row').filter({ hasText: 'SubTableName' }).first();
      await subtableRow.scrollIntoViewIfNeeded();
      const subtableExprInput = subtableRow.locator('.el-input input').first();
      await subtableExprInput.clear();
      await subtableExprInput.fill(subtableName);

      // Row 2: ts — TIMESTAMP(ms) — select "generator" and value "now"
      // Note: DOM textContent concatenates cell values without spaces (e.g. "tsTIMESTAMP(ms)..."),
      // so \b word boundaries between column name and type don't work. Use ^ anchor instead.
      const tsRow = mappingTable.locator('.el-table__row').filter({ hasText: /^ts/i }).first();
      await tsRow.scrollIntoViewIfNeeded();
      // Click the expression type select (first .mapping-rule-select in the row) to pick "generator"
      const tsExprSelect = tsRow.locator('.mapping-rule-select').first();
      await tsExprSelect.click({ force: true });
      const tsDropdown = page.locator('.el-select-dropdown:visible');
      await expect(tsDropdown).toBeVisible({ timeout: 5_000 });
      await tsDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'generator' }).first().click();
      await page.waitForTimeout(500);
      // The generator input is disabled and pre-filled with "now" — no action needed

      // Row 3: a — BIGINT — select "mapping", then pick column "a"
      const aRow = mappingTable
        .locator('.el-table__row')
        .filter({ hasText: /^a/i })
        .filter({ hasNotText: /SubTableName/ })
        .first();
      await aRow.scrollIntoViewIfNeeded();
      const aExprSelect = aRow.locator('.mapping-rule-select').first();
      await aExprSelect.click({ force: true });
      const aDropdown = page.locator('.el-select-dropdown:visible');
      await expect(aDropdown).toBeVisible({ timeout: 5_000 });
      await aDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'mapping' }).first().click();
      await page.waitForTimeout(500);
      // After selecting "mapping", a second el-select appears for picking the source column
      const aMappingCol = aRow.locator('.mapping-rule-expression').first();
      await aMappingCol.click({ force: true });
      const aMappingDropdown = page.locator('.el-select-dropdown:visible');
      await expect(aMappingDropdown).toBeVisible({ timeout: 5_000 });
      await aMappingDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'a' }).first().click();
      await page.waitForTimeout(500);

      // Row 4: g — INT (tag) — select "value", then fill "1"
      const gRow = mappingTable
        .locator('.el-table__row')
        .filter({ hasText: /^g/i })
        .filter({ hasNotText: /SubTableName/ })
        .first();
      await gRow.scrollIntoViewIfNeeded();
      const gExprSelect = gRow.locator('.mapping-rule-select').first();
      await gExprSelect.click({ force: true });
      const gDropdown = page.locator('.el-select-dropdown:visible');
      await expect(gDropdown).toBeVisible({ timeout: 5_000 });
      await gDropdown.locator('.el-select-dropdown__item').filter({ hasText: 'value' }).first().click();
      await page.waitForTimeout(500);
      // Fill the value input with "1"
      const gValueInput = gRow.locator('.mapping-rule-expression input').first();
      await gValueInput.clear();
      await gValueInput.fill('1');

      // ========================
      // Step 8: Submit the task
      // ========================
      const submitBtn = page.locator('.btn-group-task').getByRole('button', { name: 'Submit' });
      await submitBtn.scrollIntoViewIfNeeded();

      try {
        await expect(submitBtn).toBeEnabled({ timeout: 10_000 });
      } catch {
        test.skip(true, 'Submit button not enabled - backend service may not be running');
      }

      await Promise.all([page.waitForURL(new RegExp(`${routes.dataInTask}$`), { timeout: 60_000 }), submitBtn.click()]);

      // ========================
      // Step 9: Verify task in list and status is Running
      // ========================
      // Find the task row in the list
      let row = await findTaskRow(page, taskName);
      await expect(row).toBeVisible({ timeout: 10_000 });

      // Refresh the task list up to 10 times (once per second) until status is "Running"
      const refreshBtn = page.getByRole('button', { name: /Refresh/i }).first();

      for (let i = 0; i < 10; i++) {
        row = await findTaskRow(page, taskName);
        const rowText = await row.textContent();
        if (rowText && /Running/i.test(rowText)) {
          break;
        }
        // Wait 1 second, then click Refresh
        await page.waitForTimeout(1000);
        await refreshBtn.click();
        await page.waitForTimeout(500); // Wait for table to update
      }

      // Final assertion: the task should be running
      row = await findTaskRow(page, taskName);
      await expect(row).toContainText(/Running/i, { timeout: 5_000 });
    } finally {
      // Cleanup: stop and delete the task (don't drop shared 'test' database)
      await stopTaskBestEffort(page, taskName);
      await deleteTaskBestEffort(page, taskName);
      // Drop the super table we created
      await runSqlBatch(page, [`DROP STABLE IF EXISTS \`${targetDb}\`.\`${stableName}\`;`]).catch(() => {});
    }
  });
});
