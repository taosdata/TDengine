/**
 * E2E tests for Kafka multi-data-structures transformer feature.
 *
 * Covers:
 *  - Rule blocks section rendering when Kafka datasource is selected
 *  - Adding and removing rule blocks interactively
 *  - Matches expression preview filters rows to matching records only
 *  - Extract preview within a rule block passes the matches condition
 *  - Importing a pre-configured multi-rule task and verifying echo on edit
 */

import * as path from 'path';
import * as os from 'os';
import * as fs from 'fs/promises';
import { fileURLToPath } from 'url';
import { test, expect } from './_utils/test';
import {
  gotoDataInTask,
  openAddSourceFromList,
  selectElOptionByText,
  findTaskRow,
  viewTaskReadonlyFromRow
} from './_utils/datain';
import { stopTaskBestEffort, deleteTaskBestEffort } from './_utils/cleanup';
import { runSqlBatch } from './_utils/explorerSql';
import { rewriteKafkaImportContent } from './_utils/importTaskFile';

// Resource file that contains a pre-configured Kafka task with two rule blocks.
const IMPORT_FILE = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  'resources/datain-kafka-multi-datastructures.json'
);

let preparedImportFilePromise: Promise<string> | undefined;

// Task name embedded in the resource file.
const IMPORTED_TASK_NAME = 'kafka';

// Target database encoded in the resource file's "to" URL.
const IMPORT_FILE_DB = 'test';

// Two sample messages used to drive preview tests.
// Message 1: modelQualifier = "air_robot_v"  → matches rule 1 only
// Message 2: modelQualifier = "air_conditioner_v" → matches rule 2 only
const SAMPLE_ROBOT_MSG =
  '{"dataQualifier":"jobInfo","deviceName":"设备名称详情","deviceQualifier":"air_conditioner$MF202203110001$air_conditioner_plc_protocol0$2","factoryCode":"MF202203110001","factoryName":"北京赢识科技有限公司","machineType":5,"machineTypeName":"机器人","modelQualifier":"air_robot_v","reportData":{"DeviceNo":2,"originTime":1766366626844,"RunMode":0,"DStatus":4},"uuid":"9b1bb91c-1d6d-4bbc-914f-36a066863121"}';

const SAMPLE_CONDITIONER_MSG =
  '{"dataQualifier":"jobInfo","deviceName":"设备名称详情","deviceQualifier":"air_conditioner$MF202203110001$air_conditioner_plc_protocol0$2","factoryCode":"MF202203110001","factoryName":"北京赢识科技有限公司","machineType":5,"machineTypeName":"空调","modelQualifier":"air_conditioner_v","reportData":{"DeviceNo":2,"HumiSet":0,"originTime":1766366626844,"Temp":13.628554344177246,"RunMode":0,"TempSet":20,"DeviceStatus":4,"Humi":63.07699203491211},"uuid":"9b1bb91c-1d6d-4bbc-914f-36a066863121"}';

const SAMPLE_PAYLOAD = [SAMPLE_ROBOT_MSG, SAMPLE_CONDITIONER_MSG].join('\n');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Upload the multi-data-structures JSON export and wait for the import dialog. */
async function openImportDialog(page: import('playwright/test').Page) {
  const importFile = await getImportFileForCurrentEnv();
  const fileInput = page.locator('.inline-upload input[type="file"]');
  await fileInput.setInputFiles(importFile);

  const dialog = page.locator('.el-dialog:visible').filter({ hasText: /import task/i });
  await expect(dialog).toBeVisible({ timeout: 15_000 });
  return dialog;
}

async function getImportFileForCurrentEnv() {
  preparedImportFilePromise ??= (async () => {
    const ciBroker = process.env.INTEGRATION_TEST_KAFKA_BROKER;
    if (!ciBroker) {
      return IMPORT_FILE;
    }

    const rewritten = rewriteKafkaImportContent(await fs.readFile(IMPORT_FILE, 'utf8'), ciBroker);
    const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'kafka-import-'));
    const importFile = path.join(tempDir, path.basename(IMPORT_FILE));

    await fs.writeFile(importFile, rewritten, 'utf8');
    return importFile;
  })();

  return preparedImportFilePromise;
}

/** Navigate to DataIn > add, choose Kafka, fill the sample-data textarea and wait for rule blocks. */
async function setupKafkaTransformerWithPayload(page: import('playwright/test').Page) {
  await gotoDataInTask(page);
  await openAddSourceFromList(page);
  await selectElOptionByText(page, 'type', 'Kafka');

  const transformer = page.locator('.common-transformer');
  await transformer.scrollIntoViewIfNeeded();
  await expect(transformer).toBeVisible({ timeout: 15_000 });

  const textarea = transformer.locator('.msgbody textarea').first();
  await textarea.scrollIntoViewIfNeeded();
  await expect(textarea).toBeVisible({ timeout: 10_000 });
  await textarea.fill(SAMPLE_PAYLOAD);

  // Ensure the rule blocks sidebar is visible before returning
  const ruleBlocksSection = transformer.locator('.rule-blocks');
  await ruleBlocksSection.scrollIntoViewIfNeeded();
  await expect(ruleBlocksSection).toBeVisible({ timeout: 10_000 });
}

/**
 * Click the parse-preview button and wait for column chips to appear.
 * Required before calling previewMatches() — the handler reads
 * transformerState.topParse which is only populated after a successful parse preview.
 */
async function triggerParsePreview(page: import('playwright/test').Page) {
  const transformer = page.locator('.common-transformer');
  const parsePreviewBtn = transformer.locator('.extract-parse button').last();
  await parsePreviewBtn.scrollIntoViewIfNeeded();
  await expect(parsePreviewBtn).toBeVisible({ timeout: 10_000 });
  await parsePreviewBtn.click();
  // Wait for at least one column chip — confirms parse succeeded and topParse is set
  const colChips = transformer.locator('.col-list li');
  await expect(colChips.first()).toBeVisible({ timeout: 15_000 });
}

// ---------------------------------------------------------------------------
// Test suite 1: Rule block UI structure
// ---------------------------------------------------------------------------

test.describe('DataIn Kafka - Rule blocks UI', () => {
  test('rule blocks section is visible when Kafka datasource is selected', async ({ page }) => {
    test.setTimeout(60_000);

    await gotoDataInTask(page);
    await openAddSourceFromList(page);
    await selectElOptionByText(page, 'type', 'Kafka');

    const transformer = page.locator('.common-transformer');
    await transformer.scrollIntoViewIfNeeded();
    await expect(transformer).toBeVisible({ timeout: 15_000 });

    // The "Rule Blocks" section should be visible
    const ruleBlocksSection = transformer.locator('.rule-blocks');
    await ruleBlocksSection.scrollIntoViewIfNeeded();
    await expect(ruleBlocksSection).toBeVisible({ timeout: 10_000 });

    // Should have exactly one rule block card initially
    const ruleCards = ruleBlocksSection.locator('.rule-block-card');
    await expect(ruleCards).toHaveCount(1, { timeout: 5_000 });
  });

  test('initial rule block card has a match expression input and preview button', async ({ page }) => {
    test.setTimeout(60_000);

    await gotoDataInTask(page);
    await openAddSourceFromList(page);
    await selectElOptionByText(page, 'type', 'Kafka');

    const transformer = page.locator('.common-transformer');
    await expect(transformer).toBeVisible({ timeout: 15_000 });

    const ruleCard = transformer.locator('.rule-block-card').first();
    await ruleCard.scrollIntoViewIfNeeded();
    await expect(ruleCard).toBeVisible({ timeout: 10_000 });

    // Match expression input should exist — the <input> itself carries the class and data-test attr
    const matchesInput = ruleCard.locator('[data-test="rule-matches-input"]');
    await expect(matchesInput).toBeVisible({ timeout: 5_000 });

    // Preview button should exist (shows the PREVIEW icon without border)
    const previewBtn = ruleCard.locator('[data-test="rule-preview-matches-button"]');
    await expect(previewBtn).toBeVisible({ timeout: 5_000 });
  });

  test('Add Rule button appends a new rule block card', async ({ page }) => {
    test.setTimeout(60_000);

    await gotoDataInTask(page);
    await openAddSourceFromList(page);
    await selectElOptionByText(page, 'type', 'Kafka');

    const transformer = page.locator('.common-transformer');
    await expect(transformer).toBeVisible({ timeout: 15_000 });

    const ruleBlocksSection = transformer.locator('.rule-blocks');
    await ruleBlocksSection.scrollIntoViewIfNeeded();

    // Click "Add Rule"
    const addBtn = ruleBlocksSection.locator('.rule-blocks__add-btn');
    await addBtn.scrollIntoViewIfNeeded();
    await expect(addBtn).toBeVisible({ timeout: 5_000 });
    await addBtn.click();

    // There should now be two rule block cards
    const ruleCards = ruleBlocksSection.locator('.rule-block-card');
    await expect(ruleCards).toHaveCount(2, { timeout: 5_000 });
  });

  test('deleting a rule block removes it from the list', async ({ page }) => {
    test.setTimeout(60_000);

    await gotoDataInTask(page);
    await openAddSourceFromList(page);
    await selectElOptionByText(page, 'type', 'Kafka');

    const transformer = page.locator('.common-transformer');
    await expect(transformer).toBeVisible({ timeout: 15_000 });

    const ruleBlocksSection = transformer.locator('.rule-blocks');
    await ruleBlocksSection.scrollIntoViewIfNeeded();

    // Add a second rule block
    const addBtn = ruleBlocksSection.locator('.rule-blocks__add-btn');
    await addBtn.click();
    await expect(ruleBlocksSection.locator('.rule-block-card')).toHaveCount(2, { timeout: 5_000 });

    // Delete the second rule block (the delete button has --danger modifier)
    const secondCard = ruleBlocksSection.locator('.rule-block-card').nth(1);
    const deleteBtn = secondCard.locator('.rule-block-card__button--danger');
    await deleteBtn.click();

    // Back to one rule block
    await expect(ruleBlocksSection.locator('.rule-block-card')).toHaveCount(1, { timeout: 5_000 });
  });
});

// ---------------------------------------------------------------------------
// Test suite 2: Matches preview
// ---------------------------------------------------------------------------

test.describe('DataIn Kafka - Matches expression preview', () => {
  test('matches preview shows only rows that satisfy the filter expression', async ({ page }) => {
    test.setTimeout(120_000);

    await setupKafkaTransformerWithPayload(page);
    // Parse preview must run first so that transformerState.topParse is populated
    await triggerParsePreview(page);

    const transformer = page.locator('.common-transformer');
    const ruleBlocksSection = transformer.locator('.rule-blocks');
    await ruleBlocksSection.scrollIntoViewIfNeeded();

    // Set the matches expression on the first rule block to filter for robot messages
    const firstCard = ruleBlocksSection.locator('.rule-block-card').first();
    const matchesInput = firstCard.locator('[data-test="rule-matches-input"]');
    await matchesInput.clear();
    await matchesInput.fill('modelQualifier == "air_robot_v"');

    // Click the matches preview button
    const previewBtn = firstCard.locator('[data-test="rule-preview-matches-button"]');
    // Wait for the API response — the table stays visible throughout, so we must wait
    // for the backend to return the filtered result before checking content.
    const responsePromise = page.waitForResponse(r => r.url().includes('/transform/sample'));
    await previewBtn.click();
    await responsePromise;

    // Wait for a result table to appear
    const resultTable = page.locator('.result-table');
    await expect(resultTable).toBeVisible({ timeout: 10_000 });

    // The result should contain "air_robot_v" (matched) but NOT "air_conditioner_v"
    await expect(resultTable).toContainText('air_robot_v', { timeout: 10_000 });
    await expect(resultTable).not.toContainText('air_conditioner_v', { timeout: 5_000 });
  });

  test('matches preview with no-match expression returns empty table', async ({ page }) => {
    test.setTimeout(120_000);

    await setupKafkaTransformerWithPayload(page);
    await triggerParsePreview(page);

    const transformer = page.locator('.common-transformer');
    const ruleBlocksSection = transformer.locator('.rule-blocks');
    await ruleBlocksSection.scrollIntoViewIfNeeded();

    // Expression that matches nothing
    const firstCard = ruleBlocksSection.locator('.rule-block-card').first();
    const matchesInput = firstCard.locator('[data-test="rule-matches-input"]');
    await matchesInput.clear();
    await matchesInput.fill('modelQualifier == "nonexistent_model"');

    const previewBtn = firstCard.locator('[data-test="rule-preview-matches-button"]');
    // Wait for the API response before checking — the table stays visible throughout (no v-if toggle),
    // so we must wait for the backend to return the filtered (empty) result set.
    const responsePromise = page.waitForResponse(r => r.url().includes('/transform/sample'));
    await previewBtn.click();
    await responsePromise;

    const resultTable = page.locator('.result-table');
    await expect(resultTable).toBeVisible({ timeout: 5_000 });

    // Wait for the matches-filtered result to replace the earlier parse-preview content.
    // Neither qualifier should appear in the result (nonexistent_model matches nothing).
    await expect(resultTable).not.toContainText('air_robot_v', { timeout: 10_000 });
    await expect(resultTable).not.toContainText('air_conditioner_v', { timeout: 5_000 });
  });

  test('second rule block matches preview is independent from first rule block', async ({ page }) => {
    test.setTimeout(120_000);

    await setupKafkaTransformerWithPayload(page);
    await triggerParsePreview(page);

    const transformer = page.locator('.common-transformer');
    const ruleBlocksSection = transformer.locator('.rule-blocks');
    await ruleBlocksSection.scrollIntoViewIfNeeded();

    // Add a second rule block
    const addBtn = ruleBlocksSection.locator('.rule-blocks__add-btn');
    await addBtn.click();
    await expect(ruleBlocksSection.locator('.rule-block-card')).toHaveCount(2, { timeout: 5_000 });

    // Set second rule to match air_conditioner_v
    const secondCard = ruleBlocksSection.locator('.rule-block-card').nth(1);
    await secondCard.click(); // activate it
    const matchesInput = secondCard.locator('[data-test="rule-matches-input"]');
    await matchesInput.clear();
    await matchesInput.fill('modelQualifier == "air_conditioner_v"');

    // Click the second rule's preview button
    const previewBtn = secondCard.locator('[data-test="rule-preview-matches-button"]');
    // Wait for the API response before checking — the table stays visible throughout (no v-if toggle),
    // so we must wait for the backend to return the filtered result set.
    const responsePromise = page.waitForResponse(r => r.url().includes('/transform/sample'));
    await previewBtn.click();
    await responsePromise;

    const resultTable = page.locator('.result-table');
    await expect(resultTable).toBeVisible({ timeout: 5_000 });

    // Wait for the matches-filtered result to replace the earlier parse-preview content.
    // Only air_conditioner_v rows should be present; air_robot_v should be absent.
    await expect(resultTable).toContainText('air_conditioner_v', { timeout: 10_000 });
    await expect(resultTable).not.toContainText('air_robot_v', { timeout: 5_000 });
  });
});

// ---------------------------------------------------------------------------
// Test suite 3: Extract preview respects matches
// ---------------------------------------------------------------------------

test.describe('DataIn Kafka - Extract preview within rule block', () => {
  test('extract preview passes matches condition and shows filtered rows', async ({ page }) => {
    test.setTimeout(120_000);

    await setupKafkaTransformerWithPayload(page);

    const transformer = page.locator('.common-transformer');
    const ruleBlocksSection = transformer.locator('.rule-blocks');
    await ruleBlocksSection.scrollIntoViewIfNeeded();

    // Set the matches expression on the first rule block
    const firstCard = ruleBlocksSection.locator('.rule-block-card').first();
    const matchesInput = firstCard.locator('[data-test="rule-matches-input"]');
    await matchesInput.clear();
    await matchesInput.fill('modelQualifier == "air_robot_v"');

    // Parse preview first so columns are available
    const parsePreviewBtn = transformer.locator('.extract-parse button').last();
    await parsePreviewBtn.scrollIntoViewIfNeeded();
    await expect(parsePreviewBtn).toBeVisible({ timeout: 10_000 });
    await parsePreviewBtn.click();

    // Wait for column chips to appear
    const colChips = transformer.locator('.col-list li');
    await expect(colChips.first()).toBeVisible({ timeout: 15_000 });

    const resultTable = page.locator('.result-table');
    await expect(resultTable).toBeVisible({ timeout: 10_000 });

    // The parsed result shows both records (parse is before matches filter)
    const parseText = await resultTable.textContent();
    expect(parseText).toContain('air_robot_v');
  });
});

// ---------------------------------------------------------------------------
// Test suite 4: Import and echo
// ---------------------------------------------------------------------------

test.describe('DataIn Kafka - Import multi-data-structures task', () => {
  // All import tests share the same task name from the resource file.
  // Run serially to avoid parallel conflicts on that fixed name.
  test.describe.configure({ mode: 'serial' });
  test('import dialog shows one row for the multi-data-structures task', async ({ page }) => {
    test.setTimeout(60_000);

    await gotoDataInTask(page);
    const dialog = await openImportDialog(page);

    const tableRows = dialog.locator('.el-table__row');
    await expect(tableRows.first()).toBeVisible({ timeout: 10_000 });
    await expect(tableRows).toHaveCount(1);

    // Cancel to avoid creating the task
    const cancelBtn = dialog.locator('.dialog-footer').getByRole('button', { name: /cancel/i });
    await cancelBtn.click();
    await expect(dialog).not.toBeVisible({ timeout: 5_000 });
  });

  test('confirming import creates the task with multi-rule configuration', async ({ page }) => {
    test.setTimeout(120_000);

    await runSqlBatch(page, [`CREATE DATABASE IF NOT EXISTS \`${IMPORT_FILE_DB}\`;`]);

    await gotoDataInTask(page);
    const dialog = await openImportDialog(page);

    // Select all rows
    const headerCheckbox = dialog.locator('thead .el-checkbox').first();
    await headerCheckbox.click();

    // Confirm the target db is pre-selected
    const dbSelect = dialog.locator('.el-table__row').first().locator('td').last().locator('.el-select');
    await expect(dbSelect.locator('.el-select__wrapper')).toContainText(IMPORT_FILE_DB, { timeout: 10_000 });

    const confirmBtn = dialog.locator('.dialog-footer').getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(dialog).not.toBeVisible({ timeout: 15_000 });

    try {
      await gotoDataInTask(page);
      const row = await findTaskRow(page, IMPORTED_TASK_NAME);
      await expect(row).toBeVisible({ timeout: 15_000 });
    } finally {
      await stopTaskBestEffort(page, IMPORTED_TASK_NAME);
      await deleteTaskBestEffort(page, IMPORTED_TASK_NAME);
    }
  });

  test('imported task view page shows two rule blocks with correct matches expressions', async ({ page }) => {
    test.setTimeout(180_000);

    await runSqlBatch(page, [`CREATE DATABASE IF NOT EXISTS \`${IMPORT_FILE_DB}\`;`]);

    await gotoDataInTask(page);
    const dialog = await openImportDialog(page);

    const headerCheckbox = dialog.locator('thead .el-checkbox').first();
    await headerCheckbox.click();

    const confirmBtn = dialog.locator('.dialog-footer').getByRole('button', { name: /confirm/i });
    await confirmBtn.click();
    await expect(dialog).not.toBeVisible({ timeout: 15_000 });

    try {
      await gotoDataInTask(page);
      const row = await findTaskRow(page, IMPORTED_TASK_NAME);
      // Use viewTaskReadonlyFromRow — imported Kafka tasks start in "Created" state
      // where "Edit" is unavailable; "View" opens the same form component (readonly=true).
      await viewTaskReadonlyFromRow(page, row);

      // The transformer section should show the rule blocks panel
      const transformer = page.locator('.common-transformer');
      await transformer.scrollIntoViewIfNeeded();
      await expect(transformer).toBeVisible({ timeout: 30_000 });

      const ruleBlocksSection = transformer.locator('.rule-blocks');
      await ruleBlocksSection.scrollIntoViewIfNeeded();
      await expect(ruleBlocksSection).toBeVisible({ timeout: 15_000 });

      // Two rule blocks should be present
      const ruleCards = ruleBlocksSection.locator('.rule-block-card');
      await expect(ruleCards).toHaveCount(2, { timeout: 15_000 });

      // First rule block should have the air_robot_v matches expression
      const firstCardInput = ruleCards.nth(0).locator('[data-test="rule-matches-input"]');
      await expect(firstCardInput).toHaveValue(/air_robot_v/, { timeout: 5_000 });

      // Second rule block should have the air_conditioner_v matches expression
      const secondCardInput = ruleCards.nth(1).locator('[data-test="rule-matches-input"]');
      await expect(secondCardInput).toHaveValue(/air_conditioner_v/, { timeout: 5_000 });
    } finally {
      await stopTaskBestEffort(page, IMPORTED_TASK_NAME);
      await deleteTaskBestEffort(page, IMPORTED_TASK_NAME);
    }
  });
});
