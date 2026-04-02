import { test, expect } from './_utils/test';
import { gotoDataInTask, openAddSourceFromList, selectElOptionByText } from './_utils/datain';

/**
 * Regression test for: Filter preview drops falsy values (boolean false, numeric 0).
 *
 * The bug was in filterExpression.vue where a bare truthiness check
 * `data[index] ? data[index].toString() : null` treated boolean `false` and
 * numeric `0` as empty/null. The fix uses the `filterEmpty()` utility instead.
 */
test.describe('DataIn - Filter preview preserves falsy values', () => {
  const SAMPLE_PAYLOAD = [
    '{"name": "san", "age": 28, "isStudent": false, "score": 0, "address": {"city": "beijing"}}',
    '{"name": "si", "age": 28, "isStudent": false, "score": 0, "address": {"city": "beijing"}}'
  ].join('\n');

  test('boolean false and numeric 0 are preserved in filter preview results', async ({ page }) => {
    test.setTimeout(120_000);

    // Navigate to DataIn add page and select Kafka as datasource type
    await gotoDataInTask(page);
    await openAddSourceFromList(page);
    await selectElOptionByText(page, 'type', 'Kafka');

    // Wait for Kafka-specific form fields to render
    const transformerSection = page.locator('.common-transformer');
    await transformerSection.scrollIntoViewIfNeeded();
    await expect(transformerSection).toBeVisible({ timeout: 15_000 });

    // Fill the sample payload textarea with JSON containing boolean false and numeric 0
    const sampleDataTextarea = transformerSection.locator('.msgbody textarea').first();
    await sampleDataTextarea.scrollIntoViewIfNeeded();
    await expect(sampleDataTextarea).toBeVisible({ timeout: 10_000 });
    await sampleDataTextarea.fill(SAMPLE_PAYLOAD);

    // Click the parse preview button to extract columns
    const parsePreviewBtn = transformerSection.locator('.extract-parse button').last();
    await parsePreviewBtn.scrollIntoViewIfNeeded();
    await expect(parsePreviewBtn).toBeVisible({ timeout: 5_000 });
    await parsePreviewBtn.click();

    // Wait for column chips to appear (indicates parsing succeeded)
    const colChips = transformerSection.locator('.col-list li');
    await expect(colChips.first()).toBeVisible({ timeout: 15_000 });

    // Verify parse preview result table shows correct values
    const resultTable = page.locator('.result-table');
    await expect(resultTable).toBeVisible({ timeout: 10_000 });

    // Verify parse preview contains "false" for isStudent (not empty)
    const parseResultText = await resultTable.textContent();
    expect(parseResultText).toContain('false');

    // Click "Add Filter" button
    const addFilterBtn = transformerSection.locator('button').filter({ hasText: /Add Filter/i }).first();
    await addFilterBtn.scrollIntoViewIfNeeded();
    await expect(addFilterBtn).toBeEnabled({ timeout: 5_000 });
    await addFilterBtn.click();

    // Fill the filter expression input
    const filterInput = transformerSection.locator('.filter-expression input').first();
    await expect(filterInput).toBeVisible({ timeout: 5_000 });
    await filterInput.fill('!name.is_empty()');

    // Click the filter preview button (the second button in the filter row, after delete)
    const filterBtns = transformerSection.locator('.filter-expression .btns button');
    // The preview button is the last one (after the delete button)
    const filterPreviewBtn = filterBtns.last();
    await filterPreviewBtn.scrollIntoViewIfNeeded();
    await filterPreviewBtn.click();

    // Wait for the result table to update with filter results
    await expect(resultTable).toContainText('Preview Filter Results', { timeout: 10_000 });

    // Extract the full text content from the filter result table
    const filterResultText = await resultTable.textContent();

    // Verify boolean false is preserved (the core bug fix)
    expect(filterResultText).toContain('false');

    // Verify numeric 0 is preserved (same class of bug - falsy values)
    // The score column should contain "0"
    // Extract cell values from the table to verify more precisely
    const tableCells = resultTable.locator('.el-table__body-wrapper .el-table__row td .cell');
    const cellTexts: string[] = [];
    const cellCount = await tableCells.count();
    for (let i = 0; i < cellCount; i++) {
      cellTexts.push((await tableCells.nth(i).textContent()) ?? '');
    }

    // Verify that "false" appears as a cell value (isStudent column)
    expect(cellTexts).toContain('false');

    // Verify that "0" appears as a cell value (score column)
    expect(cellTexts).toContain('0');

    // Verify that expected data rows are present
    expect(cellTexts).toContain('san');
    expect(cellTexts).toContain('si');
    expect(cellTexts).toContain('28');
  });
});
