import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { reactive, ref } from 'vue';

describe('resetTransformerPreviewState', () => {
  beforeEach(() => {
    vi.stubGlobal('reactive', reactive);
    vi.stubGlobal('ref', ref);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('clears visible preview result state', async () => {
    const { resetTransformerPreviewState, transformerState } = await import('./util');

    transformerState.transformResultTable = [{ a: 1 }];
    transformerState.transResultName = 'identified';
    transformerState.showResultTb = true;
    transformerState.resultTbTitle = 'parseResTb';
    transformerState.activeColumns = ['a'];
    transformerState.resultCurrentPage = 3;

    resetTransformerPreviewState();

    expect(transformerState.transformResultTable).toEqual([]);
    expect(transformerState.transResultName).toBe('');
    expect(transformerState.showResultTb).toBe(false);
    expect(transformerState.resultTbTitle).toBe('');
    expect(transformerState.activeColumns).toEqual([]);
    expect(transformerState.resultCurrentPage).toBe(1);
  });
});

describe('limitPreviewRows', () => {
  it('keeps at most the shared preview row limit', async () => {
    const { limitPreviewRows, PREVIEW_ROW_LIMIT } = await import('./util');
    const rows = Array.from({ length: PREVIEW_ROW_LIMIT + 5 }, (_, index) => ({ index }));

    const limitedRows = limitPreviewRows(rows);

    expect(limitedRows).toHaveLength(PREVIEW_ROW_LIMIT);
    expect(limitedRows[0]).toEqual({ index: 0 });
    expect(limitedRows[PREVIEW_ROW_LIMIT - 1]).toEqual({ index: PREVIEW_ROW_LIMIT - 1 });
  });

  it('returns a new array even when row count is already within the limit', async () => {
    const { limitPreviewRows } = await import('./util');
    const rows = [{ index: 1 }];

    const limitedRows = limitPreviewRows(rows);

    expect(limitedRows).toEqual(rows);
    expect(limitedRows).not.toBe(rows);
  });
});

describe('mapPreviewRows', () => {
  it('stops mapping rows once the shared preview row limit is reached', async () => {
    const { mapPreviewRows, PREVIEW_ROW_LIMIT } = await import('./util');
    const entries = [
      { columns: Array.from({ length: PREVIEW_ROW_LIMIT + 5 }, (_, index) => [index]) }
    ];
    const mappedIndexes: number[] = [];

    const rows = mapPreviewRows(entries, (row: number[]) => {
      mappedIndexes.push(row[0]);
      return { index: row[0] };
    });

    expect(rows).toHaveLength(PREVIEW_ROW_LIMIT);
    expect(mappedIndexes).toHaveLength(PREVIEW_ROW_LIMIT);
    expect(mappedIndexes).toEqual(Array.from({ length: PREVIEW_ROW_LIMIT }, (_, index) => index));
  });
});

describe('convert', () => {
  it('keeps tag rows marked as TAG when tag definitions omit length', async () => {
    const { convert } = await import('./util');

    const rows = convert({
      columns: [
        { name: 'ts', type: 'TIMESTAMP' },
        { name: 'val', type: 'FLOAT' }
      ],
      tags: [{ name: 'code', type: 'VARCHAR(50)' }]
    });

    expect(rows).toEqual([
      ['ts', 'TIMESTAMP', ''],
      ['val', 'FLOAT', ''],
      ['code', 'VARCHAR(50)', '', 'TAG']
    ]);
  });

  it('normalizes backticked tag names before marking them as TAG', async () => {
    const { convert } = await import('./util');

    const rows = convert({
      columns: [{ name: 'ts', type: 'TIMESTAMP' }],
      tags: [{ name: '`code`', type: 'VARCHAR(50)' }]
    });

    expect(rows).toEqual([
      ['ts', 'TIMESTAMP', ''],
      ['code', 'VARCHAR(50)', '', 'TAG']
    ]);
  });
});
