import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { reactive, ref } from 'vue';

// These tests cover the "preview button" code path for the three transformer
// preview entry points (main mapping rule, filter expression rule, extract /
// split rule). The shared concern is that the upstream
// `api/x/transform/sample/flat` endpoint can return an empty array `[]` (or a
// `{ message: '...' }` error envelope), and the UI used to crash with
// `Cannot read properties of undefined (reading 'fields')` when accessing
// `result[0].fields` directly. These tests exercise the helper used by all
// three call sites and the watcher that derives mapping columns.

describe('isEmptyParserResult (preview button empty-state guard)', () => {
  beforeEach(() => {
    vi.stubGlobal('reactive', reactive);
    vi.stubGlobal('ref', ref);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.resetModules();
  });

  it('treats an empty array as an empty preview result', async () => {
    const { isEmptyParserResult } = await import('./util');
    expect(isEmptyParserResult([])).toBe(true);
  });

  it('treats a non-array (e.g. error envelope) as an empty preview result', async () => {
    const { isEmptyParserResult } = await import('./util');
    expect(isEmptyParserResult({ message: 'parse failed' })).toBe(true);
    expect(isEmptyParserResult(null)).toBe(true);
    expect(isEmptyParserResult(undefined)).toBe(true);
  });

  it('treats an entry without a fields array as empty', async () => {
    const { isEmptyParserResult } = await import('./util');
    expect(isEmptyParserResult([{}])).toBe(true);
    expect(isEmptyParserResult([{ fields: undefined }])).toBe(true);
    expect(isEmptyParserResult([{ fields: null }])).toBe(true);
    expect(isEmptyParserResult([{ fields: 'not-an-array' }])).toBe(true);
  });

  it('treats an entry without a columns array as empty', async () => {
    const { isEmptyParserResult } = await import('./util');
    expect(isEmptyParserResult([{ fields: [{ name: 'col1' }] }])).toBe(true);
    expect(isEmptyParserResult([{ fields: [{ name: 'col1' }], columns: undefined }])).toBe(true);
    expect(isEmptyParserResult([{ fields: [{ name: 'col1' }], columns: null }])).toBe(true);
    expect(isEmptyParserResult([{ fields: [{ name: 'col1' }], columns: 'not-an-array' }])).toBe(true);
  });

  it('accepts a normal preview result with at least one field', async () => {
    const { isEmptyParserResult } = await import('./util');
    expect(
      isEmptyParserResult([
        {
          fields: [{ name: 'col1' }],
          columns: [['v1']]
        }
      ])
    ).toBe(false);
  });

  it('accepts a normal result whose columns array is empty', async () => {
    const { isEmptyParserResult } = await import('./util');
    expect(
      isEmptyParserResult([
        {
          fields: [{ name: 'col1' }],
          columns: []
        }
      ])
    ).toBe(false);
  });
});

describe('preview button: clearing transformer preview state', () => {
  beforeEach(() => {
    vi.stubGlobal('reactive', reactive);
    vi.stubGlobal('ref', ref);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.resetModules();
  });

  it('resetTransformerPreviewState clears all visible preview fields', async () => {
    const { resetTransformerPreviewState, transformerState } = await import('./util');

    transformerState.transformResultTable = [{ a: 1 }];
    transformerState.transResultName = 'identified';
    transformerState.showResultTb = true;
    transformerState.resultTbTitle = 'parseResTb';
    transformerState.activeColumns = ['a'];
    transformerState.resultCurrentPage = 4;

    resetTransformerPreviewState();

    expect(transformerState.transformResultTable).toEqual([]);
    expect(transformerState.transResultName).toBe('');
    expect(transformerState.showResultTb).toBe(false);
    expect(transformerState.resultTbTitle).toBe('');
    expect(transformerState.activeColumns).toEqual([]);
    expect(transformerState.resultCurrentPage).toBe(1);
  });

  it('handles the empty-result preview flow: no crash and downstream state is cleared', async () => {
    const { isEmptyParserResult, resetTransformerPreviewState, transformerState } = await import(
      './util'
    );

    // Pretend the preview button was clicked and the API returned [].
    const apiResponse: unknown = [];

    transformerState.transformerMapColumns = [
      {
        value: 'mapping',
        label: 'mapping',
        children: [{ value: 'old', label: 'old' }]
      }
    ];

    expect(() => {
      if (isEmptyParserResult(apiResponse)) {
        resetTransformerPreviewState();
        transformerState.transformerMapColumns = [];
      }
    }).not.toThrow();

    expect(transformerState.transformerMapColumns).toEqual([]);
    expect(transformerState.transformResultTable).toEqual([]);
    expect(transformerState.showResultTb).toBe(false);
  });

  it('showEmptyTransformerPreview keeps the preview table visible with empty data', async () => {
    const { showEmptyTransformerPreview, transformerState } = await import('./util');

    transformerState.transformResultTable = [{ a: 1 }];
    transformerState.transResultName = 'old';
    transformerState.showResultTb = false;
    transformerState.resultTbTitle = '';
    transformerState.activeColumns = ['a'];
    transformerState.resultCurrentPage = 5;

    showEmptyTransformerPreview('mappingResTb', 'mapping');

    // After clicking preview with an empty API response, the preview panel
    // must still be rendered so the user sees the table title; the rows are
    // simply empty.
    expect(transformerState.showResultTb).toBe(true);
    expect(transformerState.resultTbTitle).toBe('mappingResTb');
    expect(transformerState.transResultName).toBe('mapping');
    expect(transformerState.transformResultTable).toEqual([]);
    expect(transformerState.activeColumns).toEqual([]);
    expect(transformerState.resultCurrentPage).toBe(1);
  });

  it('showEmptyTransformerPreview supports the filter-rule preview path', async () => {
    const { showEmptyTransformerPreview, transformerState } = await import('./util');

    showEmptyTransformerPreview('filterResTb', 'filter');

    expect(transformerState.showResultTb).toBe(true);
    expect(transformerState.resultTbTitle).toBe('filterResTb');
    expect(transformerState.transResultName).toBe('filter');
    expect(transformerState.transformResultTable).toEqual([]);
  });

  it('showEmptyTransformerPreview supports the extract/split preview path', async () => {
    const { showEmptyTransformerPreview, transformerState } = await import('./util');

    showEmptyTransformerPreview('extractResTb', 'col_a');

    expect(transformerState.showResultTb).toBe(true);
    expect(transformerState.resultTbTitle).toBe('extractResTb');
    expect(transformerState.transResultName).toBe('col_a');
    expect(transformerState.transformResultTable).toEqual([]);
  });
});

// Mirrors the watcher in `index.vue` (~line 1483) that derives mapping
// column labels from `transformerState.transformerMapColumns`. The original
// code dereferenced `val.filter(...)[0].children` directly and crashed when
// the upstream guard set the array to []. This test exercises the safe
// implementation shape.
function deriveMappingColumns(
  val: Array<{ value: string; label: string; children?: Array<{ label: string }> }>
): { mappingColumns: Array<{ label: string }>; labels: string[] } {
  const mappingEntry = val.filter(item => item.value === 'mapping')[0];
  const mappingColumns = mappingEntry?.children ?? [];
  const labels = mappingColumns.map(item => item.label);
  return { mappingColumns, labels };
}

describe('preview button: mapping columns watcher', () => {
  it('produces empty columns when the source array is empty', () => {
    expect(() => deriveMappingColumns([])).not.toThrow();
    expect(deriveMappingColumns([])).toEqual({ mappingColumns: [], labels: [] });
  });

  it('produces empty columns when no entry has value "mapping"', () => {
    const val = [
      { value: 'expression', label: 'expression', children: [{ label: 'concat' }] }
    ];
    expect(() => deriveMappingColumns(val)).not.toThrow();
    expect(deriveMappingColumns(val)).toEqual({ mappingColumns: [], labels: [] });
  });

  it('produces empty columns when mapping entry has no children', () => {
    const val = [{ value: 'mapping', label: 'mapping' }];
    expect(deriveMappingColumns(val)).toEqual({ mappingColumns: [], labels: [] });
  });

  it('returns the children labels of the mapping entry for a normal result', () => {
    const val = [
      { value: 'expression', label: 'expression', children: [{ label: 'concat' }] },
      {
        value: 'mapping',
        label: 'mapping',
        children: [{ label: 'col1' }, { label: 'col2' }]
      }
    ];
    expect(deriveMappingColumns(val)).toEqual({
      mappingColumns: [{ label: 'col1' }, { label: 'col2' }],
      labels: ['col1', 'col2']
    });
  });
});
