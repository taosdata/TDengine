import { describe, expect, it } from 'vitest';
import { applyExpressionMode, hasConfiguredExpression, syncMappingColumns } from './mappingExpressionState';
import type { TableRow } from './type';

function createRow(overrides: Partial<TableRow> = {}): TableRow {
  return {
    Name: 'value',
    Type: 'FLOAT',
    exprname: 'mapping',
    maptype: ['string', 'FLOAT'],
    Expression: 'source_value',
    ...overrides
  };
}

describe('mappingExpressionState', () => {
  it('resets multi-select expression modes to an array model value', () => {
    const sumRow = createRow({ exprname: 'sum' });
    const joinRow = createRow({ exprname: 'join' });

    applyExpressionMode(sumRow);
    applyExpressionMode(joinRow);

    expect(sumRow.Expression).toEqual([]);
    expect(joinRow.Expression).toEqual([]);
  });

  it('counts only non-empty configured expressions', () => {
    expect(hasConfiguredExpression('value')).toBe(true);
    expect(hasConfiguredExpression('  ')).toBe(false);
    expect(hasConfiguredExpression(['a'])).toBe(true);
    expect(hasConfiguredExpression([])).toBe(false);
  });

  it('preserves mapping expressions when preview clears mapping columns', () => {
    const row = createRow({ Expression: 'source_value' });

    const labels = syncMappingColumns([], [row]);

    expect(labels).toEqual([]);
    expect(row.Expression).toBe('source_value');
  });

  it('clears mapping expressions that no longer exist in the latest preview columns', () => {
    const row = createRow({ Expression: 'stale_value' });

    const labels = syncMappingColumns([{ label: 'fresh_value' }], [row]);

    expect(labels).toEqual(['fresh_value']);
    expect(row.Expression).toBe('');
  });
});
