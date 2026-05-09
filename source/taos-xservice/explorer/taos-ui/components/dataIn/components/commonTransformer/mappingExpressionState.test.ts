import { describe, expect, it } from 'vitest';
import { applyExpressionMode, hasConfiguredExpression } from './mappingExpressionState';
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
});
