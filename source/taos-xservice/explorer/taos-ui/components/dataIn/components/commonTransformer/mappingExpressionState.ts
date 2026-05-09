import type { TableRow } from './type';

function isMultiSelectExpressionMode(exprname: string) {
  return exprname === 'sum' || exprname === 'join';
}

export function applyExpressionMode(row: TableRow) {
  row.Expression = isMultiSelectExpressionMode(row.exprname) ? [] : '';

  if (row.default !== undefined && row.default !== '') {
    row.default = '';
  }
  if (row.defaultValueError) {
    row.defaultValueError = '';
  }

  if (row.exprname === 'generator') {
    row.Expression = 'now';
  }
}

export function shouldShowGeneratorWarning(row: TableRow) {
  return !!row.PrimaryKey && row.exprname === 'generator';
}

export function isExpressionDisabled(row: TableRow) {
  return row.exprname === 'generator';
}

export function hasConfiguredExpression(expression: TableRow['Expression']) {
  if (typeof expression === 'string') {
    return expression.trim() !== '';
  }

  return expression.length > 0;
}
