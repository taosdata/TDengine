
import { defaultValueProcessorByRule } from './defaultValueProcessorByRule';
import { mapSQLOperator, mapSQLField, quoteFieldNamesWithArray } from './utils';

export const defaultRuleProcessorSQL = (
  rule,
  {
    parseNumbers,
    escapeQuotes,
    quoteFieldNamesWith = ['', ''],
    valueProcessor = defaultValueProcessorByRule,
  } = {}
) => {
  const value = valueProcessor(rule, { parseNumbers, escapeQuotes, quoteFieldNamesWith });
  const operator = mapSQLOperator(rule.operator);
  const field = mapSQLField(rule.field)

  const operatorLowerCase = operator.toLowerCase();
  if (
    (operatorLowerCase === 'in' ||
      operatorLowerCase === 'not in' ||
      operatorLowerCase === 'between' ||
      operatorLowerCase === 'not between') &&
    !value
  ) {
    return '';
  }

  const [qFNWpre, qFNWpost] = quoteFieldNamesWithArray(quoteFieldNamesWith);

  return `${qFNWpre}${field}${qFNWpost} ${operator} ${value}`.trim();
};
