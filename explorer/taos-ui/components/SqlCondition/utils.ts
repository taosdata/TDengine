import { isArray, isString } from 'utils/validate';
import { ConcatAndOperator, ContainOperator, StringOperator } from 'constants1/tdengine';
import { getFieldType, escapeSpecialChar } from 'utils/tdengine';
export interface ConditionProps {
  modelValue: DataItem[];
  fields: Field[];
  top?: boolean;
  parentField?: string;
  isForm?: boolean;
}

export type DataItem = RuleDataItem | GroupItem;
export interface RuleDataItem {
  id: number;
  field: string;
  operator: string;
  value: string | (number | string)[];
  connector: 'AND' | 'OR';
}
export interface GroupItem {
  id: number;
  connector: 'AND' | 'OR';
  children: DataItem[];
}
export function isGroupItemType(data: any): data is GroupItem {
  return typeof data === 'object' && data !== null && Array.isArray(data.children);
}

export interface conditionItemProps {
  modelValue: RuleDataItem;
  fields: Field[];
  lasted: boolean;
  parentField: string;
  isForm: boolean;
}

export interface Field {
  field: string;
  type: string;
  [key: string]: any;
}

// 根据树形数据生成condition数据
export function generateConditionString(data: DataItem[], fields: Field[], isTag = false): string {
  const len = data.length;
  if (len == 0) return '';
  return data
    .map((item, index): string => {
      if (isGroupItemType(item)) {
        if (item.children.length == 0) return '';
        return (
          `(${generateConditionString(item.children, fields, isTag)})` + (index == len - 1 ? '' : ` ${item.connector} `)
        );
      } else {
        if (!item.field || !item.value) return '';
        const connector = index == len - 1 ? '' : ' ' + item.connector;
        const value = getFieldVlaue(item, fields);
        if (isTag) {
          return `tag_name='${item.field}' and tag_value ${item.operator} ${value} ` + connector;
        } else {
          return `${item.field} ${item.operator} ${value} ` + connector;
        }
      }
    })
    .join(' ')
    .trim();
}
export function getFieldVlaue(data: RuleDataItem, fields: Field[]): string | number {
  const { field, value, operator } = data;
  if (!value) return '';
  const valueType = getFieldType(fields.find(item => item.field == field)?.type ?? '');
  switch (valueType) {
    case 'STRING':
      return processStringValue(operator, value as string);
    case 'TIMESTAMP':
      return processTimestampValue(operator, value as string);
    case 'NUMBER':
      return processNumberValue(operator, value as string);
    default:
      return value as string;
  }
}

function processTimestampValue(operator: string, value: string | string[]): string | number {
  if (ConcatAndOperator.includes(operator) && isArray(value))
    return value.map((val: string) => (isNaN(Number(val)) && val.includes('-') ? `"${val}"` : val)).join(' AND ');
  if (ContainOperator.includes(operator) && isString(value))
    return (
      '(' +
      value
        .split(',')
        .map(val => (isNaN(Number(val)) && val.includes('-') ? `"${val}"` : val))
        .join(',') +
      ')'
    );
  if (isNaN(Number(value))) {
    return `"${value}"`;
  } else {
    return Number(value);
  }
}

function processStringValue(operator: string, value: string) {
  if (ContainOperator.includes(operator))
    return (
      '(' +
      value
        .split(',')
        .map(val => `'${escapeSpecialChar(val)}'`)
        .join(',') +
      ')'
    );
  value = escapeSpecialChar(value);
  if (StringOperator.includes(operator)) return `'${value}'`;
  return `'${value}'`;
}
function processNumberValue(operator: string, value: string | string[]) {
  if (ContainOperator.includes(operator) && isString(value)) {
    const valList = value.split(',');
    return '(' + valList.map(val => (isNaN(Number(val)) ? `"${val}"` : Number(val))).join(',') + ')';
  }
  if (ConcatAndOperator.includes(operator) && isArray(value)) return value.map(val => Number(val)).join(' AND ');
  return Number(value);
}

let idCounter = 0;
// 解析where条件
export function parseWhereCondition(condition: string): DataItem[] {
  const tokens = tokenize(condition);
  return parseTokens(tokens);
}

function tokenize(condition: string): string[] {
  const regex = /\s*(AND|OR|\(|\)|BETWEEN|AND|CONTAINS|IS NULL|LIKE|NOT LIKE|IN|NOT IN|[^\s()]+)\s*/gi;
  return condition.match(regex)?.map(item => item.trim().replace(/['"]/g, '')) || [];
}

function parseTokens(tokens: string[]): DataItem[] {
  const result: DataItem[] = [];
  let currentConnector: 'AND' | 'OR' = 'AND';
  while (tokens.length > 0) {
    const originToken = tokens[0];
    const token = tokens.shift()?.toUpperCase();

    if (token === '(') {
      const children = parseTokens(tokens);
      tokens.shift(); // Skip ')'
      const connector = tokens.shift()!.toUpperCase();
      result.push({
        id: idCounter++,
        connector: connector && connector == 'AND' ? 'AND' : 'OR',
        children
      });
    } else if (token === ')') {
      break;
    } else if (token === 'AND' || token === 'OR') {
      currentConnector = token;
    } else {
      const field = originToken;
      const operator = tokens.shift()?.toUpperCase() ?? '';
      let value: string | (number | string)[] = '';

      if (operator === 'BETWEEN') {
        const startValue = tokens.shift()!;
        tokens.shift(); // Skip 'AND'
        const endValue = tokens.shift()!;
        value = [startValue, endValue];
      } else if (operator === 'IN' || operator === 'NOT IN') {
        tokens.shift(); // Skip '('
        const values: (number | string)[] = [];
        while (tokens[0] !== ')') {
          values.push(tokens.shift()!);
          if (tokens[0] === ',') tokens.shift(); // Skip ','
        }
        tokens.shift(); // Skip ')'
        value = values;
      } else if (operator === 'IS NULL') {
        value = 'NULL';
      } else {
        value = tokens.shift()!;
      }

      const rule: RuleDataItem = {
        id: idCounter++,
        field,
        operator,
        value,
        connector: currentConnector
      };

      result.push(rule);
    }
  }

  return result;
}
