
import { numericRegex } from './misc';
import { parseNumber } from './parseNumber';

export const mapSQLOperator = (op) => {
  switch (op.toLowerCase()) {
    case 'null':
      return 'is null';
    case 'notnull':
      return 'is not null';
    case 'notin':
      return 'not in';
    case 'notbetween':
      return 'not between';
    case 'contains':
    case 'beginswith':
    case 'endswith':
      return 'like';
    case 'doesnotcontain':
    case 'doesnotbeginwith':
    case 'doesnotendwith':
      return 'not like';
    default:
      return op;
  }
};

export const mapSQLField = (fd) => {
  switch (fd.toLowerCase()) {
    case 'start time':
      return '_c0';
    case 'end time':
      return '_c0';
    default:
      return fd
  }
}

export const mongoOperators = {
  '=': '$eq',
  '!=': '$ne',
  '<': '$lt',
  '<=': '$lte',
  '>': '$gt',
  '>=': '$gte',
  in: '$in',
  notIn: '$nin',
};

export const celCombinatorMap = {
  and: '&&',
  or: '||',
};

/**
 * Register these operators with jsonLogic before applying the
 * result of formatQuery(query, 'jsonlogic').
 *
 * @example
 * ```
 * for (const [op, func] of Object.entries(jsonLogicAdditionalOperators)) {
 *   jsonLogic.add_operation(op, func);
 * }
 * jsonLogic.apply({ "startsWith": [{ "var": "firstName" }, "Stev"] }, data);
 * ```
 */
export const jsonLogicAdditionalOperators = {
  startsWith: (a, b) => typeof a === 'string' && a.startsWith(b),
  endsWith: (a, b) => typeof a === 'string' && a.endsWith(b),
};

export const numerifyValues = (rg) => ({
  ...rg,
  // @ts-expect-error TS doesn't keep track of odd/even indexes here
  rules: rg.rules.map(r => {
    if (typeof r === 'string') {
      return r;
    }

    if ('rules' in r) {
      return numerifyValues(r);
    }

    let { value } = r;
    if (typeof value === 'string') {
      value = parseNumber(value, { parseNumbers: true });
    }

    return { ...r, value };
  }),
});

export const isValidValue = (v) =>
  (typeof v === 'string' && v.length > 0) ||
  (typeof v === 'number' && !isNaN(v)) ||
  (typeof v !== 'string' && typeof v !== 'number');

export const shouldRenderAsNumber = (v, parseNumbers) =>
  parseNumbers &&
  (typeof v === 'number' ||
    typeof v === 'bigint' ||
    (typeof v === 'string' && numericRegex.test(v)));

export const isValueProcessorLegacy = (
  vp 
) => vp.length >= 3;

export const quoteFieldNamesWithArray = (
  quoteFieldNamesWith = ['', '']) =>
  Array.isArray(quoteFieldNamesWith)
    ? quoteFieldNamesWith
    : typeof quoteFieldNamesWith === 'string'
    ? [quoteFieldNamesWith, quoteFieldNamesWith]
    : quoteFieldNamesWith ?? ['', ''];
