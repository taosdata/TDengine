import { defaultJoinChar } from './defaults';

/**
 * Splits a string by a given character (default ','). Escaped characters (characters
 * preceded by a backslash) will not apply to the split, and the backslash will be
 * removed in the array element. Inverse of `joinWith`.
 *
 * @example
 * splitBy('this\\,\\,that,,the other,,,\\,')
 * // or
 * splitBy('this\\,\\,that,,the other,,,\\,', ',')
 * // would return
 * ['this,,that', '', 'the other', '', '', ',']
 */
export const splitBy = (str, splitChar = defaultJoinChar) =>
  typeof str === 'string'
    ? str
        .split(`\\${splitChar}`)
        .map(c => c.split(splitChar))
        .reduce((prev, curr, idx) => {
          if (idx === 0) {
            return curr;
          }
          return [
            ...prev.slice(0, prev.length - 1),
            `${prev[prev.length - 1]}${splitChar}${curr[0]}`,
            ...curr.slice(1),
          ];
        }, [])
    : [];

/**
 * Joins an array of strings using the given character (default ','). When the given
 * character appears in an array element, a backslash will be added just before it to
 * distinguish it from the join character. Inverse of `splitBy`.
 *
 * @example
 * joinWith(['this,,that', '', 'the other', '', '', ','])
 * // would return
 * 'this\\,\\,that,,the other,,,\\,'
 */
export const joinWith = (strArr, joinChar = defaultJoinChar) =>
  strArr.map(str => `${str}`.replaceAll(joinChar, `\\${joinChar}`)).join(joinChar);

/**
 * Trims the value if it is a string. Otherwise returns value as-is.
 */
export const trimIfString = (val) => (typeof val === 'string' ? val.trim() : val);

/**
 * Splits strings by comma and trims each element. Arrays are returned as-is but
 * any string elements are trimmed.
 */
export const toArray = (v) =>
  Array.isArray(v)
    ? v.map(trimIfString)
    : typeof v === 'string'
    ? splitBy(v, defaultJoinChar)
        .filter(s => !/^\s*$/.test(s))
        .map(s => s.trim())
    : typeof v === 'number'
    ? [v]
    : [];

export const nullFreeArray = (arr) => arr.every(Boolean);
