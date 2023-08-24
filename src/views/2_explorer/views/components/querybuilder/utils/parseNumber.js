import { numericRegex } from './misc';



export const parseNumber = (v, { parseNumbers }) => {
  if (typeof v === 'bigint' || typeof v === 'number') {
    return v;
  }
  return parseNumbers && (parseNumbers === 'native' || numericRegex.test(v)) ? parseFloat(v) : v;
};
