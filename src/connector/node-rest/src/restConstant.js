/**
 * indicate the every type's type code
 * @type {{"0": string, "1": string, "2": string, "3": string, "4": string, "5": string, "6": string, "7": string, "8": string, "9": string, "10": string}}
 */
export const typeCodesToName = {
  0: 'Null',
  1: 'Boolean',
  2: 'Tiny Int',
  3: 'Small Int',
  4: 'Int',
  5: 'Big Int',
  6: 'Float',
  7: 'Double',
  8: 'Binary',
  9: 'Timestamp',
  10: 'Nchar',
}

/**
 * get the type of input typecode, in fact the response of restful will send every column's typecode
 * @param typecode
 * @returns {*}
 */
export function getTaoType(typecode) {
  return typeCodesToName[typecode];
}