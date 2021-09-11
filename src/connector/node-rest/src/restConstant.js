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

export function getTaoType(typecode) {
  return typeCodesToName[typecode];
}