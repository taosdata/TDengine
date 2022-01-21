/**
 * Contains the the definitions/values assigned to various field types
 * @module FieldTypes
 */
/**
 * TDengine Field Types and their type codes
 * @typedef {Object} FieldTypes
 * @global
 * @property {number} C_NULL - Null
 * @property {number} C_BOOL - Boolean. Note, 0x02 is the C_BOOL_NULL value.
 * @property {number} C_TINYINT - Tiny Int, values in the range [-2^7+1, 2^7-1]. Note, -2^7 has been used as the C_TINYINT_NULL value
 * @property {number} C_SMALLINT - Small Int, values in the range [-2^15+1, 2^15-1]. Note, -2^15 has been used as the C_SMALLINT_NULL value
 * @property {number} C_INT - Int, values in the range [-2^31+1, 2^31-1]. Note, -2^31 has been used as the C_INT_NULL value
 * @property {number} C_BIGINT - Big Int, values in the range [-2^59, 2^59].
 * @property {number} C_FLOAT - Float, values in the range [-3.4E38, 3.4E38], accurate up to 6-7 decimal places.
 * @property {number} C_DOUBLE - Double, values in the range [-1.7E308, 1.7E308], accurate up to 15-16 decimal places.
 * @property {number} C_BINARY - Binary, encoded in utf-8.
 * @property {number} C_TIMESTAMP - Timestamp in format "YYYY:MM:DD HH:MM:SS.MMM". Measured in number of milliseconds passed after
                                    1970-01-01 08:00:00.000 GMT.
 * @property {number} C_NCHAR - NChar field type encoded in ASCII, a wide string.
 *
 *
 *
 * @property {number} C_TIMESTAMP_MILLI - The code for millisecond timestamps, as returned by libtaos.taos_result_precision(result).
 * @property {number} C_TIMESTAMP_MICRO - The code for microsecond timestamps, as returned by libtaos.taos_result_precision(result).
 */
module.exports = {
    C_NULL : 0,
    C_BOOL : 1,
    C_TINYINT : 2,
    C_SMALLINT : 3,
    C_INT : 4,
    C_BIGINT : 5,
    C_FLOAT : 6,
    C_DOUBLE : 7,
    C_BINARY : 8,
    C_TIMESTAMP : 9,
    C_NCHAR : 10,
    // NULL value definition
    // NOTE: These values should change according to C definition in tsdb.h
    C_BOOL_NULL : 2,
    C_TINYINT_NULL : -128,
    C_SMALLINT_NULL : -32768,
    C_INT_NULL : -2147483648,
    C_BIGINT_NULL : -9223372036854775808,
    C_FLOAT_NULL : 2146435072,
    C_DOUBLE_NULL : -9223370937343148032,
    C_NCHAR_NULL : 4294967295,
    C_BINARY_NULL : 255,
    C_TIMESTAMP_MILLI : 0,
    C_TIMESTAMP_MICRO : 1,
    getType,
}

const typeCodesToName = {
  0 : 'Null',
  1 : 'Boolean',
  2 : 'Tiny Int',
  3 : 'Small Int',
  4 : 'Int',
  5 : 'Big Int',
  6 : 'Float',
  7 : 'Double',
  8 : 'Binary',
  9 : 'Timestamp',
  10 : 'Nchar',
}

/**
 * @function
 * @param {number} typecode - The code to get the name of the type for
 * @return {string} Name of the field type
 */
function getType(typecode) {
  return typeCodesToName[typecode];
}
