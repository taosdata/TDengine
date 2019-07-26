const FieldTypes = require('./constants');

/**
 * Various objects such as TaosRow and TaosColumn that help make parsing data easier
 * @module TaosObjects
 *
 */

/**
 * The TaosRow object. Contains the data from a retrieved row from a database and functions that parse the data.
 * @typedef {Object} TaosRow - A row of data retrieved from a table.
 * @global
 * @example
 * var trow = new TaosRow(row);
 * console.log(trow.data);
 */
function TaosRow (row) {
  this.data = row;
  this.length = row.length;
  return this;
}

/**
 * @typedef {Object} TaosField - A field/column's metadata from a table.
 * @global
 * @example
 * var tfield = new TaosField(field);
 * console.log(tfield.name);
 */

function TaosField(field) {
 this._field = field;
 this.name = field.name;
 this.type = FieldTypes.getType(field.type);
 return this;
}

/**
 * A TaosTimestamp object, which is the standard date object with added functionality
 * @global
 * @memberof TaosObjects
 * @param {Date} date - A Javascript date time object or the time in milliseconds past 1970-1-1 00:00:00.000
 */
class TaosTimestamp extends Date {
  constructor(date) {
    super(date);
    this._type = 'TaosTimestamp';
  }
  /**
   * @function Returns the date into a string usable by TDengine
   * @return {string} A Taos Timestamp String
   */
  toTaosString(){
    let tsArr = this.toISOString().split("T")
    return tsArr[0] + " " + tsArr[1].substring(0, tsArr[1].length-1);
  }
}

module.exports = {TaosRow, TaosField, TaosTimestamp}
