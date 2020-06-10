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
  constructor(date, micro = false) {
    super(date);
    this._type = 'TaosTimestamp';
    if (micro) {
      this.microTime = date - Math.floor(date);
    }
  }
  /**
   * @function Returns the date into a string usable by TDengine
   * @return {string} A Taos Timestamp String
   */
  toTaosString(){
    var tzo = -this.getTimezoneOffset(),
        dif = tzo >= 0 ? '+' : '-',
        pad = function(num) {
            var norm = Math.floor(Math.abs(num));
            return (norm < 10 ? '0' : '') + norm;
        },
        pad2 = function(num) {
            var norm = Math.floor(Math.abs(num));
            if (norm < 10) return '00' + norm;
            if (norm < 100) return '0' + norm;
            if (norm < 1000) return norm;
        };
    return this.getFullYear() +
        '-' + pad(this.getMonth() + 1) +
        '-' + pad(this.getDate()) +
        ' ' + pad(this.getHours()) +
        ':' + pad(this.getMinutes()) +
        ':' + pad(this.getSeconds()) +
        '.' + pad2(this.getMilliseconds()) +
        ''  + (this.microTime ? pad2(Math.round(this.microTime * 1000)) : '');
  }
}

module.exports = {TaosRow, TaosField, TaosTimestamp}
