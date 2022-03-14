const FieldTypes = require('./constants');
const util = require('util');
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
function TaosRow(row) {
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
  constructor(date, precision = 0) {
    if (precision === 1) {
      super(Math.floor(date / 1000));
      this.precisionExtras = date % 1000;
    } else if (precision === 2) {
      // use BigInt to fix: 1623254400999999999 / 1000000 = 1623254401000 which not expected
      super(parseInt(BigInt(date) / 1000000n));
      // use BigInt to fix: 1625801548423914405 % 1000000 = 914496 which not expected (914405)
      this.precisionExtras = parseInt(BigInt(date) % 1000000n);
    } else {
      super(parseInt(date));
    }
    this.precision = precision;
  }

  /**
   * TDengine raw timestamp.
   * @returns raw taos timestamp (int64)
   */
  taosTimestamp() {
    if (this.precision == 1) {
      return (this * 1000 + this.precisionExtras);
    } else if (this.precision == 2) {
      return (this * 1000000 + this.precisionExtras);
    } else {
      return Math.floor(this);
    }
  }

  /**
   * Gets the microseconds of a Date.
   * @return {Int} A microseconds integer
   */
  getMicroseconds() {
    if (this.precision == 1) {
      return this.getMilliseconds() * 1000 + this.precisionExtras;
    } else if (this.precision == 2) {
      return this.getMilliseconds() * 1000 + this.precisionExtras / 1000;
    } else {
      return 0;
    }
  }
  /**
   * Gets the nanoseconds of a TaosTimestamp.
   * @return {Int} A nanoseconds integer
   */
  getNanoseconds() {
    if (this.precision == 1) {
      return this.getMilliseconds() * 1000000 + this.precisionExtras * 1000;
    } else if (this.precision == 2) {
      return this.getMilliseconds() * 1000000 + this.precisionExtras;
    } else {
      return 0;
    }
  }

  /**
   * @returns {String} a string for timestamp string format
   */
  _precisionExtra() {
    if (this.precision == 1) {
      return String(this.precisionExtras).padStart(3, '0');
    } else if (this.precision == 2) {
      return String(this.precisionExtras).padStart(6, '0');
    } else {
      return '';
    }
  }
  /**
   * @function Returns the date into a string usable by TDengine
   * @return {string} A Taos Timestamp String
   */
  toTaosString() {
    var tzo = -this.getTimezoneOffset(),
      dif = tzo >= 0 ? '+' : '-',
      pad = function (num) {
        var norm = Math.floor(Math.abs(num));
        return (norm < 10 ? '0' : '') + norm;
      },
      pad2 = function (num) {
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
      '' + this._precisionExtra();
  }

  /**
   * Custom console.log
   * @returns {String} string format for debug
   */
  [util.inspect.custom](depth, opts) {
    return this.toTaosString() + JSON.stringify({ precision: this.precision, precisionExtras: this.precisionExtras }, opts);
  }
  toString() {
    return this.toTaosString();
  }
}

module.exports = { TaosRow, TaosField, TaosTimestamp }
