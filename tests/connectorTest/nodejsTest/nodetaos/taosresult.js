require('./globalfunc.js')
const TaosObjects = require('./taosobjects');
const TaosRow = TaosObjects.TaosRow;
const TaosField = TaosObjects.TaosField;

module.exports = TaosResult;
/**
 * @class TaosResult
 * @classdesc A TaosResult class consts of the row data and the fields metadata, all wrapped under various objects for higher functionality.
 * @param {Array<TaosRow>} data - Array of result rows
 * @param {Array<TaosField>} fields - Array of field meta data
 * @property {Array<TaosRow>} data - Array of TaosRows forming the result data (this does not include field meta data)
 * @property {Array<TaosField>} fields - Array of TaosFields forming the fields meta data array.
 * @return {TaosResult}
 * @since 1.0.6
 */
function TaosResult(data, fields) {
  this.data = data.map(row => new TaosRow(row));
  this.rowcount = this.data.length;
  this.fields = fields.map(field => new TaosField(field));
}
/**
 * Pretty print data and the fields meta data as if you were using the taos shell
 * @memberof TaosResult
 * @function pretty
 * @since 1.0.6
 */

TaosResult.prototype.pretty = function pretty() {
  let fieldsStr = "";
  let sizing = [];
  this.fields.forEach((field,i) => {
    if (field._field.type == 8 || field._field.type == 10){
      sizing.push(Math.max(field.name.length, field._field.bytes));
    }
    else {
      sizing.push(Math.max(field.name.length, suggestedMinWidths[field._field.type]));
    }
    fieldsStr += fillEmpty(Math.floor(sizing[i]/2 - field.name.length / 2)) + field.name + fillEmpty(Math.ceil(sizing[i]/2 - field.name.length / 2)) + " | ";
  });
  var sumLengths = sizing.reduce((a,b)=> a+=b,(0)) + sizing.length * 3;

  console.log("\n" + fieldsStr);
  console.log(printN("=",sumLengths));
  this.data.forEach(row => {
    let rowStr = "";
    row.data.forEach((entry, i) => {
      if (this.fields[i]._field.type == 9) {
        entry = entry.toTaosString();
      } else {
        entry = entry == null ? 'null' : entry.toString();
      }
      rowStr += entry
      rowStr += fillEmpty(sizing[i] - entry.length) + " | ";
    });
    console.log(rowStr);
  });
}
const suggestedMinWidths = {
  0: 4,
  1: 4,
  2: 4,
  3: 6,
  4: 11,
  5: 12,
  6: 24,
  7: 24,
  8: 10,
  9: 25,
  10: 10,
}
function printN(s, n) {
  let f = "";
  for (let i = 0; i < n; i ++) {
    f += s;
  }
  return f;
}
function fillEmpty(n) {
  let str = "";
  for (let i = 0; i < n; i++) {
    str += " ";
  }
  return str;
}
