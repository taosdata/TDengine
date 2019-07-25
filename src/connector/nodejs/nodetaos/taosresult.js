/**
 * TaosResult
 * @module TaosResult
 */

 module.exports = TaosResult;
/**
 * Constructor for a TaosResult object;
 * @class TaosResult
 * @constructor
 * @param {Array<Row>} - Array of result rows
 * @param {Array<Field>} - Array of field meta data
 * @return {TaosResult}
 */
function TaosResult(result, fields) {
  this.result = result;
  this.rowcount = this.result.length;
  this.fields = parseFields(fields);
}

TaosResult.prototype.parseFields = function parseFields(fields) {
  fields.map(function(field) {
    return {}
  })
}
