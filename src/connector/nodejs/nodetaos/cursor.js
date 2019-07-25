const CTaosInterface = require('./cinterface')
const errors = require ('./error')
const TaosQuery = require('./taosquery')
module.exports = TDengineCursor;

/**
 * Constructor for a TDengine Cursor
 * @class TDengineCursor
 * @constructor
 * @param {TDengineConnection} - The TDengine Connection this cursor uses to interact with TDengine
 * @property {data} - Latest retrieved data from query execution. It is an empty array by default
 * @property {fieldNames} - Array of the field names in order from left to right of the latest data retrieved
 */
function TDengineCursor(connection=null) {
  this._description = null;
  this._rowcount = -1;
  this._connection = null;
  this._result = null;
  this._fields = null;
  this.data = [];
  this.fieldNames = null;
  this._chandle = new CTaosInterface(null, true); //pass through, just need library loaded.
  if (connection != null) {
    this._connection = connection
  }

}
/**
 * Get the description of the latest query
 * @return {string} Description
 */
TDengineCursor.prototype.description = function description() {
  return this._description;
}
/**
 * Get the row counts of the latest query
 * @return {number} Rowcount
 */
TDengineCursor.prototype.rowcount = function rowcount() {
  return this._rowcount;
}
TDengineCursor.prototype.callproc = function callproc() {
  return;
}
/**
 * Close the cursor by setting its connection to null and freeing results from the connection and resetting the results it has stored
 * @return {boolean} Whether or not the cursor was succesfully closed
 */
TDengineCursor.prototype.close = function close() {
  if (this._connection == null) {
    return false;
  }
  this._connection._clearResultSet();
  this._reset_result();
  this._connection = null;
  return true;
}
/**
 *
 * @return {TaosQuery} - A TaosQuery object
 */
TDengineCursor.prototype.query = function query(operation) {
  return new TaosQuery(operation, this);
}

/**
 * Execute a query. Also stores all the field names returned from the query into cursor.fieldNames;
 * @param {string} operation -  The query operation to execute in the taos shell
 * @param {number} verbose - A verbosity of 0 silences any logs from this function. A verbosity of 1 means this function will log query statues
 * @return {number | buffer} Number of affected rows or a buffer that points to the results of the query
 */
TDengineCursor.prototype.execute = function execute(operation, verbose) {
  if (operation == undefined) {
    return null;
  }
  if (this._connection == null) {
    throw new errors.ProgrammingError('Cursor is not connected');
  }
  this._connection._clearResultSet();
  this._reset_result();

  let stmt = operation;
  res = this._chandle.query(this._connection._conn, stmt);
  if (res == 0) {
    let fieldCount = this._chandle.fieldsCount(this._connection._conn);
    if (fieldCount == 0) {
      let response = "Query OK"
      return this._chandle.affectedRows(this._connection._conn); //return num of affected rows, common with insert, use statements
    }
    else {
      let resAndField = this._chandle.useResult(this._connection._conn, fieldCount)
      this._result = resAndField.result;
      this._fields = resAndField.fields;
      this.fieldNames = resAndField.fields.map(fieldData => fieldData.name);
      return this._handle_result(); //return a pointer to the result
    }
  }
  else {
    throw new errors.ProgrammingError(this._chandle.errStr(this._connection._conn))
  }

}
TDengineCursor.prototype.executemany = function executemany() {

}
TDengineCursor.prototype.fetchone = function fetchone() {

}
TDengineCursor.prototype.fetchmany = function fetchmany() {

}
/**
 * Fetches all results from a query and also stores results into cursor.data
 *
 * @return {Array<Row>} The resultant array, with entries corresponding to each retreived row from the query results, sorted in
 * order by the field name ordering in the table.
 * @example
 * cursor.execute('select * from db.table');
 * var data = cursor.fetchall()
 */
TDengineCursor.prototype.fetchall = function fetchall() {
  if (this._result == null || this._fields == null) {
    throw new errors.OperationalError("Invalid use of fetchall, either result or fields from query are null");
  }
  let data = [];
  this._rowcount = 0;
  let k = 0;
  while(true) {
    k+=1;
    let blockAndRows = this._chandle.fetchBlock(this._result, this._fields);
    let block = blockAndRows.blocks;
    let num_of_rows = blockAndRows.num_of_rows;

    if (num_of_rows == 0) {
      break;
    }
    this._rowcount += num_of_rows;
    for (let i = 0; i < num_of_rows; i++) {
      data.push([]);
      for (let j = 0; j < this._fields.length; j++) {
        data[data.length-1].push(block[j][i]);
      }
    }
  }
  this._connection._clearResultSet();
  this.data = data;
  return data;
}
TDengineCursor.prototype.nextset = function nextset() {
  return;
}
TDengineCursor.prototype.setinputsize = function setinputsize() {
  return;
}
TDengineCursor.prototype.setoutputsize = function setoutputsize(size, column=null) {
  return;
}
TDengineCursor.prototype._reset_result = function _reset_result() {
  this._description = null;
  this._rowcount = -1;
  this._result = null;
  this._fields = null;
  this.data = [];
  this.fieldNames = null;
}
TDengineCursor.prototype._handle_result = function _handle_result() {
  this._description = [];
  for (let field of this._fields) {
    this._description.push([field.name, field.type, null, null, null, null, false]);
  }
  return this._result;
}
