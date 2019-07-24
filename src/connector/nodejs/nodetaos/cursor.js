const CTaosInterface = require('./cinterface')
const errors = require ('./error')
module.exports = TDengineCursor;

function TDengineCursor(connection=null) {
  this._description = null;
  this._rowcount = -1;
  this._connection = null;
  this._result = null;
  this._fields = null;
  this.data = null;
  this.fieldNames = null;
  this.chandle = new CTaosInterface(null, true); //pass through, just need library loaded.
  if (connection != null) {
    this._connection = connection
  }

}
TDengineCursor.prototype.description = function description() {
  return this._description;
}
TDengineCursor.prototype.rowcount = function rowcount() {
  return this._rowcount;
}
TDengineCursor.prototype.callproc = function callproc() {
  return;
}
TDengineCursor.prototype.close = function close() {
  if (this._connection == null) {
    return false;
  }
  this._connection.clear_result_set();
  this._reset_result();
  this._connection = null;
  return true;
}
TDengineCursor.prototype.execute = function execute(operation, params=null) {
  if (operation == undefined) {
    return null;
  }
  if (this._connection == null) {
    throw new errors.ProgrammingError('Cursor is not connected');
  }
  this._connection.clear_result_set();
  this._reset_result();

  let stmt = operation;
  if (params != null) {
    //why pass?
  }
  res = this.chandle.query(this._connection._conn, stmt);
  if (res == 0) {
    let fieldCount = this.chandle.fieldsCount(this._connection._conn);
    if (fieldCount == 0) {
      return this.chandle.affectedRows(this._connection._conn); //return num of affected rows, common with insert, use statements
    }
    else {
      let resAndField = this.chandle.useResult(this._connection._conn, fieldCount)
      this._result = resAndField.result;
      this._fields = resAndField.fields;
      this.fieldNames = resAndField.fields.map(fieldData => fieldData.name);
      return this._handle_result(); //return a pointer
    }
  }
  else {
    throw new errors.ProgrammingError(this.chandle.errStr(this._connection._conn))
  }


}
TDengineCursor.prototype.executemany = function executemany() {

}
TDengineCursor.prototype.fetchone = function fetchone() {

}
TDengineCursor.prototype.fetchmany = function fetchmany() {

}
TDengineCursor.prototype.fetchall = function fetchall() {
  if (this._result == null || this._fields == null) {
    throw new errors.OperationalError("Invalid use of fetchall, either result or fields from query are null");
  }
  let data = [];
  this._rowcount = 0;
  let k = 0;
  while(true) {
    k+=1;
    let blockAndRows = this.chandle.fetchBlock(this._result, this._fields);
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
  this._connection.clear_result_set();
  this.data = data;
  return data; //data[i]  returns the ith row, with all the data
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
  this.data = null;
  this.fieldNames = null;
}
TDengineCursor.prototype._handle_result = function _handle_result() {
  this._description = [];
  for (let field of this._fields) {
    this._description.push([field.name, field.type, null, null, null, null, false]);
  }
  return this._result;
}
