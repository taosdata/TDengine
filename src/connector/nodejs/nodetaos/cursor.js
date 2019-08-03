const ref = require('ref');
require('./globalfunc.js')
const CTaosInterface = require('./cinterface')
const errors = require ('./error')
const TaosQuery = require('./taosquery')
const { PerformanceObserver, performance } = require('perf_hooks');
module.exports = TDengineCursor;

/**
 * @typedef {Object} Buffer - A Node.JS buffer. Please refer to {@link https://nodejs.org/api/buffer.html} for more details
 * @global
 */

/**
 * @class TDengineCursor
 * @classdesc  The TDengine Cursor works directly with the C Interface which works with TDengine. It refrains from
 * returning parsed data and majority of functions return the raw data such as cursor.fetchall() as compared to the TaosQuery class which
 * has functions that "prettify" the data and add more functionality and can be used through cursor.query("your query"). Instead of
 * promises, the class and its functions use callbacks.
 * @param {TDengineConnection} - The TDengine Connection this cursor uses to interact with TDengine
 * @property {data} - Latest retrieved data from query execution. It is an empty array by default
 * @property {fields} - Array of the field objects in order from left to right of the latest data retrieved
 * @since 1.0.0
 */
function TDengineCursor(connection=null) {
  //All parameters are store for sync queries only.
  this._description = null;
  this._rowcount = -1;
  this._connection = null;
  this._result = null;
  this._fields = null;
  this.data = [];
  this.fields = null;
  this._chandle = new CTaosInterface(null, true); //pass through, just need library loaded.
  if (connection != null) {
    this._connection = connection
  }

}
/**
 * Get the description of the latest query
 * @since 1.0.0
 * @return {string} Description
 */
TDengineCursor.prototype.description = function description() {
  return this._description;
}
/**
 * Get the row counts of the latest query
 * @since 1.0.0
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
 * @since 1.0.0
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
 * Create a TaosQuery object to perform a query to TDengine and retrieve data.
 * @param {string} operation - The operation string to perform a query on
 * @param {boolean} execute - Whether or not to immedietely perform the query. Default is false.
 * @return {TaosQuery | Promise<TaosResult>} A TaosQuery object
 * @example
 * var query = cursor.query("select count(*) from meterinfo.meters");
 * query.execute();
 * @since 1.0.6
 */
TDengineCursor.prototype.query = function query(operation, execute = false) {
  return new TaosQuery(operation, this, execute);
}

/**
 * Execute a query. Also stores all the field meta data returned from the query into cursor.fields. It is preferable to use cursor.query() to create
 * queries and execute them instead of using the cursor object directly.
 * @param {string} operation - The query operation to execute in the taos shell
 * @param {Object} options - Execution options object. quiet : true turns off logging from queries
 * @param {boolean} options.quiet - True if you want to surpress logging such as "Query OK, 1 row(s) ..."
 * @param {function} callback - A callback function to execute after the query is made to TDengine
 * @return {number | Buffer} Number of affected rows or a Buffer that points to the results of the query
 * @since 1.0.0
 */
TDengineCursor.prototype.execute = function execute(operation, options, callback) {
  if (operation == undefined) {
    throw new errors.ProgrammingError('No operation passed as argument');
    return null;
  }

  if (typeof options == 'function')  {
    callback = options;
  }
  if (typeof options != 'object') options = {}
  if (this._connection == null) {
    throw new errors.ProgrammingError('Cursor is not connected');
  }
  this._connection._clearResultSet();
  this._reset_result();

  let stmt = operation;
  let time = 0;
  const obs = new PerformanceObserver((items) => {
    time = items.getEntries()[0].duration;
    performance.clearMarks();
  });
  obs.observe({ entryTypes: ['measure'] });
  performance.mark('A');
  res = this._chandle.query(this._connection._conn, stmt);
  performance.mark('B');
  performance.measure('query', 'A', 'B');

  if (res == 0) {
    let fieldCount = this._chandle.fieldsCount(this._connection._conn);
    if (fieldCount == 0) {
      let affectedRowCount = this._chandle.affectedRows(this._connection._conn);
      let response = this._createAffectedResponse(affectedRowCount, time)
      if (options['quiet'] != true) {
        console.log(response);
      }
      wrapCB(callback);
      return affectedRowCount; //return num of affected rows, common with insert, use statements
    }
    else {
      let resAndField = this._chandle.useResult(this._connection._conn, fieldCount)
      this._result = resAndField.result;
      this._fields = resAndField.fields;
      this.fields = resAndField.fields;
      wrapCB(callback);
      return this._handle_result(); //return a pointer to the result
    }
  }
  else {
    throw new errors.ProgrammingError(this._chandle.errStr(this._connection._conn))
  }

}
TDengineCursor.prototype._createAffectedResponse = function (num, time) {
  return "Query OK, " + num  + " row(s) affected (" + (time * 0.001).toFixed(8) + "s)";
}
TDengineCursor.prototype._createSetResponse = function (num, time) {
  return "Query OK, " + num  + " row(s) in set (" + (time * 0.001).toFixed(8) + "s)";
}
TDengineCursor.prototype.executemany = function executemany() {

}
TDengineCursor.prototype.fetchone = function fetchone() {

}
TDengineCursor.prototype.fetchmany = function fetchmany() {

}
/**
 * Fetches all results from a query and also stores results into cursor.data. It is preferable to use cursor.query() to create
 * queries and execute them instead of using the cursor object directly.
 * @param {function} callback - callback function executing on the complete fetched data
 * @return {Array<Array>} The resultant array, with entries corresponding to each retreived row from the query results, sorted in
 * order by the field name ordering in the table.
 * @since 1.0.0
 * @example
 * cursor.execute('select * from db.table');
 * var data = cursor.fetchall(function(results) {
 *   results.forEach(row => console.log(row));
 * })
 */
TDengineCursor.prototype.fetchall = function fetchall(options, callback) {
  if (this._result == null || this._fields == null) {
    throw new errors.OperationalError("Invalid use of fetchall, either result or fields from query are null. First execute a query first");
  }

  let data = [];
  this._rowcount = 0;
  //let nodetime = 0;
  let time = 0;
  const obs = new PerformanceObserver((items) => {
    time += items.getEntries()[0].duration;
    performance.clearMarks();
  });
  /*
  const obs2 = new PerformanceObserver((items) => {
    nodetime += items.getEntries()[0].duration;
    performance.clearMarks();
  });
  obs2.observe({ entryTypes: ['measure'] });
  performance.mark('nodea');
  */
  obs.observe({ entryTypes: ['measure'] });
  performance.mark('A');
  while(true) {

    let blockAndRows = this._chandle.fetchBlock(this._result, this._fields);

    let block = blockAndRows.blocks;
    let num_of_rows = blockAndRows.num_of_rows;

    if (num_of_rows == 0) {
      break;
    }
    this._rowcount += num_of_rows;
    for (let i = 0; i < num_of_rows; i++) {
      data.push([]);
      let rowBlock = new Array(this._fields.length);
      for (let j = 0; j < this._fields.length; j++) {
        rowBlock[j] = block[j][i];
      }
      data[data.length-1] = (rowBlock);
    }

  }
  performance.mark('B');
  performance.measure('query', 'A', 'B');
  let response = this._createSetResponse(this._rowcount, time)
  console.log(response);

  this._connection._clearResultSet();
  let fields = this.fields;
  this._reset_result();
  this.data = data;
  this.fields = fields;

  wrapCB(callback, data);

  return data;
}
/**
 * Asynchrnously execute a query to TDengine. NOTE, insertion requests must be done in sync if on the same table.
 * @param {string} operation - The query operation to execute in the taos shell
 * @param {Object} options - Execution options object. quiet : true turns off logging from queries
 * @param {boolean} options.quiet - True if you want to surpress logging such as "Query OK, 1 row(s) ..."
 * @param {function} callback - A callback function to execute after the query is made to TDengine
 * @return {number | Buffer} Number of affected rows or a Buffer that points to the results of the query
 * @since 1.0.0
 */
TDengineCursor.prototype.execute_a = function execute_a (operation, options, callback, param) {
  if (operation == undefined) {
    throw new errors.ProgrammingError('No operation passed as argument');
    return null;
  }
  if (typeof options == 'function')  {
    //we expect the parameter after callback to be param
    param = callback;
    callback = options;
  }
  if (typeof options != 'object') options = {}
  if (this._connection == null) {
    throw new errors.ProgrammingError('Cursor is not connected');
  }
  if (typeof callback != 'function') {
    throw new errors.ProgrammingError("No callback function passed to execute_a function");
  }
  // Async wrapper for callback;
  var cr = this;

  let asyncCallbackWrapper = function (param2, res2, resCode) {
    if (typeof callback == 'function') {
      callback(param2, res2, resCode);
    }

    if (resCode >= 0) {
      let fieldCount = cr._chandle.numFields(res2);
      if (fieldCount == 0) {
        //get affect fields count
        cr._chandle.freeResult(res2); //result will no longer be needed
      }
      else {
        return res2;
      }

    }
    else {
      //new errors.ProgrammingError(this._chandle.errStr(this._connection._conn))
      //how to get error by result handle?
      throw new errors.ProgrammingError("Error occuring with use of execute_a async function. Status code was returned with failure");
    }
  }
  this._connection._clearResultSet();
  let stmt = operation;
  let time = 0;

  // Use ref module to write to buffer in cursor.js instead of taosquery to maintain a difference in levels. Have taosquery stay high level
  // through letting it pass an object as param
  var buf = ref.alloc('Object');
  ref.writeObject(buf, 0, param);
  const obs = new PerformanceObserver((items) => {
    time = items.getEntries()[0].duration;
    performance.clearMarks();
  });
  obs.observe({ entryTypes: ['measure'] });
  performance.mark('A');
  this._chandle.query_a(this._connection._conn, stmt, asyncCallbackWrapper, buf);
  performance.mark('B');
  performance.measure('query', 'A', 'B');
  return param;


}
/**
 * Fetches all results from an async query. It is preferable to use cursor.query_a() to create
 * async queries and execute them instead of using the cursor object directly.
 * @param {Object} options - An options object containing options for this function
 * @param {function} callback - callback function that is callbacked on the COMPLETE fetched data (it is calledback only once!).
 * Must be of form function (param, result, rowCount, rowData)
 * @param {Object} param - A parameter that is also passed to the main callback function. Important! Param must be an object, and the key "data" cannot be used
 * @return {{param:Object, result:buffer}} An object with the passed parameters object and the buffer instance that is a pointer to the result handle.
 * @since 1.2.0
 * @example
 * cursor.execute('select * from db.table');
 * var data = cursor.fetchall(function(results) {
 *   results.forEach(row => console.log(row));
 * })
 */
TDengineCursor.prototype.fetchall_a = function fetchall_a(result, options, callback, param = {}) {
  if (typeof options == 'function')  {
    //we expect the parameter after callback to be param
    param = callback;
    callback = options;
  }
  if (typeof options != 'object') options = {}
  if (this._connection == null) {
    throw new errors.ProgrammingError('Cursor is not connected');
  }
  if (typeof callback != 'function') {
    throw new errors.ProgrammingError('No callback function passed to fetchall_a function')
  }
  if (param.data) {
    throw new errors.ProgrammingError("You aren't allowed to set the key 'data' for the parameters object");
  }
  let buf = ref.alloc('Object');
  param.data = [];
  var cr = this;

  // This callback wrapper accumulates the data from the fetch_rows_a function from the cinterface. It is accumulated by passing the param2
  // object which holds accumulated data in the data key.
  let asyncCallbackWrapper = function asyncCallbackWrapper(param2, result2, numOfRows2, rowData) {
    param2 = ref.readObject(param2); //return the object back from the pointer
    // Keep fetching until now rows left.
    if (numOfRows2 > 0) {
      let buf2 = ref.alloc('Object');
      param2.data.push(rowData);
      ref.writeObject(buf2, 0, param2);
      cr._chandle.fetch_rows_a(result2, asyncCallbackWrapper, buf2);
    }
    else {

      let finalData = param2.data;
      let fields = cr._chandle.fetchFields_a(result2);
      let data = [];
      for (let i = 0; i < finalData.length; i++) {
        let num_of_rows = finalData[i][0].length; //fetched block number i;
        let block = finalData[i];
        for (let j = 0; j < num_of_rows; j++) {
          data.push([]);
          let rowBlock = new Array(fields.length);
          for (let k = 0; k < fields.length; k++) {
            rowBlock[k] = block[k][j];
          }
          data[data.length-1] = rowBlock;
        }
      }
      cr._chandle.freeResult(result2); // free result, avoid seg faults and mem leaks!
      callback(param2, result2, numOfRows2, {data:data,fields:fields});
    }
  }
  ref.writeObject(buf, 0, param);
  param = this._chandle.fetch_rows_a(result, asyncCallbackWrapper, buf); //returned param
  return {param:param,result:result};
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
  this.fields = null;
}
TDengineCursor.prototype._handle_result = function _handle_result() {
  this._description = [];
  for (let field of this._fields) {
    this._description.push([field.name, field.type]);
  }
  return this._result;
}
