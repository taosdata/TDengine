var TaosResult = require('./taosresult')
require('./globalfunc.js')
module.exports = TaosQuery;


/**
 * @class TaosQuery
 * @classdesc The TaosQuery class is one level above the TDengine Cursor in that it makes sure to generally return promises from functions, and wrap
 * all data with objects such as wrapping a row of data with Taos Row. This is meant to enable an higher level API that allows additional
 * functionality and save time whilst also making it easier to debug and enter less problems with the use of promises.
 * @param {string} query - Query to construct object from
 * @param {TDengineCursor} cursor - The cursor from which this query will execute from
 * @param {boolean} execute - Whether or not to immedietely execute the query synchronously and fetch all results. Default is false.
 * @property {string} query - The current query in string format the TaosQuery object represents
 * @return {TaosQuery}
 * @since 1.0.6
 */
function TaosQuery(query = "", cursor = null, execute = false) {
  this.query = query;
  this._cursor = cursor;
  if (execute == true) {
    return this.execute();
  }
  return this;
}

/**
 * Executes the query object and returns a Promise
 * @memberof TaosQuery
 * @return {Promise<TaosResult>} A promise that resolves with a TaosResult object, or rejects with an error
 * @since 1.0.6
 */
TaosQuery.prototype.execute = async function execute() {
  var taosQuery = this; //store the current instance of taosQuery to avoid async issues?
  var executionPromise = new Promise(function(resolve, reject) {
    let data = [];
    let fields = [];
    let result;
    try {
      taosQuery._cursor.execute(taosQuery.query);
      if (taosQuery._cursor._fields) fields = taosQuery._cursor._fields;
      if (taosQuery._cursor._result != null) data = taosQuery._cursor.fetchall();
      result = new TaosResult(data, fields)
    }
    catch(err) {
      reject(err);
    }
    resolve(result)

  });
  return executionPromise;
}

/**
 * Executes the query object asynchronously and returns a Promise. Completes query to completion.
 * @memberof TaosQuery
 * @param {Object} options - Execution options
 * @return {Promise<TaosResult>} A promise that resolves with a TaosResult object, or rejects with an error
 * @since 1.2.0
 */
TaosQuery.prototype.execute_a = async function execute_a(options = {}) {
  var executionPromise =  new Promise( (resolve, reject) => {

  });
  var fres;
  var frej;
  var fetchPromise =  new Promise( (resolve, reject) => {
    fres = resolve;
    frej = reject;
  });
  let asyncCallbackFetchall = async function(param, res, numOfRows, blocks) {
    if (numOfRows > 0) {
      // Likely a query like insert
      fres();
    }
    else {
      fres(new TaosResult(blocks.data, blocks.fields));
    }
  }
  let asyncCallback = async function(param, res, code) {
    //upon success, we fetchall results
    this._cursor.fetchall_a(res, options, asyncCallbackFetchall, {});
  }
  this._cursor.execute_a(this.query, asyncCallback.bind(this), {});
  return fetchPromise;
}

/**
 * Bind arguments to the query and automatically parses them into the right format
 * @param {array | ...args} args - A number of arguments to bind to each ? in the query
 * @return {TaosQuery}
 * @example
 * // An example of binding a javascript date and a number to a query
 * var query = cursor.query("select count(*) from meterinfo.meters where ts <= ? and areaid = ?").bind(new Date(), 3);
 * var promise1 = query.execute();
 * promise1.then(function(result) {
 *   result.pretty(); // Log the prettified version of the results.
 * });
 * @since 1.0.6
 */
TaosQuery.prototype.bind = function bind(f, ...args) {
  if (typeof f == 'object' && f.constructor.name != 'Array') args.unshift(f); //param is not an array object
  else if (typeof f != 'object') args.unshift(f);
  else { args = f; }
  args.forEach(function(arg) {
    if (arg.constructor.name == 'TaosTimestamp') arg = "\"" + arg.toTaosString() + "\"";
    else if (arg.constructor.name == 'Date') arg = "\"" + toTaosTSString(arg) + "\"";
    else if (typeof arg == 'string') arg = "\"" + arg + "\"";
    this.query = this.query.replace(/\?/,arg);
  }, this);
  return this;
}
