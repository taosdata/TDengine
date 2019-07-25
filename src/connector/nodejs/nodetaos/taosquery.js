module.exports = TaosQuery;


/**
 * Constructor for TaosQuery object
 * @class TaosQuery
 * @constructor
 * @param {string} query - Query to construct object from
 * @param {TDengineCursor} - The cursor from which this query will execute from
 */
function TaosQuery(query = "", cursor = null) {
  this._query = query;
  this._cursor = cursor;
}

/**
 * Executes the query object and returns a Promise object
 * @memberof TaosQuery
 * @param {function} resultCallback - A callback function that takes the results as input
 * @param {function} metaCallback - A callback function that takes the metadata or fields as input
 * @return {Promise<results, fields>} A promise that evaluates into an object with keys "results" and "fields"
 */
TaosQuery.prototype.execute = async function execute(resultCallback, metaCallback) {
  var taosQuery = this; //store the current instance of taosQuery to avoid async issues?
  var executionPromise = new Promise(function(resolve, reject) {
    let results = [];
    try {
      taosQuery._cursor.execute(taosQuery._query);
      if (taosQuery._cursor._result != null){
        results = taosQuery._cursor.fetchall();
      }
      if (metaCallback) metaCallback(taosQuery._cursor._fields);
      if (resultCallback) resultCallback(results);
    }
    catch(err) {
      reject(err);
    }
    resolve({results:results, fields:taosQuery._cursor._fields})

  });
  return executionPromise;
}

/**
 *
 * @param {...args} args - A
 */
TaosQuery.prototype.bind = function bind(...args) {
  args.forEach(function(arg) {

  });
}
