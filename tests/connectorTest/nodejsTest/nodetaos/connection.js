const TDengineCursor = require('./cursor')
const CTaosInterface = require('./cinterface')
module.exports = TDengineConnection;

/**
 * TDengine Connection Class
 * @param {object} options - Options for configuring the connection with TDengine
 * @return {TDengineConnection}
 * @class TDengineConnection
 * @constructor
 * @example
 * //Initialize a new connection
 * var conn = new TDengineConnection({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
 *
 */
function TDengineConnection(options) {
  this._conn = null;
  this._host = null;
  this._user = "root"; //The default user
  this._password = "taosdata"; //The default password
  this._database = null;
  this._port = 0;
  this._config = null;
  this._chandle = null;
  this._configConn(options)
  return this;
}
/**
 * Configure the connection to TDengine
 * @private
 * @memberof TDengineConnection
 */
TDengineConnection.prototype._configConn = function _configConn(options) {
  if (options['host']) {
    this._host = options['host'];
  }
  if (options['user']) {
    this._user = options['user'];
  }
  if (options['password']) {
    this._password = options['password'];
  }
  if (options['database']) {
    this._database = options['database'];
  }
  if (options['port']) {
    this._port = options['port'];
  }
  if (options['config']) {
    this._config = options['config'];
  }
  this._chandle = new CTaosInterface(this._config);
  this._conn = this._chandle.connect(this._host, this._user, this._password, this._database, this._port);
}
/** Close the connection to TDengine */
TDengineConnection.prototype.close = function close() {
  this._chandle.close(this._conn);
}
/**
 * Initialize a new cursor to interact with TDengine with
 * @return {TDengineCursor}
 */
TDengineConnection.prototype.cursor = function cursor() {
  //Pass the connection object to the cursor
  return new TDengineCursor(this);
}
TDengineConnection.prototype.commit = function commit() {
  return this;
}
TDengineConnection.prototype.rollback = function rollback() {
  return this;
}
/**
 * Clear the results from connector
 * @private
 */
/*
 TDengineConnection.prototype._clearResultSet = function _clearResultSet() {
  var result = this._chandle.useResult(this._conn).result;
  if (result) {
    this._chandle.freeResult(result)
  }
}
*/
