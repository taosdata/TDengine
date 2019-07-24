const TDengineCursor = require('./cursor')
const CTaosInterface = require('./cinterface')
module.exports = TDengineConnection;

/*
 * TDengine Connection object
 * @param {Object.<string, string>} options - Options for configuring the connection with TDengine
 * @return {TDengineConnection}
 *
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
  this.config(options)
  return this;
}

TDengineConnection.prototype.config = function config(options) {
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

TDengineConnection.prototype.close = function close() {
  return this._chandle.close(this._conn);
}
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
TDengineConnection.prototype.clear_result_set = function clear_result_set() {
  var result = this._chandle.useResult(this._conn).result;
  if (result) {
    this._chandle.freeResult(result)
  }
}
