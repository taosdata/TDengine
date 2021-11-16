var TDengineConnection = require('./nodetaos/connection.js')
module.exports.connect = function (connection={}) {
  return new TDengineConnection(connection);
}
