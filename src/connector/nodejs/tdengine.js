var TDengineConnection = require('./nodetaos/connection.js')
module.exports.connect = function (connection=null) {
  return new TDengineConnection(connection);
}
