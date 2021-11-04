var TDengineConnection = require('./nodetaos/connection.js')
const constants = require('./nodetaos/constants');
module.exports = {
  connect: function (connection = {}) {
    return new TDengineConnection(connection);
  },
  SCHEMALESS_PROTOCOL: constants.SCHEMALESS_PROTOCOL,
  SCHEMALESS_PRECISION: constants.SCHEMALESS_PRECISION
}