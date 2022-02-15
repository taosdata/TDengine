var TDengineConnection = require('./nodetaos/connection.js')
const TDengineConstant = require('./nodetaos/constants.js')
const TaosBind = require('./nodetaos/taosBind')
const { TaosMultiBind } = require('./nodetaos/taosMultiBind')

module.exports = {
  connect: function (connection = {}) {
    return new TDengineConnection(connection);
  },
  SCHEMALESS_PROTOCOL: TDengineConstant.SCHEMALESS_PROTOCOL,
  SCHEMALESS_PRECISION: TDengineConstant.SCHEMALESS_PRECISION,
  TaosBind,
  TaosMultiBind,
}