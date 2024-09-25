const taos = require("@tdengine/websocket");

var host = null;
for(var i = 2; i < global.process.argv.length; i++){
  var key = global.process.argv[i].split("=")[0];
  var value = global.process.argv[i].split("=")[1];
  if("host" == key){
    host = value;
  }
}

if(host == null){
    host = 'localhost'
}

let dbData = ["{\"metric\": \"meter_current\",\"timestamp\": 1626846402,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}",
              "{\"metric\": \"meter_current\",\"timestamp\": 1626846403,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1002\"}}",
              "{\"metric\": \"meter_current\",\"timestamp\": 1626846404,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1003\"}}"]

async function createConnect() {
    let dsn = 'ws://' + host + ':6041'
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root');
    conf.setPwd('taosdata');
    conf.setDb('power');
    return await taos.sqlConnect(conf);
}

async function schemalessInsert() {
    let wsSql = null;
    let wsRows = null;
    let reqId = 0;
    try {
        wsSql = await createConnect()
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;', reqId++);
        await wsSql.schemalessInsert([dbData], taos.SchemalessProto.OpenTSDBJsonFormatProtocol, taos.Precision.SECONDS, 0);
    }
    catch (err) {
        console.error(err.code, err.message);
        throw err
    }
    finally {
        if (wsRows) {
            await wsRows.close();
        }
        if (wsSql) {
            await wsSql.close();
        }
        taos.destroy();
    }
}

async function test() {
    await schemalessInsert();
}

test()