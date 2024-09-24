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
    console.log("Usage: node nodejsChecker.js host=<hostname> port=<port>");
    process.exit(1);
  }

  let dbData = ["meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
  "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
  "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
  "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
  "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
  "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
  "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
  "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",];

async function createConnect() {
    let dsn = 'ws://' + host + ':6041'
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root');
    conf.setPwd('taosdata');

    return await taos.sqlConnect(conf);
}

async function test() {
    let wsSql = null;
    let wsRows = null;
    let reqId = 0;
    try {
        wsSql = await createConnect()
        await wsSql.exec('create database if not exists power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;', reqId++);
        await wsSql.exec('use power', reqId++);
        await wsSql.schemalessInsert(dbData, taos.SchemalessProto.OpenTSDBTelnetLineProtocol, taos.Precision.MILLI_SECONDS, 0);
    }
    catch (err) {
        console.error(err.code, err.message);
        process.exitCode = 1;
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
test()