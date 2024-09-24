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


async function createConnect() {
    let dsn = 'ws://' + host + ':6041'
    console.log(dsn)
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root')
    conf.setPwd('taosdata')
    return await taos.sqlConnect(conf);
}

async function test() {
    let wsSql = null;
    let wsRows = null;
    let reqId = 0;
    try {
        wsSql = await createConnect()
        let version = await wsSql.version();
        console.log(version);
        let taosResult = await wsSql.exec('SHOW DATABASES', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('CREATE DATABASE IF NOT EXISTS power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('USE power', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('CREATE STABLE IF NOT EXISTS meters (_ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('DESCRIBE meters', reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec('INSERT INTO d1001 USING meters (location, groupId) TAGS ("California.SanFrancisco", 3) VALUES (NOW, 10.2, 219, 0.32)', reqId++);
        console.log(taosResult);
        
        wsRows = await wsSql.query('SELECT * FROM meters', reqId++);
        let meta = wsRows.getMeta();
        console.log("wsRow:meta:=>", meta);

        while (await wsRows.next()) {
            let result = wsRows.getData();
            console.log('queryRes.Scan().then=>', result);
        }

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