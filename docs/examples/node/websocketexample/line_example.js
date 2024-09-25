const taos = require("@tdengine/websocket");

let influxdbData = ["meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"];
let jsonData = ["{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"]
let telnetData = ["metric_telnet 1707095283260 4 host=host0 interface=eth0"];

async function createConnect() {
    let dsn = 'ws://localhost:6041'
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root');
    conf.setPwd('taosdata');
    let wsSql = await taos.sqlConnect(conf);
    await wsSql.exec('CREATE DATABASE IF NOT EXISTS power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;');
    await wsSql.exec('USE power');
    return wsSql;
}

async function test() {
    let wsSql = null;
    let wsRows = null;
    let ttl = 0;
    try {
        wsSql = await createConnect()
        await wsSql.schemalessInsert(influxdbData, taos.SchemalessProto.InfluxDBLineProtocol, taos.Precision.MILLI_SECONDS, ttl);
        await wsSql.schemalessInsert(telnetData, taos.SchemalessProto.OpenTSDBTelnetLineProtocol, taos.Precision.MILLI_SECONDS, ttl);
        await wsSql.schemalessInsert(jsonData, taos.SchemalessProto.OpenTSDBJsonFormatProtocol, taos.Precision.SECONDS, ttl);
        console.log("Inserted data with schemaless successfully.")
    }
    catch (err) {
        console.error(`Failed to insert data with schemaless, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
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
