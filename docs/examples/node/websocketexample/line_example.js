const taos = require("@tdengine/websocket");

let influxdbData = ["meters,location=California.LosAngeles,groupId=2 current=11.8,voltage=221,phase=0.28 1648432611249",
    "meters,location=California.LosAngeles,groupId=2 current=13.4,voltage=223,phase=0.29 1648432611250",
    "meters,location=California.LosAngeles,groupId=3 current=10.8,voltage=223,phase=0.29 1648432611249"];

let jsonData = ["{\"metric\": \"meter_current\",\"timestamp\": 1626846402,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}",
    "{\"metric\": \"meter_current\",\"timestamp\": 1626846403,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1002\"}}",
    "{\"metric\": \"meter_current\",\"timestamp\": 1626846404,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1003\"}}"]

let telnetData = ["meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
    "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
    "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3"];

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
        await wsSql.schemalessInsert(jsonData, taos.SchemalessProto.OpenTSDBJsonFormatProtocol, taos.Precision.SECONDS, ttl);
        await wsSql.schemalessInsert(telnetData, taos.SchemalessProto.OpenTSDBTelnetLineProtocol, taos.Precision.MILLI_SECONDS, ttl);
    }
    catch (err) {
        console.error(err.code, err.message);
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