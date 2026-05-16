import { WSConfig } from "../src/common/config";
import { Precision, SchemalessProto } from "../src/sql/wsProto";
import { sqlConnect, destroy, setLogLevel } from "../src";
let dsn = "ws://root:taosdata@localhost:6041";
let db = "power";
let influxdbData =
    'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000';
let telnetData = "stb0_0 1626006833 4 host=host0 interface=eth0";
let jsonData =
    '{"metric": "meter_current","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}';
const dropDB = `drop database if exists ${db}`;

async function Prepare() {
    let conf: WSConfig = new WSConfig(dsn);
    conf.setUser("root");
    conf.setPwd("taosdata");
    let wsSql = await sqlConnect(conf);
    await wsSql.exec(dropDB);

    await wsSql.exec(
        "create database if not exists power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;"
    );
    await wsSql.exec(
        "CREATE STABLE if not exists power.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);"
    );
    await wsSql.close();
}

(async () => {
    let wsSchemaless = null;
    try {
        await Prepare();
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser("root");
        conf.setPwd("taosdata");
        conf.setDb("power");
        wsSchemaless = await sqlConnect(conf);
        await wsSchemaless.schemalessInsert(
            [influxdbData],
            SchemalessProto.InfluxDBLineProtocol,
            Precision.NANO_SECONDS,
            0
        );
        await wsSchemaless.schemalessInsert(
            [telnetData],
            SchemalessProto.OpenTSDBTelnetLineProtocol,
            Precision.SECONDS,
            0
        );
        await wsSchemaless.schemalessInsert(
            [jsonData],
            SchemalessProto.OpenTSDBJsonFormatProtocol,
            Precision.SECONDS,
            0
        );
    } catch (e) {
        console.error(e);
    } finally {
        if (wsSchemaless) {
            await wsSchemaless.close();
        }
        destroy();
    }
})();
