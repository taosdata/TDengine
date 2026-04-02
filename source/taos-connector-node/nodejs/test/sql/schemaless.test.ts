import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { Precision, SchemalessProto } from "@src/sql/wsProto";
import { WsSql } from "@src/sql/wsSql";
import { testPassword, testUsername } from "@test-helpers/utils";

let dns = "ws://localhost:6041";

beforeAll(async () => {
    let conf: WSConfig = new WSConfig(dns);
    conf.setUser(testUsername());
    conf.setPwd(testPassword());

    let wsSql = await WsSql.open(conf);
    await wsSql.exec("drop database if exists power_schemaless;");
    await wsSql.exec(
        "create database if not exists power_schemaless KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;"
    );
    await wsSql.exec(
        "CREATE STABLE if not exists power_schemaless.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);"
    );
    await wsSql.close();
});

describe("TDWebSocket.WsSchemaless()", () => {
    jest.setTimeout(20 * 1000);
    let influxdbData =
        'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000';
    let telnetData = "stb0_0 1626006833 4 host=host0 interface=eth0";
    let jsonData =
        '{"metric": "meter_current","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}';

    test("normal connect", async () => {
        let conf: WSConfig = new WSConfig(dns);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        conf.setDb("power_schemaless");
        let wsSchemaless = await WsSql.open(conf);
        expect(wsSchemaless.state()).toBeGreaterThan(0);
        await wsSchemaless.close();
    });

    test("connect db with error", async () => {
        expect.assertions(1);
        let wsSchemaless = null;
        try {
            let conf: WSConfig = new WSConfig(dns);
            conf.setUser(testUsername());
            conf.setPwd(testPassword());
            conf.setDb("jest");
            wsSchemaless = await WsSql.open(conf);
        } catch (e: any) {
            console.log(e);
            expect(e.message).toMatch("Database not exist");
        } finally {
            if (wsSchemaless) {
                await wsSchemaless.close();
            }
        }
    });

    test("normal insert", async () => {
        let conf: WSConfig = new WSConfig(dns);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        conf.setDb("power_schemaless");
        let wsSchemaless = await WsSql.open(conf);
        expect(wsSchemaless.state()).toBeGreaterThan(0);
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
        await wsSchemaless.close();
    });

    test("normal wsSql insert", async () => {
        let conf: WSConfig = new WSConfig(dns);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        conf.setDb("power_schemaless");
        let wsSchemaless = await WsSql.open(conf);
        expect(wsSchemaless.state()).toBeGreaterThan(0);
        await wsSchemaless.schemalessInsert(
            [influxdbData],
            SchemalessProto.InfluxDBLineProtocol,
            Precision.NOT_CONFIGURED,
            0
        );
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

        await wsSchemaless.close();
    });

    test("SchemalessProto error", async () => {
        let conf: WSConfig = new WSConfig(dns);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        conf.setDb("power_schemaless");
        let wsSchemaless = await WsSql.open(conf);
        expect(wsSchemaless.state()).toBeGreaterThan(0);
        try {
            await wsSchemaless.schemalessInsert(
                [influxdbData],
                SchemalessProto.OpenTSDBTelnetLineProtocol,
                Precision.NANO_SECONDS,
                0
            );
        } catch (e: any) {
            expect(e.message).toMatch("parse timestamp failed");
        }

        await wsSchemaless.close();
    });
});

afterAll(async () => {
    let conf: WSConfig = new WSConfig(dns);
    conf.setUser(testUsername());
    conf.setPwd(testPassword());

    let wsSql = await WsSql.open(conf);
    await wsSql.exec("drop database if exists power_schemaless;");
    await wsSql.close();
    WebSocketConnectionPool.instance().destroyed();
});
