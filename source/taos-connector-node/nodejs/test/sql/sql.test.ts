import { WebSocketConnectionPool } from "@src/client/wsConnectorPool";
import { WSConfig } from "@src/common/config";
import { WsSql } from "@src/sql/wsSql";
import { Sleep, testPassword, testUsername, testEnterprise, testNon3360 } from "@test-helpers/utils";
import { setLevel } from "@src/common/log";

let dsn = "ws://localhost:6041";
let password1 = "Ab1!@#$%,.:?<>;~";
let password2 = "Bc%^&*()-_+=[]{}";
setLevel("debug");
beforeAll(async () => {
    let conf: WSConfig = new WSConfig(dsn);
    conf.setUser(testUsername());
    conf.setPwd(testPassword());
    let wsSql = await WsSql.open(conf);
    await wsSql.exec("drop database if exists sql_test");
    await wsSql.exec("drop database if exists sql_create");
    await wsSql.exec(`create user user1 pass '${password1}'`);
    await wsSql.exec(`create user user2 pass '${password2}'`);
    await wsSql.exec("create user token_user pass 'token_pass_1'");
    await wsSql.exec(
        "create database if not exists sql_test KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;"
    );
    await Sleep(100);
    await wsSql.exec("use sql_test");
    await wsSql.exec(
        "CREATE STABLE if not exists meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);"
    );
    await wsSql.close();
});

describe("TDWebSocket.WsSql()", () => {
    jest.setTimeout(20 * 1000);
    test("normal connect", async () => {
        let wsSql = null;
        let conf: WSConfig = new WSConfig("");
        conf.setUrl(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        conf.setDb("sql_test");
        conf.setTimezone("America/New_York");
        conf.setTimeOut(6000);
        wsSql = await WsSql.open(conf);
        expect(wsSql.state()).toBeGreaterThan(0);
        let wsRows = await wsSql.query("select timezone()");
        while (await wsRows.next()) {
            let result = wsRows.getData();
            console.log(result);
            expect(result).toBeTruthy();
            expect(JSON.stringify(result)).toContain("America/New_York");
        }
        await wsSql.close();
    });

    test("special characters connect1", async () => {
        let wsSql = null;
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser("user1");
        conf.setPwd(password1);
        wsSql = await WsSql.open(conf);
        expect(wsSql.state()).toBeGreaterThan(0);
        let version = await wsSql.version();
        expect(version).not.toBeNull();
        expect(version).not.toBeUndefined();
        await wsSql.close();
    });
    test("special characters connect2", async () => {
        let wsSql = null;
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser("user2");
        conf.setPwd(password2);
        wsSql = await WsSql.open(conf);
        expect(wsSql.state()).toBeGreaterThan(0);
        let version = await wsSql.version();
        expect(version).not.toBeNull();
        expect(version).not.toBeUndefined();
        await wsSql.close();
    });

    test("connect db with error", async () => {
        expect.assertions(1);
        let wsSql = null;
        try {
            let conf: WSConfig = new WSConfig(dsn);
            conf.setUser(testUsername());
            conf.setPwd(testPassword());
            conf.setDb("jest");
            wsSql = await WsSql.open(conf);
        } catch (e) {
            let err: any = e;
            expect(err.message).toMatch("Database not exist");
        } finally {
            if (wsSql) {
                await wsSql.close();
            }
        }
    });

    test("connect url", async () => {
        let url =
            `ws://${testUsername()}:${testPassword()}@localhost:6041/information_schema?timezone=Asia/Shanghai`;
        let conf: WSConfig = new WSConfig(url);
        let wsSql = await WsSql.open(conf);
        let version = await wsSql.version();
        console.log(version);
        expect(version).toBeTruthy();
        let wsRows = await wsSql.query("select timezone()");
        while (await wsRows.next()) {
            let result = wsRows.getData();
            console.log(result);
            expect(result).toBeTruthy();
            expect(JSON.stringify(result)).toContain("Asia/Shanghai");
        }
        await wsSql.close();
    });

    test("get taosc version", async () => {
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let version = await wsSql.version();
        await wsSql.close();
        console.log(version);
        expect(version).toBeTruthy();
    });

    test("show databases", async () => {
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let taosResult = await wsSql.exec("show databases");
        await wsSql.close();
        console.log(taosResult);
        expect(taosResult).toBeTruthy();
    });

    test("create databases", async () => {
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let taosResult = await wsSql.exec(
            "create database if not exists sql_create KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;"
        );
        await wsSql.close();
        console.log(taosResult);
        expect(taosResult).toBeTruthy();
    });

    test("create stable", async () => {
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let taosResult = await wsSql.exec("use sql_test");
        console.log(taosResult);
        expect(taosResult).toBeTruthy();

        taosResult = await wsSql.exec(
            "CREATE STABLE if not exists meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);"
        );
        await wsSql.close();
        console.log(taosResult);
        expect(taosResult).toBeTruthy();
    });

    test("insert recoder", async () => {
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let taosResult = await wsSql.exec("use sql_test");
        console.log(taosResult);
        expect(taosResult).toBeTruthy();

        taosResult = await wsSql.exec("describe meters");
        console.log(taosResult);

        taosResult = await wsSql.exec(
            'INSERT INTO d1001 USING meters (location, groupid) TAGS ("California", 3) VALUES (NOW, 10.2, 219, 0.32)'
        );
        console.log(taosResult);
        expect(taosResult.getAffectRows()).toBeGreaterThanOrEqual(1);
        await wsSql.close();
    });

    test("query sql", async () => {
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let taosResult = await wsSql.exec("use sql_test");
        console.log(taosResult);
        expect(taosResult).toBeTruthy();
        for (let i = 0; i < 10; i++) {
            let wsRows = await wsSql.query("select * from meters limit 3");
            expect(wsRows).toBeTruthy();
            let meta = wsRows.getMeta();
            expect(meta).toBeTruthy();
            console.log("wsRow:meta:=>", meta);
            while (await wsRows.next()) {
                let result = await wsRows.getData();
                expect(result).toBeTruthy();
            }
            await wsRows.close();
        }

        await wsSql.close();
    });

    test("query sql no getdata", async () => {
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        let wsSql = await WsSql.open(conf);
        let taosResult = await wsSql.exec("use sql_test");
        console.log(taosResult);
        expect(taosResult).toBeTruthy();
        let wsRows = await wsSql.query("select * from meters");
        await wsRows.close();
        await wsSql.close();
    });

    test("timestamp order check", async () => {
        const conf: WSConfig = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());

        const wsSql = await WsSql.open(conf);
        await wsSql.exec("use sql_test");
        await wsSql.exec("drop table if exists t_order");
        await wsSql.exec("create table t_order (ts timestamp, c1 int)");
        await wsSql.exec("insert into t_order values (1726803356466, 1)");
        await wsSql.exec("insert into t_order values (1726803357466, 2)");
        await wsSql.exec("insert into t_order values (1726803358466, 3)");

        const expectRowsAsc = [
            [1726803356466n, 1],
            [1726803357466n, 2],
            [1726803358466n, 3],
        ];
        const expectRowsDesc = expectRowsAsc.slice().reverse();

        const actualRowsAsc = [];
        const actualRowsDesc = [];

        const rowsAsc = await wsSql.query("select * from t_order order by ts asc");
        while (await rowsAsc.next()) {
            const data = rowsAsc.getData();
            if (!data) break;
            actualRowsAsc.push(data);
        }
        await rowsAsc.close();
        expect(actualRowsAsc).toEqual(expectRowsAsc);

        const rowsDesc = await wsSql.query("select * from t_order order by ts desc");
        while (await rowsDesc.next()) {
            const data = rowsDesc.getData();
            if (!data) break;
            actualRowsDesc.push(data);
        }
        await rowsDesc.close();
        expect(actualRowsDesc).toEqual(expectRowsDesc);

        await wsSql.close();
    });

    testEnterprise("connect with token", async () => {
        const conf = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const wsSql = await WsSql.open(conf);
        const wsRows = await wsSql.query("create token test_bearer_token from user token_user");
        await wsRows.next();
        const token = wsRows.getData()?.[0] as string;
        expect(token).toBeTruthy();
        await wsRows.close();
        await wsSql.close();

        const assertServerVersionWithConfig = async (config: WSConfig) => {
            const client = await WsSql.open(config);
            const rows = await client.query("select server_version()");
            await rows.next();
            const version = rows.getData()?.[0] as string;
            expect(version).toBeTruthy();
            await rows.close();
            await client.close();
        };

        const conf1 = new WSConfig(dsn);
        conf1.setBearerToken(token);
        await assertServerVersionWithConfig(conf1);

        const conf2 = new WSConfig("ws://localhost:6041?bearer_token=" + token);
        await assertServerVersionWithConfig(conf2);
    });

    testEnterprise("connect with invalid token", async () => {
        let conf = new WSConfig("ws://localhost:6041?bearer_token=invalid_token");
        await expect(WsSql.open(conf)).rejects.toMatchObject({
            message: expect.stringMatching(/invalid token/i),
        });

        conf = new WSConfig("ws://localhost:6041");
        conf.setBearerToken("invalid_token1");
        await expect(WsSql.open(conf)).rejects.toMatchObject({
            message: expect.stringMatching(/invalid token/i),
        });

        conf = new WSConfig("ws://localhost:6041");
        conf.setBearerToken(" ");
        await expect(WsSql.open(conf)).rejects.toMatchObject({
            message: expect.stringMatching(/invalid token/i),
        });

        conf = new WSConfig("ws://localhost:6041?bearer_token=");
        await expect(WsSql.open(conf)).rejects.toMatchObject({
            message: expect.stringMatching(/invalid url/i),
        });

        conf = new WSConfig("ws://localhost:6041");
        conf.setBearerToken("");
        await expect(WsSql.open(conf)).rejects.toMatchObject({
            message: expect.stringMatching(/invalid url/i),
        });
    });

    testNon3360("connector version info", async () => {
        const conf = new WSConfig(dsn);
        conf.setUser(testUsername());
        conf.setPwd(testPassword());
        const wsSql = await WsSql.open(conf);
        await Sleep(2000);
        const wsRows = await wsSql.query("show connections");
        let hasNodejsWs = false;
        while (await wsRows.next()) {
            const data = wsRows.getData();
            if (Array.isArray(data) && data.some(v => typeof v === "string" && v.includes("nodejs-ws"))) {
                hasNodejsWs = true;
                break;
            }
        }
        expect(hasNodejsWs).toBe(true);
        await wsRows.close();
        await wsSql.close();
    });
});

afterAll(async () => {
    let conf: WSConfig = new WSConfig(dsn);
    conf.setUser(testUsername());
    conf.setPwd(testPassword());
    let wsSql = await WsSql.open(conf);
    await wsSql.exec("drop database sql_test");
    await wsSql.exec("drop database sql_create");
    await wsSql.exec("drop user user1");
    await wsSql.exec("drop user user2");
    await wsSql.exec("drop user token_user");
    await wsSql.close();
    WebSocketConnectionPool.instance().destroyed();
});
