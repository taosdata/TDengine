import { WSConfig } from "../src/common/config";
import { sqlConnect, destroy, setLogLevel } from "../src";

let dsn = "ws://root:taosdata@localhost:6041";
(async () => {
    let wsSql = null;
    let wsRows = null;
    let reqId = 0;
    try {
        setLogLevel("debug");
        let conf: WSConfig = new WSConfig(dsn);
        conf.setUser("root");
        conf.setPwd("taosdata");
        wsSql = await sqlConnect(conf);

        let version = await wsSql.version();
        console.log(version);

        let taosResult = await wsSql.exec("show databases", reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec(
            "create database if not exists power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;",
            reqId++
        );
        console.log(taosResult);

        taosResult = await wsSql.exec("use power", reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec(
            "CREATE STABLE if not exists meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);",
            reqId++
        );
        console.log(taosResult);

        taosResult = await wsSql.exec("describe meters", reqId++);
        console.log(taosResult);

        taosResult = await wsSql.exec(
            'INSERT INTO d1001 USING meters TAGS ("California.SanFrancisco", 3) VALUES (NOW, 10.2, 219, 0.32)',
            reqId++
        );
        console.log(taosResult);
        for (let i = 0; i < 100; i++) {
            wsRows = await wsSql.query("select * from meters", reqId++);
            let meta = wsRows.getMeta();
            console.log("wsRow:meta:=>", meta);

            while (await wsRows.next()) {
                let result = wsRows.getData();
                console.log("queryRes.Scan().then=>", result);
            }
            await wsRows.close();
        }
    } catch (err: any) {
        console.error(err.code, err.message);
    } finally {
        if (wsRows) {
            await wsRows.close();
        }
        if (wsSql) {
            await wsSql.close();
        }
        destroy();
        console.log("finish!");
    }
})();
