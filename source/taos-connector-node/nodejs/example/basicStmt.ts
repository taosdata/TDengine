import { WSConfig } from "../src/common/config";
import { destroy, sqlConnect } from "../src";

let db = "power";
let stable = "meters";
let numOfSubTable = 10;
let numOfRow = 10;

function getRandomInt(min: number, max: number): number {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

let dsn = "ws://root:taosdata@localhost:6041";
async function Prepare() {
    let conf: WSConfig = new WSConfig(dsn);
    let wsSql = await sqlConnect(conf);
    await wsSql.exec(
        `create database if not exists ${db} KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;`
    );
    await wsSql.exec(
        `CREATE STABLE if not exists ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`
    );
    wsSql.close();
}

(async () => {
    let stmt = null;
    let connector = null;
    try {
        await Prepare();
        let wsConf = new WSConfig(dsn);
        wsConf.setDb(db);
        connector = await sqlConnect(wsConf);
        stmt = await connector.stmtInit();
        await stmt.prepare(
            `INSERT INTO ? USING ${db}.${stable} (location, groupId) TAGS (?, ?) VALUES (?, ?, ?, ?)`
        );

        for (let i = 0; i < numOfSubTable; i++) {
            await stmt.setTableName(`d_bind_${i}`);
            let tagParams = stmt.newStmtParam();
            tagParams.setVarchar([`location_${i}`]);
            tagParams.setInt([i]);
            await stmt.setTags(tagParams);
            let timestampParams: number[] = [];
            let currentParams: number[] = [];
            let voltageParams: number[] = [];
            let phaseParams: number[] = [];
            const currentMillis = new Date().getTime();
            for (let j = 0; j < numOfRow; j++) {
                timestampParams.push(currentMillis + j);
                currentParams.push(Math.random() * 30);
                voltageParams.push(getRandomInt(100, 300));
                phaseParams.push(Math.random());
            }

            let bindParams = stmt.newStmtParam();
            bindParams.setTimestamp(timestampParams);
            bindParams.setFloat(currentParams);
            bindParams.setInt(voltageParams);
            bindParams.setFloat(phaseParams);
            await stmt.bind(bindParams);
            await stmt.batch();
            await stmt.exec();
            console.log(
                `d_bind_${i} insert ` + stmt.getLastAffected() + " rows."
            );
        }
    } catch (e) {
        console.error(e);
    } finally {
        if (stmt) {
            await stmt.close();
        }
        if (connector) {
            await connector.close();
        }
        destroy();
    }
})();
