const taos = require("@tdengine/websocket");

let db = 'power';
let stable = 'meters';
let numOfSubTable = 10;
let numOfRow = 10;
let dsn = 'ws://localhost:6041'
function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function prepare() {

    let conf = new taos.WSConfig(dsn);
    conf.setUser('root')
    conf.setPwd('taosdata')
    conf.setDb(db)
    let wsSql = await taos.sqlConnect(conf);
    await wsSql.exec(`CREATE DATABASE IF NOT EXISTS ${db} KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;`);
    await wsSql.exec(`CREATE STABLE IF NOT EXISTS ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`);
    return wsSql
}

async function stmtInsert() {
    let stmt = null;
    let connector = null;
    try {
        connector = await prepare();
        stmt = await connector.stmtInit();
        await stmt.prepare(`INSERT INTO ? USING ${db}.${stable} (location, groupId) TAGS (?, ?) VALUES (?, ?, ?, ?)`);
        for (let i = 0; i < numOfSubTable; i++) {
            await stmt.setTableName(`d_bind_${i}`);
            let tagParams = stmt.newStmtParam();
            tagParams.setVarchar([`location_${i}`]);
            tagParams.setInt([i]);
            await stmt.setTags(tagParams);
            let timestampParams = [];
            let currentParams = [];
            let voltageParams = [];
            let phaseParams = [];
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
            console.log("Successfully inserted " + stmt.getLastAffected() + " to power.meters.");
        }
    }
    catch (err) {
        console.error(`Failed to insert to table meters using stmt, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err;
    }
    finally {
        if (stmt) {
            await stmt.close();
        }
        if (connector) {
            await connector.close();
        }
        taos.destroy();
    }
}

async function test () {
    console.log("begin stmt_example")
    await stmtInsert();
    console.log("end stmt_example")
}

test()