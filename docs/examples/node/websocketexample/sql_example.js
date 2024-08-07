// ANCHOR: createConnect
const taos = require("@tdengine/websocket");

async function createConnect() {
    let dsn = 'ws://localhost:6041';
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root');
    conf.setPwd('taosdata');
    conf.setDb('power');
    return await taos.sqlConnect(conf);
}
// ANCHOR_END: createConnect

// ANCHOR: create_db_and_table
async function createDbAndTable(wsSql) {
    let wsSql = null;
    try {
        wsSql = await createConnect();
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS POWER ' +
        'KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;');

        await wsSql.exec('USE power');

        await wsSql.exec('CREATE STABLE IF NOT EXISTS meters ' + 
        '(_ts timestamp, current float, voltage int, phase float) ' +
        'TAGS (location binary(64), groupId int);');

        taosResult = await wsSql.exec('describe meters');
        console.log(taosResult);
    } catch (err) {
        console.error("Failed to create db and table, ErrCode:" + err.code + "; ErrMessage: " + err.message);
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }

}
// ANCHOR_END: create_db_and_table

// ANCHOR: insertData
async function insertData(wsSql) {
    let wsSql = null;
    try {
        wsSql = await createConnect();
        let insertQuery = "INSERT INTO " +
        "power.d1001 USING power.meters (location, groupId) TAGS('California.SanFrancisco', 2) " +
        "VALUES " +
        "(NOW + 1a, 10.30000, 219, 0.31000) " +
        "(NOW + 2a, 12.60000, 218, 0.33000) " +
        "(NOW + 3a, 12.30000, 221, 0.31000) " +
        "power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) " +
        "VALUES " +
        "(NOW + 1a, 10.30000, 218, 0.25000) ";
        taosResult = await wsSql.exec(insertQuery);
        console.log(taosResult);
    } catch (err) {
        console.error("Failed to insert data to power.meters, ErrCode:" + err.code + "; ErrMessage: " + err.message);
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }
}
// ANCHOR_END: insertData

// ANCHOR: queryData
async function queryData() {
    let wsRows = null;
    let wsSql = null;
    try {
        wsSql = await createConnect();
        wsRows = await wsSql.query('SELECT ts, current, location FROM power.meters limit 100');
        let meta = wsRows.getMeta();
        console.log("wsRow:meta:=>", meta);
        while (await wsRows.next()) {
            let result = wsRows.getData();
            console.log('queryRes.Scan().then=>', result);
        }
    }
    catch (err) {
        console.error("Failed to query data from power.meters," + err.code + "; ErrMessage: " + err.message);
    }
    finally {
        if (wsRows) {
            await wsRows.close();
        } 
        if (wsSql) {
            await wsSql.close();
        }
    }
}
// ANCHOR_END: queryData

// ANCHOR: sqlWithReqid
async function sqlWithReqid(wsSql) {

    let wsRows = null;
    let wsSql = null;
    try {
        wsSql = await createConnect();
        wsRows = await wsSql.query('SELECT ts, current, location FROM power.meters limit 100', 1);
        let meta = wsRows.getMeta();
        console.log("wsRow:meta:=>", meta);
        while (await wsRows.next()) {
            let result = wsRows.getData();
            console.log('queryRes.Scan().then=>', result);
        }
    }
    catch (err) {
        console.error("Failed to execute sql with reqId," + err.code + "; ErrMessage: " + err.message);
    }
    finally {
        if (wsRows) {
            await wsRows.close();
        } 
        if (wsSql) {
            await wsSql.close();
        }
    }
}
// ANCHOR_END: sqlWithReqid

async function test() {
    await createDbAndTable();
    await insertData();
    await queryData();
    await sqlWithReqid();
    taos.destroy(); 
}

test()