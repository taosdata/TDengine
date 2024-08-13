// ANCHOR: createConnect
const taos = require("@tdengine/websocket");

let dsn = 'ws://localhost:6041';
async function createConnect() {

    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        conf.setDb('power');
        conn = await taos.sqlConnect(conf); 
        console.log("Connected to " + dsn + " successfully.");
        return conn;      
    } catch (err) {
        console.log("Failed to connect to " + dns + "; ErrCode:" + err.code + "; ErrMessage: " + err.message);
        throw err;
    }

}
// ANCHOR_END: createConnect

// ANCHOR: create_db_and_table
async function createDbAndTable() {
    let wsSql = null;
    try {
        wsSql = await createConnect();
        // create database
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS power');
        console.log("Create database power successfully.");
        // create table
        await wsSql.exec('CREATE STABLE IF NOT EXISTS power.meters ' + 
        '(_ts timestamp, current float, voltage int, phase float) ' +
        'TAGS (location binary(64), groupId int);');

        console.log("Create stable power.meters successfully");
    } catch (err) {
        console.error("Failed to create db and table, url:" + dns + "; ErrCode:" + err.code + "; ErrMessage: " + err.message);
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }

}
// ANCHOR_END: create_db_and_table

// ANCHOR: insertData
async function insertData() {
    let wsSql = null
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
        console.log("Successfully inserted " + taosResult.getAffectRows() + " rows to power.meters.");
    } catch (err) {
        console.error("Failed to insert data to power.meters, url:" + dsn + "; ErrCode:" + err.code + "; ErrMessage: " + err.message);
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
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log('ts: ' + row[0] + ', current: ' +  row[1] + ', location:  ' + row[2]);
        }
    }
    catch (err) {
        console.error("Failed to query data from power.meters, url:" + dsn + " ; ErrCode:" + err.code + "; ErrMessage: " + err.message);
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
async function sqlWithReqid() {
    let wsRows = null;
    let wsSql = null;
    let reqId = 1;
    try {
        wsSql = await createConnect();
        wsRows = await wsSql.query('SELECT ts, current, location FROM power.meters limit 100', reqId);
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log('ts: ' + row[0] + ', current: ' +  row[1] + ', location:  ' + row[2]);
        }
    }
    catch (err) {
        console.error("Failed to execute sql with reqId: " + reqId + ", ErrCode:" + err.code + "; ErrMessage: " + err.message);
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