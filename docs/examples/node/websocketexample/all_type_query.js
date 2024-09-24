const taos = require("@tdengine/websocket");

let dsn = 'ws://localhost:6041';
async function json_tag_example() {
    let wsSql = null;
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        wsSql = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        
        // create database
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS example_json_tag');
        console.log("Create database example_json_tag successfully.");
        
        // create table
        await wsSql.exec('create table if not exists example_json_tag.stb (ts timestamp, v int) tags(jt json)');

        console.log("Create stable example_json_tag.stb successfully");

        let insertQuery = 'INSERT INTO ' +
            'example_json_tag.tb1 USING example_json_tag.stb TAGS(\'{"name":"value"}\') ' +
            "values(now, 1) ";
        taosResult = await wsSql.exec(insertQuery);
        console.log("Successfully inserted " + taosResult.getAffectRows() + " rows to example_json_tag.stb.");

        let sql = 'SELECT ts, v, jt FROM example_json_tag.stb limit 100';
        wsRows = await wsSql.query(sql);
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log('ts: ' + row[0] + ', v: ' + row[1] + ', jt:  ' + row[2]);
        }

    } catch (err) {
        console.error(`Failed to create database example_json_tag or stable stb, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }

}

async function all_type_example() {
    let wsSql = null;
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        wsSql = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        
        // create database
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS all_type_example');
        console.log("Create database all_type_example successfully.");
        
        // create table
        await wsSql.exec('create table if not exists all_type_example.stb (ts timestamp, ' + 
            'int_col INT, double_col DOUBLE, bool_col BOOL, binary_col BINARY(100),' +
            'nchar_col NCHAR(100), varbinary_col VARBINARY(100), geometry_col GEOMETRY(100)) ' +
            'tags(int_tag INT, double_tag DOUBLE, bool_tag BOOL, binary_tag BINARY(100),' +
            'nchar_tag NCHAR(100), varbinary_tag VARBINARY(100), geometry_tag GEOMETRY(100));');

        console.log("Create stable all_type_example.stb successfully");

        let insertQuery = "INSERT INTO all_type_example.tb1 using all_type_example.stb "
            + "tags(1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)') "
            + "values(now, 1, 1.1, true, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)')";
        taosResult = await wsSql.exec(insertQuery);
        console.log("Successfully inserted " + taosResult.getAffectRows() + " rows to all_type_example.stb.");

        let sql = 'SELECT * FROM all_type_example.stb limit 100';
        let wsRows = await wsSql.query(sql);
        let meta = wsRows.getMeta();
        console.log("wsRow:meta:=>", meta);
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log(row);
        }

    } catch (err) {
        console.error(`Failed to create database all_type_example or stable stb, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }

}

async function test() {
    await json_tag_example()
    await all_type_example()
    taos.destroy();
}

test()
