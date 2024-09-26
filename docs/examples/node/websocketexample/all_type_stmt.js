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
        
        await wsSql.exec('use example_json_tag');

        // create table
        await wsSql.exec('create table if not exists stb (ts timestamp, v int) tags(jt json)');

        console.log("Create stable example_json_tag.stb successfully");

        let stmt = await wsSql.stmtInit();
        await stmt.prepare("INSERT INTO ? using stb tags(?) VALUES (?,?)");
        await stmt.setTableName(`tb1`);
        let tagParams = stmt.newStmtParam();
        tagParams.setJson(['{"name":"value"}'])
        await stmt.setTags(tagParams);
        let bindParams = stmt.newStmtParam();
        const currentMillis = new Date().getTime();
        bindParams.setTimestamp([currentMillis]);
        bindParams.setInt([1]);
        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        await stmt.close();

        let sql = 'SELECT ts, v, jt FROM example_json_tag.stb limit 100';
        wsRows = await wsSql.query(sql);
        while (await wsRows.next()) {
            let row = wsRows.getData();
            console.log('ts: ' + row[0] + ', v: ' + row[1] + ', jt:  ' + row[2]);
        }

    } catch (err) {
        console.error(`Failed to create database example_json_tag or stable stb, ErrCode: ${err.code}, ErrMessage: ${err.message}`);
        throw err
    } finally {
        if (wsSql) {
            await wsSql.close();
        }
    }

}

async function all_type_example() {
    let wsSql = null;
    let stmt = null;
    try {
        let conf = new taos.WSConfig(dsn);
        conf.setUser('root');
        conf.setPwd('taosdata');
        wsSql = await taos.sqlConnect(conf);
        console.log("Connected to " + dsn + " successfully.");
        
        // create database
        await wsSql.exec('CREATE DATABASE IF NOT EXISTS all_type_example');
        console.log("Create database all_type_example successfully.");

        await wsSql.exec('use all_type_example');

        // create table
        await wsSql.exec('create table if not exists stb (ts timestamp, ' + 
            'int_col INT, double_col DOUBLE, bool_col BOOL, binary_col BINARY(100),' +
            'nchar_col NCHAR(100), varbinary_col VARBINARY(100), geometry_col GEOMETRY(100)) ' +
            'tags(int_tag INT, double_tag DOUBLE, bool_tag BOOL, binary_tag BINARY(100),' +
            'nchar_tag NCHAR(100), varbinary_tag VARBINARY(100), geometry_tag GEOMETRY(100));');

        console.log("Create stable all_type_example.stb successfully");

        let geometryData = new Uint8Array([0x01,0x01,0x00,0x00,0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x59,0x40,0x00,0x00,0x00,0x00,0x00,0x00,0x59,0x40,]).buffer;
        
        const encoder = new TextEncoder();    
        let vbData = encoder.encode(`Hello, world!`).buffer;
    
        stmt = await wsSql.stmtInit();
        await stmt.prepare("INSERT INTO ? using stb tags(?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)");
        await stmt.setTableName(`tb1`);
        let tagParams = stmt.newStmtParam();
        tagParams.setInt([1]);
        tagParams.setDouble([1.1]);
        tagParams.setBoolean([true]);
        tagParams.setVarchar(["hello"]);
        tagParams.setNchar(["stmt"]);
        tagParams.setGeometry([geometryData]);
        tagParams.setVarBinary([vbData]);
        await stmt.setTags(tagParams);


        let bindParams = stmt.newStmtParam();
        const currentMillis = new Date().getTime();
        bindParams.setTimestamp([currentMillis]);
        bindParams.setInt([1]);
        bindParams.setDouble([1.1]);
        bindParams.setBoolean([true]);
        bindParams.setVarchar(["hello"]);
        bindParams.setNchar(["stmt"]);
        bindParams.setGeometry([geometryData]);
        bindParams.setVarBinary([vbData]);

        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
    
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
        throw err;
    } finally {
        if (stmt) {
            await stmt.close();
        }
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

