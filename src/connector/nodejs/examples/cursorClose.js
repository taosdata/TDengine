const taos = require('../tdengine');

const conn = taos.connect({
    host: "localhost",
});

const cursor = conn.cursor();

function createDatabase() {
    cursor.execute("CREATE DATABASE if not exists test_cursor_close");
    cursor.execute("USE test_cursor_close");
}

function insertData() {
    const lines = [
        "meters,location=Beijing.Haidian,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
        "meters,location=Beijing.Haidian,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
        "meters,location=Beijing.Haidian,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
        "meters,location=Beijing.Haidian,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250",
    ];
    cursor.schemalessInsert(
        lines,
        taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL,
        taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_MILLI_SECONDS
    );
}

function query() {
    let promise = cursor.query("select * from test_cursor_close.meters");
    promise.execute().then(result => result.pretty()).catch(err => console.log(e));
}

function destructData() {
    cursor.execute("drop database if exists test_cursor_close");
}

function main() {
    try {
        createDatabase();
        insertData();
        query();
        destructData();
    } finally {
        cursor.close();
        conn.close();
    }
}

main();