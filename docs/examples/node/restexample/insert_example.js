const { options, connect } = require("@tdengine/rest");

async function sqlInsert() {
    options.path = "/rest/sql";
    options.host = "localhost";
    options.port = 6041;
    let conn = connect(options);
    let cursor = conn.cursor();
    try {
        let res = await cursor.query('CREATE DATABASE power');
        res = await cursor.query('CREATE STABLE power.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)');
        res = await cursor.query('INSERT INTO power.d1001 USING power.meters TAGS ("California.SanFrancisco", 2) VALUES (NOW, 10.2, 219, 0.32)');
        console.log("res.getResult()", res.getResult());
    } catch (err) {
        console.log(err);
    }
}
sqlInsert();

