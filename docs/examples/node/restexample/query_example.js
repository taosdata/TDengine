const { options, connect } = require("@tdengine/rest");

async function query() {
    options.path = "/rest/sql";
    options.host = "localhost";
    options.port = 6041;
    let conn = connect(options);
    let cursor = conn.cursor();
    try {
        let res = await cursor.query('select * from power.meters');
        console.log("res.getResult()", res.getResult());
    } catch (err) {
        console.log(err);
    }
}
query();
