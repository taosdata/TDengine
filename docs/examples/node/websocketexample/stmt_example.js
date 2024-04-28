const taos = require("@tdengine/websocket");

let db = 'power';
let stable = 'meters';
let tags = ['California.SanFrancisco', 3];
let values = [
    [1706786044994, 1706786044995, 1706786044996],
    [10.2, 10.3, 10.4],
    [292, 293, 294],
    [0.32, 0.33, 0.34],
];

async function prepare() {
    let dsn = 'ws://localhost:6041'
    let conf = new taos.WSConfig(dsn);
    conf.setUser('root')
    conf.setPwd('taosdata')
    conf.setDb(db)
    let wsSql = await taos.sqlConnect(conf);
    await wsSql.exec(`CREATE DATABASE IF NOT EXISTS ${db} KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;`);
    await wsSql.exec(`CREATE STABLE IF NOT EXISTS ${db}.${stable} (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);`);
    return wsSql
}

(async () => {
    let stmt = null;
    let connector = null;
    try {
        connector = await prepare();
        stmt = await connector.stmtInit();
        await stmt.prepare(`INSERT INTO ? USING ${db}.${stable} (location, groupId) TAGS (?, ?) VALUES (?, ?, ?, ?)`);
        await stmt.setTableName('d1001');
        let tagParams = stmt.newStmtParam();
        tagParams.setVarchar([tags[0]]);
        tagParams.setInt([tags[1]]);
        await stmt.setTags(tagParams);

        let bindParams = stmt.newStmtParam();
        bindParams.setTimestamp(values[0]);
        bindParams.setFloat(values[1]);
        bindParams.setInt(values[2]);
        bindParams.setFloat(values[3]);
        await stmt.bind(bindParams);
        await stmt.batch();
        await stmt.exec();
        console.log(stmt.getLastAffected());
    }
    catch (err) {
        console.error(err.code, err.message);
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
})();
