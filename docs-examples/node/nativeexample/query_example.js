const taos = require("td2.0-connector");

const conn = taos.connect({ host: "localhost", database: "power" });
const cursor = conn.cursor();
const query = cursor.query("SELECT ts, current FROM meters LIMIT 2");
query.execute().then(function (result) {
  result.pretty();
});

// output:
// Successfully connected to TDengine
// Query OK, 2 row(s) in set (0.00317767s)

//            ts             |         current          |
// =======================================================
// 2018-10-03 14:38:05.000   | 10.3                     |
// 2018-10-03 14:38:15.000   | 12.6                     |
