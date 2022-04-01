const taos = require("td2.0-connector");

const conn = taos.connect({ host: "localhost", database: "power" });
const cursor = conn.cursor();
const query = cursor.query("SELECT ts, current FROM meters LIMIT 2");
query.execute().then(function (result) {
  result.pretty();
});
// 目前执行失败，原因不明： TD-1448
