const taos = require("td2.0-connector");

async function queryExample() {
  const conn = taos.connect({ host: "localhost", database: "power" });
  const cursor = conn.cursor();
  const query = cursor.query("SELECT ts, current FROM meters LIMIT 2");
  const result = await query.execute_a();
  console.log(result);
}

queryExample();

