const taos = require("@tdengine/client");
const conn = taos.connect({ host: "localhost", database: "power" });
const cursor = conn.cursor();

function queryExample() {
  cursor
    .query("SELECT ts, current FROM meters LIMIT 2")
    .execute_a()
    .then((result) => {
      result.pretty();
    });
}

try {
  queryExample();
} finally {
  setTimeout(() => {
    conn.close();
  }, 2000);
}
