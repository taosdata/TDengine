const taos = require("td2.0-connector");

const conn = taos.connect({
  host: "localhost",
});

const cursor = conn.cursor();

function createDatabase() {
  cursor.execute("CREATE DATABASE test");
  cursor.execute("USE test");
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

try {
  createDatabase();
  insertData();
} finally {
  // cursor.close(); // 此处抛异常
  conn.close();
}
