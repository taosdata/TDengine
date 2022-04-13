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
    "meters.current 1648432611249 10.3 location=Beijing.Chaoyang groupid=2",
    "meters.current 1648432611250 12.6 location=Beijing.Chaoyang groupid=2",
    "meters.current 1648432611249 10.8 location=Beijing.Haidian groupid=3",
    "meters.current 1648432611250 11.3 location=Beijing.Haidian groupid=3",
    "meters.voltage 1648432611249 219 location=Beijing.Chaoyang groupid=2",
    "meters.voltage 1648432611250 218 location=Beijing.Chaoyang groupid=2",
    "meters.voltage 1648432611249 221 location=Beijing.Haidian groupid=3",
    "meters.voltage 1648432611250 217 location=Beijing.Haidian groupid=3",
  ];
  cursor.schemalessInsert(
    lines,
    taos.SCHEMALESS_PROTOCOL.TSDB_SML_TELNET_PROTOCOL,
    taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_NOT_CONFIGURED
  );
}

try {
  createDatabase();
  insertData();
} finally {
  cursor.close();
  conn.close();
}
