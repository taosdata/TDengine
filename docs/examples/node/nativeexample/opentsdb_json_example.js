const taos = require("@tdengine/client");

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
    {
      metric: "meters.current",
      timestamp: 1648432611249,
      value: 10.3,
      tags: { location: "California.SanFrancisco", groupid: 2 },
    },
    {
      metric: "meters.voltage",
      timestamp: 1648432611249,
      value: 219,
      tags: { location: "California.LosAngeles", groupid: 1 },
    },
    {
      metric: "meters.current",
      timestamp: 1648432611250,
      value: 12.6,
      tags: { location: "California.SanFrancisco", groupid: 2 },
    },
    {
      metric: "meters.voltage",
      timestamp: 1648432611250,
      value: 221,
      tags: { location: "California.LosAngeles", groupid: 1 },
    },
  ];

  cursor.schemalessInsert(
    [JSON.stringify(lines)],
    taos.SCHEMALESS_PROTOCOL.TSDB_SML_JSON_PROTOCOL,
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
