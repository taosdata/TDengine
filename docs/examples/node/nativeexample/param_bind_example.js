const taos = require("td2.0-connector");

const conn = taos.connect({
  host: "localhost",
});

const cursor = conn.cursor();

function prepareSTable() {
  cursor.execute("CREATE DATABASE power");
  cursor.execute("USE power");
  cursor.execute(
    "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
  );
}

function insertData() {
  // init
  cursor.stmtInit();
  // prepare
  cursor.stmtPrepare(
    "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)"
  );

  // bind table name and tags
  let tagBind = new taos.TaosBind(2);
  tagBind.bindBinary("California.SanFrancisco");
  tagBind.bindInt(2);
  cursor.stmtSetTbnameTags("d1001", tagBind.getBind());

  // bind values
  let rows = [
    [1648432611249, 10.3, 219, 0.31],
    [1648432611749, 12.6, 218, 0.33],
  ];
  for (let row of rows) {
    let valueBind = new taos.TaosBind(4);
    valueBind.bindTimestamp(row[0]);
    valueBind.bindFloat(row[1]);
    valueBind.bindInt(row[2]);
    valueBind.bindFloat(row[3]);
    cursor.stmtBindParam(valueBind.getBind());
    cursor.stmtAddBatch();
  }

  // execute
  cursor.stmtExecute();
  cursor.stmtClose();
}

try {
  prepareSTable();
  insertData();
} finally {
  cursor.close();
  conn.close();
}
