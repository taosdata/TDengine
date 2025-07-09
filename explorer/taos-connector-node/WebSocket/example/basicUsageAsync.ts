import { connect } from "../index";

let dsn =
  "ws://localhost:8085/rest/ws?token=da082dedcca5cbb69a7c719142f194e44ad9472c";
let ws = connect(dsn);

async function connectDatabase(database?: string) {
  let res;
  if (database) {
    res = await ws.connect(database);
  } else {
    res = await ws.connect();
  }
  console.log(res);
}

async function getVersion() {
  let version = await ws.version();
  console.log("version:" + version);
}

async function runSql(sql: string) {
  let queryRes = await ws.query(sql);
  console.log(queryRes);
}

(async () => {
  try {
    // await getVersion();
    await connectDatabase();
    await runSql("select count(*) from test.meters");
    // await runSql(
    //   "create database if not exists power KEEP 3650 DURATION 10 BUFFER 16 WAL_LEVEL 1;"
    // );
    // await runSql("use power");
    // await runSql("show tables");
    // await runSql(
    //   "CREATE STABLE if not exists meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);"
    // );
    // await runSql("describe meters");
    // await runSql(
    //   'INSERT INTO d1001 USING meters TAGS ("California.SanFrancisco", 2) VALUES (NOW, 10.2, 219, 0.32)'
    // );
    // await runSql("select * from meters");
    // await runSql("use test");
    ws.close();
  } catch (e) {
    console.error(e);
    ws.close();
  }
})();
