import {TDRestConnection} from "../TDengineRest";

let conn = new TDRestConnection({host: 'u195'});
let cursor = conn.cursor();

(async () => {
  result = await cursor.query("drop database if exists  node_rest");
  result.toString()
})()

const createDB = "create database if not exists  node_rest";
const dropDB = "drop database if exists  node_rest";
const createTBL = "CREATE STABLE if not exists node_rest.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)";
const dropTBL = "drop table if exists node_rest.meters ";
const insert = "INSERT INTO node_rest.d1001 USING node_rest.meters TAGS (\"Beijng.Chaoyang\", 2) VALUES (now, 10.2, 219, 0.32) ";
const select = "select * from node_rest.d1001 ";
const selectStbl = "select * from node_rest.meters";

async function execute(sql) {
  console.log(sql);
  result = await cursor.query(sql);
  result.toString()
};

(async () => {
  await execute(createDB);
  await execute(createTBL);
  await execute(insert);
  await execute(select);
  await execute(selectStbl);
  await execute(dropDB);
})()
