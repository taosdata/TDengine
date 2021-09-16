import {TDRestConnection} from "../TDengineRest";
import assert from "assert"

let conn = new TDRestConnection({host: 'u195:q' +
    ':', user: 'root', pass: 'taosdata', port: 6041});
let cursor = conn.cursor();

const createDB = "create database if not exists node_rest";
const dropDB = "drop database if exists  node_rest";
const createTBL = "CREATE STABLE if not exists node_rest.meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)";
const dropTBL = "drop table if exists node_rest.meters ";
const insert = "INSERT INTO node_rest.d1001 USING node_rest.meters TAGS (\"Beijng.Chaoyang\", 2) VALUES (now, 10.2, 219, 0.32) ";
const select = "select * from node_rest.d1001 ";
const selectStbl = "select * from node_rest.meters";

async function execute(sql) {
  console.log("SQL:" + sql);
  let result = await cursor.query(sql);
  try {
    assert.strictEqual(result.getStatus(), 'succ', new Error("response error"))
    result.toString()
  } catch (e) {
    console.log(e)
  }

}

(async () => {
  await execute(createDB);
  await execute(createTBL);
  await execute(insert);
  await execute(select);
  await execute(selectStbl);
  await execute(dropDB);
})()

// (async () => {
//   result = await cursor.query("drop database if exists  node_rest").catch(e=>console.log(e))
//   result.toString()
// })()
