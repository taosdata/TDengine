import {TDengineRestConnection} from "../src/restConnect";

let conn = new TDengineRestConnection({host: '127.0.0.1', user: 'root', pass: 'taosdata', port: 6041})
let cursor = conn.cursor();
console.log(conn)
let data = {};
(async () => {
  data = await cursor.query("show databases");
  data.toString()
})()



