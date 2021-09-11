import {TDengineRestConnection} from "../src/restConnect";

let conn = new TDengineRestConnection({host: 'u195'})
let cursor = conn.cursor();

(async () => {
  data = await cursor.query("select * from test.meters limit 10").catch(e => console.log(e));
  data.toString()
})()



