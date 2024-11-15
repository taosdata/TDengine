const taos = require('@tdengine/websocket');

var url = process.env.TDENGINE_CLOUD_URL;
var token = process.env.TDENGINE_CLOUD_TOKEN;
async function createConnect() {
  let conn = null;
  try {
    let conf = new taos.WSConfig(url);
    conf.setToken(token);
    conn = await taos.sqlConnect(conf);
    let res = await conn.query('show databases');
    while (await res.next()) {
      let row = res.getData();
      console.log(row[0]);
    }
  } catch (err) {
    throw err;
  } finally {
    if (conn) {
      await conn.close();
    }
  }
}
