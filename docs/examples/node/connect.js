const taos = require('@tdengine/websocket');

var url = process.env.TDENGINE_CLOUD_URL;
var token = process.env.TDENGINE_CLOUD_TOKEN;
async function createConnect() {
  let conn = null;
  try {
    let conf = new taos.WSConfig(url);
    conf.setToken(token);
    conn = await taos.sqlConnect(conf);
  } catch (err) {
    throw err;
  } finally {
    if (conn) {
      await conn.close();
    }
  }
}

createConnect();
