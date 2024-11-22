const taos = require('@tdengine/websocket');

var url = process.env.TDENGINE_CLOUD_URL;
async function createConnect() {
  let conn = null;
  try {
    let conf = new taos.WSConfig(url);
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
