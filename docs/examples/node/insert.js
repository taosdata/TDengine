const taos = require('@tdengine/websocket');

var url = process.env.TDENGINE_CLOUD_URL;
var token = process.env.TDENGINE_CLOUD_TOKEN;
async function createConnect() {
  let conn = null;
  try {
    let conf = new taos.WSConfig(url);
    conf.setToken(token);
    conf.setDb('test');
    conn = await taos.sqlConnect(conf);
    await conn.exec('insert into t1 using meters tags (1) values(now, 1)');
  } catch (err) {
    throw err;
  } finally {
    if (conn) {
      await conn.close();
    }
  }
}
