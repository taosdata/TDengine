const taos = require('@tdengine/websocket');

var url = process.env.TDENGINE_CLOUD_URL;
var token = process.env.TDENGINE_CLOUD_TOKEN;
async function insertData() {
  let conn = null;
  try {
    let conf = new taos.WSConfig(url);
    conf.setToken(token);
    conf.setDb('test');
    conn = await taos.sqlConnect(conf);
    await conn.exec(
      "insert into cloud using meters tags (1, 'new york') values (now, 1.1, 1, 1.1)"
    );
  } catch (err) {
    throw err;
  } finally {
    if (conn) {
      await conn.close();
    }
  }
}

insertData();
