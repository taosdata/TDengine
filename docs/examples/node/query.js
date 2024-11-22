const taos = require('@tdengine/websocket');

var url = process.env.TDENGINE_CLOUD_URL;
async function queryData() {
  let conn = null;
  try {
    let conf = new taos.WSConfig(url);
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

queryData();
