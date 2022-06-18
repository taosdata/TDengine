const { options, connect } = require("td2.0-rest-connector");

async function test() {
  options.url = process.env.TDENGINE_CLOUD_URL;
  options.query = { token: process.env.TDENGINE_CLOUD_TOKEN };
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let res = await cursor.query("select server_version()");
    res.toString();
  } catch (err) {
    console.log(err);
  }
}

test();
