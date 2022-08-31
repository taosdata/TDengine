const { options, connect } = require("@tdengine/rest");

async function test() {
  options.url = process.env.TDENGINE_CLOUD_URL;
  options.query = { token: process.env.TDENGINE_CLOUD_TOKEN };
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let res = await cursor.query("show databases");
    res.toString();
  } catch (err) {
    console.log(err);
  }
}

test();
