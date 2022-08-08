const { options, connect } = require("@tdengine/rest");

async function test() {
  options.path = "/rest/sql";
  options.host = "localhost";
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let res = await cursor.query("SELECT server_version()");
    res.toString();
  } catch (err) {
    console.log(err);
  }
}
test();

// output:
// server_version() |
// ===================
// 3.0.0.0          |
