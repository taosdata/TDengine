const { options, connect } = require("td2.0-rest-connector");

async function test() {
  options.path = "/rest/sqlt";
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
// 2.4.0.12         |
