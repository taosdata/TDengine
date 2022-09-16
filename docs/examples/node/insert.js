const { options, connect } = require("@tdengine/rest");

function checkError(result) {
  if (result.getErrCode() !== undefined) {
    console.log(result.getErrCode(), result.getErrStr());
    process.exit(1);
  }
}

async function test() {
  options.url = process.env.TDENGINE_CLOUD_URL;
  options.query = { token: process.env.TDENGINE_CLOUD_TOKEN };
  let conn = connect(options);
  let cursor = conn.cursor();
  try {
    let result = await cursor.query("DROP DATABASE IF EXISTS power");
    checkError(result);
    result = await cursor.query("CREATE DATABASE power");
    checkError(result);
    result = await cursor.query("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
    checkError(result);
    result = await cursor.query("INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000) power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)");
    checkError(result);
    console.log("AffectedRows:", result.getAffectRows())
  } catch (err) {
    console.log(err);
  }
}

test();
