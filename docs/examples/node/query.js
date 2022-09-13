const { options, connect } = require("@tdengine/client");

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
    let result = await cursor.query("SELECT ts, current FROM power.meters LIMIT 2");
    console.log(result.getMeta());
    // [
    //   { columnName: 'ts', code: 9, size: 8, typeName: 'timestamp' },
    //   { columnName: 'current', code: 6, size: 4, typeName: 'float' }
    // ]
    console.log(result.getData());
    // [ [ '2018-10-03T14:38:05Z', 10.3 ], [ '2018-10-03T14:38:15Z', 12.6 ] ]
  } catch (err) {
    console.log(err);
  }
}

test();
