// const { testMatch } = require('../../jest.config');
const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);

// This is a taos connection
let conn;
// This is a Cursor
let c1;

function executeUpdate(sql) {
  console.log(sql);
  c1.execute(sql);
}

function executeQuery(sql) {
  c1.execute(sql, { quiet: true })
  var data = c1.fetchall();
  let fields = c1.fields;
  let resArr = [];

  data.forEach(row => {
    row.forEach(data => {
      if (data instanceof Date) {
        // console.log("date obejct:"+data.valueOf());
        resArr.push(data.taosTimestamp());
      } else {
        // console.log("not date:"+data);
        resArr.push(data);
      }
      // console.log(data instanceof Date)
    })
  })
  return { resData: resArr, resFeilds: fields };
}

beforeAll(() => {
  conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
  c1 = conn.cursor();
  executeUpdate("create database if not exists node_test_db keep 3650;");
  executeUpdate("use node_test_db;");
});

// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.
afterAll(() => {
  executeUpdate("drop database if exists node_test_db;");
  c1.close();
  conn.close();
});

describe("test unsigned type", () => {

  test(`name:test unsinged tinnyint ntable;` +
    `author:${author};` +
    `desc:create,insert,query with unsigned tinnyint;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists utinnytest(ts timestamp,ut tinyint unsigned,i4 int,rownum nchar(20));";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = [1641827743305, 254, 124, 'row1'
        , 1641827743306, 0, -123, 'row2'
        , 1641827743307, 54, 0, 'row3'];
      let insertSql = buildInsertSql('utinnytest', '', expectResData, [], 4);

      executeUpdate(insertSql);
      let result = executeQuery("select * from utinnytest;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:test unsinged smallint ntable;` +
    `author:${author};` +
    `desc:create,insert,query with unsigned smallint;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists usmalltest(ts timestamp,ut smallint unsigned,i4 int,rownum nchar(20));";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = [1641827743305, 65534, 124, 'row1', 1641827743306, 0, -123, 'row2', 1641827743307, 79, 0, 'row3'];
      let insertSql = buildInsertSql('usmalltest', '', expectResData, [], 4);

      executeUpdate(insertSql);
      let result = executeQuery("select * from usmalltest;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:test unsinged int ntable;` +
    `author:${author};` +
    `desc:create,insert,query with unsigned int;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists uinttest(ts timestamp,ui int unsigned,i4 int,rownum nchar(20));";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = [1641827743305, 4294967294, 2147483647, 'row1', 1641827743306, 0, -2147483647, 'row2', 1641827743307, 105, 0, 'row3'];
      let insertSql = buildInsertSql('uinttest', '', expectResData, [], 4);

      executeUpdate(insertSql);
      let result = executeQuery("select * from uinttest;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:test unsinged bigint ntable;` +
    `author:${author};` +
    `desc:create,insert,query with unsigned bigint;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists ubigtest(ts timestamp,ui bigint unsigned,i8 bigint,rownum nchar(20));";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = [1641827743305, 18446744073709551614n, 9223372036854775807n, 'row1',
        1641827743306, 0n, -9223372036854775807n, 'row2',
        1641827743307, 130n, 0n, 'row3'];
      let insertSql = buildInsertSql('ubigtest', '', expectResData, [], 4);

      executeUpdate(insertSql);
      let result = executeQuery("select * from ubigtest;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:test unsinged type ntable;` +
    `author:${author};` +
    `desc:create,insert,query with mutiple unsinged type;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists unsigntest(ts timestamp,ut tinyint unsigned,us smallint unsigned,ui int unsigned,ub bigint unsigned,bi bigint);";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = [1641827743305, 254, 65534, 4294967294, 18446744073709551614n, 9223372036854775807n,
        1641827743306, 0, 0, 0, 0n, -9223372036854775807n];
      let insertSql = buildInsertSql('unsigntest', '', expectResData, [], 6);

      executeUpdate(insertSql);
      let result = executeQuery("select * from unsigntest;");
      // console.log(`result.data:${result.resData}`);
      // console.log(`result.feilds:${result.resFeilds}`);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:test unsinged type stable max value;` +
    `author:${author};` +
    `desc:this is a description;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists max_unsigned_tag_test" +
        "(ts timestamp" +
        ",ut tinyint unsigned" +
        ",us smallint unsigned" +
        ",ui int unsigned" +
        ",ub bigint unsigned" +
        ",bi bigint)" +
        "tags(" +
        "ut1 tinyint unsigned" +
        ",us2 smallint unsigned" +
        ",ui4 int unsigned" +
        ",ubi8 bigint unsigned" +
        ",desc_nchr nchar(200)" +
        ");";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData = [1641827743305, 254, 65534, 4294967294, 18446744073709551614n, 9223372036854775807n,
        1641827743306, 0, 0, 0, 0n, -9223372036854775807n,
        1641827743307, 201, 44, 2, 8n, 1531n];
      let tagData = [254, 65534, 4294967294, 18446744073709551614n, 'max value of unsinged type tag']
      let insertSql = buildInsertSql('max_unsigned_tag_test_sub1', 'max_unsigned_tag_test', colData, tagData, 6);
      let expectResData = getResData(colData, tagData, 6);

      executeUpdate(insertSql);
      let result = executeQuery("select * from max_unsigned_tag_test;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:test unsinged type stable minimal value;` +
    `author:${author};` +
    `desc:this is a description;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists min_unsigned_tag_test" +
        "(ts timestamp" +
        ",ut tinyint unsigned" +
        ",us smallint unsigned" +
        ",ui int unsigned" +
        ",ub bigint unsigned" +
        ",bi bigint)" +
        "tags(" +
        "ut1 tinyint unsigned" +
        ",us2 smallint unsigned" +
        ",ui4 int unsigned" +
        ",ubi8 bigint unsigned" +
        ",desc_nchr nchar(200)" +
        ");";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData = [1641827743305, 254, 65534, 4294967294, 18446744073709551614n, 9223372036854775807n,
        1641827743306, 0, 0, 0, 0n, -9223372036854775807n,
        1641827743307, 201, 44, 2, 8n, 1531n];
      let tagData = [0, 0, 0, 0n, 'minimal value of unsinged type tag']
      let insertSql = buildInsertSql('min_unsigned_tag_test_sub1', 'min_unsigned_tag_test', colData, tagData, 6);
      let expectResData = getResData(colData, tagData, 6);

      executeUpdate(insertSql);
      let result = executeQuery("select * from min_unsigned_tag_test;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      console.log("expectResData.length:" + expectResData.length + " actualResData.length:" + actualResData.length);
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

  test(`name:test unsinged type stable mixed value;` +
    `author:${author};` +
    `desc:this is a description;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let createSql = "create table if not exists mix_unsigned_tag_test" +
        "(ts timestamp" +
        ",ut tinyint unsigned" +
        ",us smallint unsigned" +
        ",ui int unsigned" +
        ",ub bigint unsigned" +
        ",bi bigint)" +
        "tags(" +
        "ut1 tinyint unsigned" +
        ",us2 smallint unsigned" +
        ",ui4 int unsigned" +
        ",ubi8 bigint unsigned" +
        ",desc_nchr nchar(200)" +
        ");";
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData = [1641827743305, 254, 65534, 4294967294, 18446744073709551614n, 9223372036854775807n,
        1641827743306, 0, 0, 0, 0n, -9223372036854775807n,
        1641827743307, 201, 44, 2, 8n, 1531n];
      let tagData = [1, 20, 300, 4000n, 'mixed value of unsinged type tag']
      let insertSql = buildInsertSql('mix_unsigned_tag_test_sub1', 'mix_unsigned_tag_test', colData, tagData, 6);
      let expectResData = getResData(colData, tagData, 6);

      executeUpdate(insertSql);
      let result = executeQuery("select * from mix_unsigned_tag_test;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    });

})

describe("test cn character", () => {
  test(`name:test cn ntable;` +
    `author:${author};` +
    `desc:create,insert,query with cn characters;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      createSql = "create table if not exists nchartest(ts timestamp,value int,text binary(200),detail nchar(200));"
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = [1641827743305, 1, 'taosdata', 'tdengine'
        , 1641827743306, 2, 'tasdata', '涛思数据'
        , 1641827743307, 3, '涛思数据', 'tdengine'
        , 1641827743308, 4, '涛思数据taosdata', 'tdengine'
        , 1641827743309, 5, '涛思数据taosdata', 'tdengine涛思数据'];
      let insertSql = buildInsertSql('nchartest', '', expectResData, [], 4);

      executeUpdate(insertSql);
      let result = executeQuery("select * from nchartest;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });
      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    })

  test(`name:test cn stable;` +
    `author:${author};` +
    `desc:create,insert,query with cn characters;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      createSql = "create table if not exists nchartest_s(ts timestamp,value int,text binary(200),detail nchar(200))tags(tag_bi binary(50),tag_nchr nchar(50));"
      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData = [1641827743305, 1, 'taosdata', 'tdengine'
        , 1641827743306, 2, 'tasdata', '涛思数据'
        , 1641827743307, 3, '涛思数据', 'tdengine'
        , 1641827743308, 4, '涛思数据taosdata', 'tdengine'
        , 1641827743309, 5, '涛思数据taosdata', 'tdengine涛思数据'];
      let tagData = ['tags涛思', '数据tags'];
      let insertSql = buildInsertSql('sb_1', 'nchartest_s', colData, tagData, 4);
      let expectResData = getResData(colData, tagData, 4);

      executeUpdate(insertSql);
      let result = executeQuery("select * from nchartest_s;");
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    })
})

describe("test schemaless", () => {
  test(`name:sml line protocal using string;` +
    `author:${author};` +
    `desc:using line protocal to schemaless insert with a string;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let stablename = 'line_protocal_string';
      createSql = `create table if not exists ${stablename}(ts timestamp,c1 bigint,c3 nchar(6),c2 bool,c4 double)`
        + `tags(t1 nchar(4),t2 nchar(4),t3 nchar(4));`

      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData = [1626006833639, 3n, 'passit', false, 4.000000000];
      let tagData = ['3i64', '4f64', '\"t3\"'];
      let expectResData = getResData(colData, tagData, 5);
      let lineStr = stablename + ",t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639";


      c1.schemalessInsert(lineStr, taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_MILLI_SECONDS);
      let result = executeQuery(`select * from ${stablename};`);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    })

  test(`name:sml line protocal using Array;` +
    `author:${author};` +
    `desc:using line protocal to schemaless insert with an Array;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let stablename = 'line_protocol_arr';

      createSql = `create table if not exists ${stablename}(ts timestamp,c1 bigint,c3 nchar(10),c2 bool,c4 double,c5 double)`
        + `tags(t1 nchar(4),t2 nchar(4),t3 nchar(4),t4 nchar(4));`

      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData1 = [1626006833641, 3n, 'passitagin', true, 5, 5]
      let colData2 = [1626006833639, 3n, 'passit', false, 4, null];
      let tagData1 = ['4i64', '5f64', '\"t4\"', '5f64'];
      let tagData2 = ['3i64', '4f64', '\"t3\"', null];
      let expectResDataTable1 = getResData(colData1, tagData1, 6);
      let expectResDataTable2 = getResData(colData2, tagData2, 6);
      let expectResData = expectResDataTable1.concat(expectResDataTable2);

      let lineStr = [stablename + ",t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
      stablename + ",t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000"
      ];

      c1.schemalessInsert(lineStr, taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_NANO_SECONDS);
      let result = executeQuery(`select * from ${stablename};`);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    })

  test(`name:sml json protocal using string;` +
    `author:${author};` +
    `desc:using json protocal to schemaless insert with a json string;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let stablename = 'json_protocol_str';

      createSql = `create table if not exists ${stablename}(ts timestamp,value double)`
        + `tags(t1 bool,t2 bool,t3 double,t4 nchar(35));`

      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData1 = [1626006833000, 10]
      let tagData1 = [true, false, 10, '123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>'];
      let expectResData = getResData(colData1, tagData1, 2);

      let jsonStr = "{"
        + "\"metric\": \"" + stablename + "\","
        + "\"timestamp\": 1626006833000,"
        + "\"value\": 10,"
        + "\"tags\": {"
        + " \"t1\": true,"
        + "\"t2\": false,"
        + "\"t3\": 10,"
        + "\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
        + "}"
        + "}";

      c1.schemalessInsert(jsonStr, taos.SCHEMALESS_PROTOCOL.TSDB_SML_JSON_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_MILLI_SECONDS);

      let result = executeQuery(`select * from ${stablename};`);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    })

  test(`name:sml json protocal using Array;` +
    `author:${author};` +
    `desc:using json protocal to schemaless insert with a json array;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let stablename = 'json_protocol_arr';

      createSql = `create table if not exists ${stablename}(ts timestamp,value double)`
        + `tags(t1 bool,t2 bool,t3 double,t4 nchar(35));`

      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData1 = [1626006833000, 10]
      let tagData1 = [true, false, 10, '123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>'];
      let expectResData = getResData(colData1, tagData1, 2);

      let jsonArr = ["{"
        + "\"metric\": \"" + stablename + "\","
        + "\"timestamp\": 1626006833,"
        + "\"value\": 10,"
        + "\"tags\": {"
        + " \"t1\": true,"
        + "\"t2\": false,"
        + "\"t3\": 10,"
        + "\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
        + "}"
        + "}"
      ];

      c1.schemalessInsert(jsonArr, taos.SCHEMALESS_PROTOCOL.TSDB_SML_JSON_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_SECONDS);

      let result = executeQuery(`select * from ${stablename};`);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    })
})

describe("test support microsecond", () => {
  test(`name:ms support ntable;` +
    `author:${author};` +
    `desc:test normal table supports microseconds;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let db = 'nodejs_support_ms_ntable';
      let table = 'us_test_ntable';
      let expectResData = [1625801548423914, 1, 1625801548423914,
        1625801548423915, 2, 1625801548423914,
        1625801548423916, 3, 1625801548423914,
        1625801548423917, 4, 1625801548423914];

      let createDB = `create database if not exists ${db} keep 3650 precision \'us\';`;
      let createSql = `create table if not exists ${db}.${table} (ts timestamp, seq int,record_date timestamp);`;
      let dropDB = `drop database if exists ${db};`;
      let insertSql = buildInsertSql(db + '.' + table, '',expectResData, [], 3);
      let querySql = `select * from ${db}.${table};`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      
      executeUpdate(dropDB);
      executeUpdate(createDB);
      executeUpdate(createSql);
      executeUpdate(insertSql);
      let result = executeQuery(querySql);

      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
      executeUpdate(dropDB);

    });

  test(`name:ms support stable;` +
    `author:${author};` +
    `desc:test stable supports microseconds;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let db = 'nodejs_support_ms_stable';
      let stable = 'us_test_stable';
      let table = "sub_1";
      let colData = [1625801548423914, 1, 1625801548423914,
        1625801548423915, 2, 1625801548423914,
        1625801548423916, 3, 1625801548423914,
        1625801548423917, 4, 1625801548423914];
      let tagData = [1,1625801548423914];
      let createDB = `create database if not exists ${db} keep 3650 precision \'us\';`;
      let createSql = `create table if not exists ${db}.${stable} (ts timestamp,seq int,`+
      `record_date timestamp)tags(id int,htime timestamp);`;
      let dropDB = `drop database if exists ${db};`;
      let insertSql = buildInsertSql(db + '.' + table, db + '.' + stable, colData,tagData, 3);
      let querySql = `select * from ${db}.${stable};`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(colData, tagData, 3);
      
      executeUpdate(dropDB);
      executeUpdate(createDB);
      executeUpdate(createSql);
      executeUpdate(insertSql);
      let result = executeQuery(querySql);

      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
      executeUpdate(dropDB);
    })
})

describe("test support nanosecond", () => {
  test(`name:ns support ntable;` +
    `author:${author};` +
    `desc:test normal table supports nanoseconds;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let db = 'nodejs_support_ns_ntable';
      let table = 'ns_test_ntable';
      let expectResData = [1625801548423914100, 1, 1625801548423914
        ,1625801548423914200, 2, 1625801548423914
      ];

      let createDB = `create database if not exists ${db} keep 3650 precision \'ns\';`;
      let createSql = `create table if not exists ${db}.${table} (ts timestamp, seq int,record_date timestamp);`;
      let dropDB = `drop database if exists ${db};`;
      let insertSql = buildInsertSql(db + '.' + table, '',expectResData, [], 3);
      let querySql = `select * from ${db}.${table};`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      
      executeUpdate(dropDB);
      executeUpdate(createDB);
      executeUpdate(createSql);
      executeUpdate(insertSql);
      console.log(querySql);
      let result = executeQuery(querySql);

      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        console.log((index));
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
      executeUpdate(dropDB);
    });

  test(`name:ns support stable;` +
    `author:${author};` +
    `desc:test stable supports nanoseconds;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let db = 'nodejs_support_ns_stable';
      let stable = 'ns_test_stable';
      let table = "sub_1";
      let colData = [1625801548423914100, 1, 1625801548423914,
        1625801548423914200, 2, 1625801548423914];
      let tagData = [1,1625801548423914100];
      let createDB = `create database if not exists ${db} keep 3650 precision \'ns\';`;
      let createSql = `create table if not exists ${db}.${stable} (ts timestamp,seq int,`+
      `record_date timestamp)tags(id int,htime timestamp);`;
      let dropDB = `drop database if exists ${db};`;
      let insertSql = buildInsertSql(db + '.' + table, db + '.' + stable, colData,tagData, 3);
      let querySql = `select * from ${db}.${stable};`;
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let expectResData = getResData(colData, tagData, 3);
      
      executeUpdate(dropDB);
      executeUpdate(createDB);
      executeUpdate(createSql);
      executeUpdate(insertSql);
      let result = executeQuery(querySql);

      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
      executeUpdate(dropDB);
    })
})

describe("test json tag", () => {
  test(`name:json tag;` +
    `author:${author};` +
    `desc:create,insert,query with json tag;` +
    `filename:${fileName};` +
    `result:${result}`, () => {
      let tableName = 'jsons1';
      let createSql = `create table if not exists ${tableName}(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json);`;

      executeUpdate(createSql);
      let expectResField = getFieldArr(getFeildsFromDll(createSql));
      let colData = [1591060618000, 1, false, 'json1', '涛思数据'
        , 1591060628000, 23, true, '涛思数据', 'json'
        , 1591060638000, 54, false, 'tdengine', 'taosdata'];
      let tagData = ['{\"tag1\":\"fff\",\"tag2\":5,\"tag3\":true}']
      let insertSql = buildInsertSql('json_sub_1', tableName, colData, tagData, 5);
      let expectResData = getResData(colData, tagData, 5);

      executeUpdate(insertSql);
      let result = executeQuery(`select * from ${tableName};`);
      let actualResData = result.resData;
      let actualResFields = result.resFeilds;

      //assert result data length 
      expect(expectResData.length).toEqual(actualResData.length);
      //assert result data
      expectResData.forEach((item, index) => {
        expect(item).toEqual(actualResData[index]);
      });

      //assert result meta data
      expectResField.forEach((item, index) => {
        expect(item).toEqual(actualResFields[index])
      })
    })
})