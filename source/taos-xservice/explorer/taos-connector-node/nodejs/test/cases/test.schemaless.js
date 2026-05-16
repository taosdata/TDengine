const taos = require('../../tdengine');
const { getFeildsFromDll: getFelidFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'sml_db'

// This is a taos connection
let conn;
// This is a Cursor
let c1;

function executeUpdate(sql, printFlag = false) {
  if (printFlag === true) {
      console.log(sql);
  }
  c1.execute(sql, { 'quiet': false });
}

function executeQuery(sql, printFlag = false) {
  if (printFlag === true) {
      console.log(sql);
  }

  c1.execute(sql, { quiet: true })
  var data = c1.fetchall({ 'quiet': false });
  let fields = c1.fields;
  let resArr = [];

  data.forEach(row => {
      row.forEach(data => {
          if (data instanceof Date) {
              resArr.push(data.taosTimestamp());
          } else {
              resArr.push(data);
          }
      })
  })
  return { resData: resArr, resFields: fields };
}

beforeAll(() => {
  conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
  c1 = conn.cursor();
  executeUpdate(`create database if not exists ${db} keep 36500;`);
  executeUpdate(`use ${db};`);
});

// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.
afterAll(() => {
  // executeUpdate(`drop database if exists ${db};`);
  c1.close();
  conn.close();
});


describe("test schemaless", () => {
    test(`name:sml line protocal using string;` +
      `author:${author};` +
      `desc:using line protocal to schemaless insert with a string;` +
      `filename:${fileName};` +
      `result:${result}`, () => {
        let stablename = 'line_protocal_string';

        // executeUpdate(createSql);
        let colData = [1658213992000, 3n, 'passit', false, 4.000000000];
        let tagData = ['3', '4', 't3'];
        let expectResData = getResData(colData, tagData, 5);
        // let lineStr = stablename + ",t1=3i64,t2=4f64,t3=t3 c1=3i64,c3=\"passit\",c2=false,c4=4f64 1658213992000";
        let lineStr = stablename + ",t1=3,t2=4,t3=t3 c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1658213992000000"
  
        c1.schemalessInsert(lineStr, taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_MICRO_SECONDS);
        let result = executeQuery(`select * from ${stablename};`);
        let actualResData = result.resData;
        // console.log(expectResData)

        //assert result data length 
        expect(expectResData.length).toEqual(actualResData.length);

        //assert result data
        expectResData.forEach((item, index) => {
          // console.log(index,item,actualResData[index]);
          expect(item).toEqual(actualResData[index]);
        });
  
      })
  
    test(`name:sml line protocal using Array;` +
      `author:${author};` +
      `desc:using line protocal to schemaless insert with an Array;` +
      `filename:${fileName};` +
      `result:${result}`, () => {
        let stablename = 'line_protocol_arr';
  
        createSql = `create table if not exists ${stablename}(_ts timestamp,c1 bigint,c3 nchar(16),c2 bool,c4 double,c5 double)`
          + `tags(t1 nchar(8),t2 nchar(8),t3 nchar(8),t4 nchar(8));`
  
        //executeUpdate(createSql);

        let expectResField = getFieldArr(getFelidFromDll(createSql));
        let colData2 = [1626006833641, 3n, 'passitagin', true, 5, 5]
        let colData1 = [1626006833639, 3n, 'passit', false, 4, null];
        let tagData2 = ['4i64', '5f64', '\"t4\"', '5f64'];
        let tagData1 = ['3i64', '4f64', '\"t3\"', null];
        let expectResDataTable1 = getResData(colData1, tagData1, 6);
        let expectResDataTable2 = getResData(colData2, tagData2, 6);
        let expectResData = expectResDataTable1.concat(expectResDataTable2);
  
        let lineStr = [stablename + ",t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
        stablename + ",t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000"
        ];

        c1.schemalessInsert(lineStr, taos.SCHEMALESS_PROTOCOL.TSDB_SML_LINE_PROTOCOL, taos.SCHEMALESS_PRECISION.TSDB_SML_TIMESTAMP_NANO_SECONDS);
        
        let result = executeQuery(`select * from ${stablename} order by _ts;`);
        let actualResData = result.resData;
        let actualResFields = result.resFields;
  
        //assert result data length 
        expect(expectResData.length).toEqual(actualResData.length);
        // console.log(lineStr);
        //assert result data
        expectResData.forEach((item, index) => {
          // console.log(`sml line protocal using Array:acutualResData[${index}]:${actualResData[index]} expectData:${item}`);
          expect(item).toEqual(actualResData[index]);
        });
  
        //assert result meta data
        expectResField.forEach((item, index) => {
          // console.log(`sml line protocal using Array:acutualField[${index}]:${JSON.stringify(actualResFields[index]) } expectField:${JSON.stringify(item)}`);
          expect(actualResFields[index]).toEqual(item)
        })
      })
  
    test(`name:sml json protocal using string;` +
      `author:${author};` +
      `desc:using json protocal to schemaless insert with a json string;` +
      `filename:${fileName};` +
      `result:${result}`, () => {
        let stablename = 'json_protocol_str';
  
        createSql = `create table if not exists ${stablename}(ts timestamp,_value double)`
          + `tags(t1 bool,t2 bool,t3 double,t4 nchar(35));`
  
        executeUpdate(createSql);
        let expectResField = getFieldArr(getFelidFromDll(createSql));
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
        let actualResFields = result.resFields;
  
        //assert result data length 
        // expect(expectResData.length).toEqual(actualResData.length);
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
  
        createSql = `create table if not exists ${stablename}(ts timestamp,_value double)`
          + `tags(t1 bool,t2 bool,t3 double,t4 nchar(35));`
  
        executeUpdate(createSql);
        let expectResField = getFieldArr(getFelidFromDll(createSql));
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
        let actualResFields = result.resFields;
  
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