const taos = require('../../tdengine');
const { getFeildsFromDll: getFelidFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'null_db';

// This is a taos connection
let conn;
// This is a Cursor
let c1;

function executeUpdate(sql,printFlag = false) {
  if(printFlag === true){
    console.log(sql);
  }
  c1.execute(sql,{'quiet':false});
}

function executeQuery(sql,printFlag = false) {
  if(printFlag === true){
    console.log(sql);
  }
  
  c1.execute(sql, { quiet: true })
  var data = c1.fetchall({'quiet':false});
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
  return { resData: resArr, resFeilds: fields };
}

beforeAll(() => {
  conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
  c1 = conn.cursor();
  executeUpdate(`create database if not exists ${db} keep 3650;`);
  executeUpdate(`use ${db};`);
});

// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.
afterAll(() => {
  executeUpdate(`drop database if exists ${db};`);
  c1.close();
  conn.close();
});

describe("test TD-20163 query null", () => {

    test(`name:query number mixed with null and value;` +
      `author:${author};` +
      `desc:query number mixed with null and value;` +
      `filename:${fileName};` +
      `result:${result}`, () => {
        let createSql = "create table test_follow_null (ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,ui1 tinyint unsigned,ui2 smallint unsigned,ui4 int unsigned,ui8 bigint unsigned,f4 float,d8 double,ts8 timestamp,bnr binary(200),nchr nchar(300));";
        executeUpdate(createSql);
        let expectResField = getFieldArr(getFelidFromDll(createSql));
        let expectResData = [1641827743305, null,null,null,null,null,null,null,null,null,null,null,null,null
          , 1641827743306, -1,-2,-4,-8n,1,2,3,4n,3.14,3.141292653,1641827744306,'bnry_1','nchar_1'];
        let insertSql = buildInsertSql('test_follow_null', '', expectResData, [], 14);
  
        executeUpdate(insertSql);
        let result = executeQuery("select * from test_follow_null;");
        let actualResData = result.resData;
        let actualResFields = result.resFeilds;
  
        //assert result data length 
        expect(expectResData.length).toEqual(actualResData.length);
        //assert result data
        expectResData.forEach((item, index) => {
          // console.log(item,actualResData[index])
          expect(item).toEqual(actualResData[index]);
        });
  
        //assert result meta data
        expectResField.forEach((item, index) => {
          expect(item).toEqual(actualResFields[index])
        })
      })
    }
)