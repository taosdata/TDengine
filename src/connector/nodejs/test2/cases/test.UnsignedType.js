const { testMatch } = require('../../jest.config');
const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);

var conn;
var c1;
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
        resArr.push(data.valueOf());
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
  executeUpdate("create database if not exists nodedb;");
  executeUpdate("use nodedb;");
});
// Clears the database and adds some testing data.
// Jest will wait for this promise to resolve before running tests.

afterAll(() => {
  executeUpdate("drop database if exists nodedb;");
  conn.close();
});

test(`name:test unsinged tinnyint ntable;author:${author};desc:create,insert,query with unsigned tinnyint;filename:${fileName};result:${result}`, () => {
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

test(`name:test unsinged smallint ntable;author:${author};desc:create,insert,query with unsigned smallint;filename:${fileName};result:${result}`, () => {
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

test(`name:test unsinged int ntable;author:${author};desc:create,insert,query with unsigned int;filename:${fileName};result:${result}`, () => {
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

test(`name:test unsinged bigint ntable;author:${author};desc:create,insert,query with unsigned bigint;filename:${fileName};result:${result}`, () => {
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
  console.log(actualResData);
  //assert result data length 
  expect(expectResData.length).toEqual(actualResData.length);
  //assert result data
  expectResData.forEach((item, index) => {
    console.log(index);
    expect(item).toEqual(actualResData[index]);
  });

  //assert result meta data
  expectResField.forEach((item, index) => {
    expect(item).toEqual(actualResFields[index])
  })
});

test(`name:test unsinged type ntable;author:${author};desc:create,insert,query with mutiple unsinged type;filename:${fileName};result:${result}`, () => {
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

test.skip(`name:test unsinged type stable;author:${author};desc:this is a description;filename:${fileName};result:${result}`, () => {
  let createSql = "create table if not exists unsigntest"+
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
                  ",desc nchar(200)" +
                  ");";
  executeUpdate(createSql);
  let expectResField = getFieldArr(getFeildsFromDll(createSql));
  let expectResData = [1641827743305, 254, 65534, 4294967294, 18446744073709551614n, 9223372036854775807n, 1641827743306, 0, 0, 0, 0n, -9223372036854775807n];
  let insertSql = buildInsertSql('sub1', 'unsigntest', expectResData, [], 6);

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





