const taos = require('../../tdengine');
const { getFeildsFromDll: getFelidFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'json_db'

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

describe("test json tag", () => {
    test(`name:json tag;` +
        `author:${author};` +
        `desc:create,insert,query with json tag;` +
        `filename:${fileName};` +
        `result:${result}`, () => {
            let tableName = 'jsons1';
            let createSql = `create table if not exists ${tableName}(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json);`;

            executeUpdate(createSql);
            let expectResField = getFieldArr(getFelidFromDll(createSql));
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
                // console.log(item,index,actualResFields[index]);
                expect(item).toEqual(actualResFields[index])
            })
        })
})