const { TaosField } = require('../../nodetaos/taosobjects');
const taos = require('../../tdengine');
const { getFeildsFromDll, buildInsertSql, getFieldArr, getResData } = require('../utils/utilTools')


const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);
const db = 'fetch_field_db'

const C_TYPE = {
    'BOOL': 1,
    'TINYINT': 2,
    'SMALLINT': 3,
    'INT': 4,
    'BIGINT': 5,
    'FLOAT': 6,
    'DOUBLE': 7,
    'BINARY': 8,
    'VARCHAR': 8,
    'TIMESTAMP': 9,
    'NCHAR': 10,
    'TINYINT UNSIGNED': 11,
    'SMALLINT UNSIGNED': 12,
    'INT UNSIGNED': 13,
    'BIGINT UNSIGNED': 14,
    'JSON': 15,
}

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

describe("test fetchFeild", () => {
    test(`name:test fetchField for normal type;` +
        `author:${author};` +
        `desc:fetchField result test for normal type;` +
        `filename:${fileName};` +
        `result:${result}`, () => {
            let table = "meta_normal"
            let createTable = `create table if not exists ${table} ` +
                `(ts timestamp,` +
                `bl bool,` +
                `i8 tinyint,` +
                `i16 smallint,` +
                `i32 int,` +
                `i64 bigint,` +
                `f32 float,` +
                `d64 double,` +
                `bnr binary(50),` +
                `nchr nchar(50),` +
                `u8 tinyint unsigned,` +
                `u16 smallint unsigned,` +
                `u32 int unsigned,` +
                `u64 bigint unsigned` +
                `)tags(` +
                `t_bl bool,` +
                `t_i8 tinyint,` +
                `t_i16 smallint,` +
                `t_i32 int,` +
                `t_i64 bigint,` +
                `t_f32 float,` +
                `t_d64 double,` +
                `t_bnr binary(50),` +
                `t_nchr nchar(50),` +
                `t_u8 tinyint unsigned,` +
                `t_u16 smallint unsigned,` +
                `t_u32 int unsigned,` +
                `t_u64 bigint unsigned` +
                `);`
            let describeTable = `describe ${table};`
            let expectResField = getFieldArr(getFeildsFromDll(createTable));
            let expectFieldArr = new Array();

            for (let i = 0; i < expectResField.length; i++) {
                let tf = new TaosField(expectResField[i]);

                expectFieldArr.push(tf);
            }

            executeUpdate(createTable);
            let res = executeQuery(describeTable);
            let actualResDataArr = new Array()

            for (let i = 0; i < res.resData.length;) {

                let f = { 'name': res.resData[i], 'type': C_TYPE[res.resData[i + 1]], 'bytes': res.resData[i + 2] }
                let tf = new TaosField(f);

                actualResDataArr.push(tf)
                i += 4;
            }

            expect(actualResDataArr.length).toEqual(expectFieldArr.length);
            expectFieldArr.forEach((element, index) => {
                expect(element).toEqual(actualResDataArr[index]);
            });
        })
    test(`name:test fetchField for JSON tag` +
        `author:${author};` +
        `desc:fetchField result test for JSON tag;` +
        `filename:${fileName};` +
        `result:${result}`, () => {
            let table = "meta_json"
            let createTable = `create table if not exists ${table} ` +
                `(ts timestamp,` +
                `bl bool,` +
                `i8 tinyint,` +
                `i16 smallint,` +
                `i32 int,` +
                `i64 bigint,` +
                `f32 float,` +
                `d64 double,` +
                `bnr binary(50),` +
                `nchr nchar(50),` +
                `u8 tinyint unsigned,` +
                `u16 smallint unsigned,` +
                `u32 int unsigned,` +
                `u64 bigint unsigned` +
                `)tags(` +
                `json_tag json` +
                `);`
            let describeTable = `describe ${table};`
            let expectResField = getFieldArr(getFeildsFromDll(createTable));

            let expectFieldArr = new Array();
            for (let i = 0; i < expectResField.length; i++) {
                let tf = new TaosField(expectResField[i]);
                expectFieldArr.push(tf);
            }

            executeUpdate(createTable);
            let res = executeQuery(describeTable);
            let actualResDataArr = new Array()

            for (let i = 0; i < res.resData.length;) {

                let f = { 'name': res.resData[i], 'type': C_TYPE[res.resData[i + 1]], 'bytes': res.resData[i + 2] }
                let tf = new TaosField(f);

                actualResDataArr.push(tf)
                i += 4;
            }

            expect(actualResDataArr.length).toEqual(expectFieldArr.length);
            expectFieldArr.forEach((element, index) => {
                expect(element).toEqual(actualResDataArr[index]);
            });
        })
    })
