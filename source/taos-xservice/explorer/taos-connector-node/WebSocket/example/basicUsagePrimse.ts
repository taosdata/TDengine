import { connect } from "../index";

let ws = connect("ws://root:taosdata@127.0.0.1:6041/rest/ws")

const db = 'test';
const table = 'ws'
const createDB = `create database if not exists ${db} keep 3650`;
const useDB = `use ${db}`;
const dropDB = `drop database if exists ${db}`;
const createTB = `create table if not exists ${table}(ts timestamp,i8 tinyint,i16 smallint,i32 int,i64 bigint,bnr binary(40),nchr nchar(40))`;
const addColumn = `alter table ${db}.${table} add column new_column nchar(40) `;
const dropColumn = `alter table ${db}.${table} drop column new_column`;
const insertSql = `insert into ${db}.${table} values('2022-03-30 18:30:51.567',1,2,3,4,'binary1','nchar1')` +
    `('2022-03-30 18:30:51.568',5,6,7,8,'binary2','nchar2')` +
    `('2022-03-30 18:30:51.569',9,0,1,2,'binary3','nchar3')`;
const querySql = `select * from ${db}.${table}`;
const errorSql = 'show database';


ws.connect()
    .then(res=>console.log(res))   
    .then(() => ws.query(createDB))
    .then(res => { console.log(res) })
    .then(() => ws.query(useDB))
    .then(res => { console.log(res) })
    .then(() => ws.query(createTB))
    .then(res => { console.log(res) })
    .then(() => ws.query(insertSql))
    .then(res => console.log(res))
    .then(() => ws.query(addColumn))
    .then(res => console.log(res))
    .then(() => ws.query(dropColumn))
    .then(res => console.log(res))
    .then(() => ws.query(querySql))
    .then(res => console.log(res))
    .then(() => ws.query(dropDB))
    .then(res => console.log(res))
    .then(() => ws.query(errorSql))
    .then(res=>console.log(res))
    .then(() => ws.close())
    .catch(e => { console.log(e); ws.close() })


