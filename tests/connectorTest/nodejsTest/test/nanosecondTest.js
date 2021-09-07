const taos = require('../tdengine');
var conn = taos.connect({config:"/etc/taos"});
var c1 = conn.cursor();


function checkData(sql,row,col,data){

    c1.execute(sql)
    var d = c1.fetchall();
    let checkdata = d[row][col];
    if (checkdata == data)  {
        
        // console.log('check pass')
    }
    else{
        console.log('check failed')
        console.log(checkdata) 
        console.log(data)
        

    }
}


// nano basic case

c1.execute('reset query cache')
c1.execute('drop database if exists db')
c1.execute('create database db precision "ns";')
c1.execute('use db');
c1.execute('create table tb (ts timestamp, speed int)')
c1.execute('insert into tb values(\'2021-06-10 00:00:00.100000001\', 1);')
c1.execute('insert into tb values(1623254400150000000, 2);')
c1.execute('import into tb values(1623254400300000000, 3);')
c1.execute('import into tb values(1623254400299999999, 4);')
c1.execute('insert into tb values(1623254400300000001, 5);')
c1.execute('insert into tb values(1623254400999999999, 7);')
c1.execute('insert into tb values(1623254400123456789, 8);')
sql = 'select * from tb;'

console.log('*******************************************')
console.log('this is area about checkdata result')
//check data about insert data
checkData(sql,0,0,'2021-06-10 00:00:00.100000001')
checkData(sql,1,0,'2021-06-10 00:00:00.123456789') 
checkData(sql,2,0,'2021-06-10 00:00:00.150000000')
checkData(sql,3,0,'2021-06-10 00:00:00.299999999') //error
checkData(sql,4,0,'2021-06-10 00:00:00.300000000')
checkData(sql,5,0,'2021-06-10 00:00:00.300000001')
checkData(sql,6,0,'2021-06-10 00:00:00.999999999') //error

// // us basic case

// c1.execute('reset query cache')
// c1.execute('drop database if exists db')
// c1.execute('create database db precision "us";')
// c1.execute('use db');
// c1.execute('create table tb (ts timestamp, speed int)')
// c1.execute('insert into tb values(\'2021-06-10 00:00:00.100001\', 1);')
// c1.execute('insert into tb values(1623254400150000, 2);')
// c1.execute('import into tb values(1623254400300000, 3);')
// c1.execute('import into tb values(1623254400299999, 4);')
// c1.execute('insert into tb values(1623254400300001, 5);')
// c1.execute('insert into tb values(1623254400999999, 7);')
// c1.execute('insert into tb values(1623254400123789, 8);')
// sql = 'select * from tb;'

// console.log('*******************************************')

// //check data about insert data
// checkData(sql,0,0,'2021-06-10 00:00:00.100001')
// checkData(sql,1,0,'2021-06-10 00:00:00.123789') 
// checkData(sql,2,0,'2021-06-10 00:00:00.150000')
// checkData(sql,3,0,'2021-06-10 00:00:00.299999') 
// checkData(sql,4,0,'2021-06-10 00:00:00.300000')
// checkData(sql,5,0,'2021-06-10 00:00:00.300001')
// checkData(sql,6,0,'2021-06-10 00:00:00.999999') 

// console.log('*******************************************')

// // ms basic case

// c1.execute('reset query cache')
// c1.execute('drop database if exists db')
// c1.execute('create database db precision "ms";')
// c1.execute('use db');
// c1.execute('create table tb (ts timestamp, speed int)')
// c1.execute('insert into tb values(\'2021-06-10 00:00:00.101\', 1);')
// c1.execute('insert into tb values(1623254400150, 2);')
// c1.execute('import into tb values(1623254400300, 3);')
// c1.execute('import into tb values(1623254400299, 4);')
// c1.execute('insert into tb values(1623254400301, 5);')
// c1.execute('insert into tb values(1623254400789, 7);')
// c1.execute('insert into tb values(1623254400999, 8);')
// sql = 'select * from tb;'

// console.log('*******************************************')
// console.log('this is area about checkdata result')
// //check data about insert data
// checkData(sql,0,0,'2021-06-10 00:00:00.101')
// checkData(sql,1,0,'2021-06-10 00:00:00.150') 
// checkData(sql,2,0,'2021-06-10 00:00:00.299')
// checkData(sql,3,0,'2021-06-10 00:00:00.300') 
// checkData(sql,4,0,'2021-06-10 00:00:00.301')
// checkData(sql,5,0,'2021-06-10 00:00:00.789')
// checkData(sql,6,0,'2021-06-10 00:00:00.999') 

console.log('*******************************************')

// offfical query result to show 
// console.log('this is area about fetch all data')
// var query = c1.query(sql)
// var promise = query.execute();
// promise.then(function(result) {
//   result.pretty(); 
// });

console.log('*******************************************')
// checkData(sql,3,1,3)
// checkData(sql,4,1,5)
// checkData(sql,5,1,7)






// checkData(3,1,3)
// checkData(4,1,5)
// checkData(5,1,7)

// tdSql.query('select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400100000002;')
// tdSql.checkData(0,0,1)
// tdSql.query('select count(*) from tb where ts > \'2021-06-10 0:00:00.100000001\' and ts < \'2021-06-10 0:00:00.160000000\';')
// tdSql.checkData(0,0,1)

// tdSql.query('select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400150000000;')
// tdSql.checkData(0,0,1)
// tdSql.query('select count(*) from tb where ts > \'2021-06-10 0:00:00.100000000\' and ts < \'2021-06-10 0:00:00.150000000\';')
// tdSql.checkData(0,0,1)

// tdSql.query('select count(*) from tb where ts > 1623254400400000000;')
// tdSql.checkData(0,0,1)
// tdSql.query('select count(*) from tb where ts < \'2021-06-10 00:00:00.400000000\';')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb where ts > now + 400000000b;')
// tdSql.checkRows(0)

// tdSql.query('select count(*) from tb where ts >= \'2021-06-10 0:00:00.100000001\';')
// tdSql.checkData(0,0,6)

// tdSql.query('select count(*) from tb where ts <= 1623254400300000000;')
// tdSql.checkData(0,0,4)

// tdSql.query('select count(*) from tb where ts = \'2021-06-10 0:00:00.000000000\';')
// tdSql.checkRows(0)

// tdSql.query('select count(*) from tb where ts = 1623254400150000000;')
// tdSql.checkData(0,0,1)

// tdSql.query('select count(*) from tb where ts = \'2021-06-10 0:00:00.100000001\';')
// tdSql.checkData(0,0,1)

// tdSql.query('select count(*) from tb where ts between 1623254400000000000 and 1623254400400000000;')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb where ts between \'2021-06-10 0:00:00.299999999\' and \'2021-06-10 0:00:00.300000001\';')
// tdSql.checkData(0,0,3)

// tdSql.query('select avg(speed) from tb interval(5000000000b);')
// tdSql.checkRows(1)

// tdSql.query('select avg(speed) from tb interval(100000000b)')
// tdSql.checkRows(4)

// tdSql.error('select avg(speed) from tb interval(1b);')
// tdSql.error('select avg(speed) from tb interval(999b);')

// tdSql.query('select avg(speed) from tb interval(1000b);')
// tdSql.checkRows(5)

// tdSql.query('select avg(speed) from tb interval(1u);')
// tdSql.checkRows(5)

// tdSql.query('select avg(speed) from tb interval(100000000b) sliding (100000000b);')
// tdSql.checkRows(4)

// tdSql.query('select last(*) from tb')
// tdSql.checkData(0,0, '2021-06-10 0:00:00.999999999')
// tdSql.checkData(0,0, 1623254400999999999)

// tdSql.query('select first(*) from tb')
// tdSql.checkData(0,0, 1623254400100000001)
// tdSql.checkData(0,0, '2021-06-10 0:00:00.100000001')

// c1.execute('insert into tb values(now + 500000000b, 6);')
// tdSql.query('select * from tb;')
// tdSql.checkRows(7)

// tdLog.debug('testing nanosecond support in other timestamps')
// c1.execute('create table tb2 (ts timestamp, speed int, ts2 timestamp);')
// c1.execute('insert into tb2 values(\'2021-06-10 0:00:00.100000001\', 1, \'2021-06-11 0:00:00.100000001\');')
// c1.execute('insert into tb2 values(1623254400150000000, 2, 1623340800150000000);')
// c1.execute('import into tb2 values(1623254400300000000, 3, 1623340800300000000);')
// c1.execute('import into tb2 values(1623254400299999999, 4, 1623340800299999999);')
// c1.execute('insert into tb2 values(1623254400300000001, 5, 1623340800300000001);')
// c1.execute('insert into tb2 values(1623254400999999999, 7, 1623513600999999999);')

// tdSql.query('select * from tb2;')
// tdSql.checkData(0,0,'2021-06-10 0:00:00.100000001')
// tdSql.checkData(1,0,'2021-06-10 0:00:00.150000000')
// tdSql.checkData(2,1,4)
// tdSql.checkData(3,1,3)
// tdSql.checkData(4,2,'2021-06-11 00:00:00.300000001')
// tdSql.checkData(5,2,'2021-06-13 00:00:00.999999999')
// tdSql.checkRows(6)
// tdSql.query('select count(*) from tb2 where ts2 > 1623340800000000000 and ts2 < 1623340800150000000;')
// tdSql.checkData(0,0,1)
// tdSql.query('select count(*) from tb2 where ts2 > \'2021-06-11 0:00:00.100000000\' and ts2 < \'2021-06-11 0:00:00.100000002\';')
// tdSql.checkData(0,0,1)

// tdSql.query('select count(*) from tb2 where ts2 > 1623340800500000000;')
// tdSql.checkData(0,0,1)
// tdSql.query('select count(*) from tb2 where ts2 < \'2021-06-11 0:00:00.400000000\';')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb2 where ts2 > now + 400000000b;')
// tdSql.checkRows(0)

// tdSql.query('select count(*) from tb2 where ts2 >= \'2021-06-11 0:00:00.100000001\';')
// tdSql.checkData(0,0,6)

// tdSql.query('select count(*) from tb2 where ts2 <= 1623340800400000000;')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb2 where ts2 = \'2021-06-11 0:00:00.000000000\';')
// tdSql.checkRows(0)

// tdSql.query('select count(*) from tb2 where ts2 = \'2021-06-11 0:00:00.300000001\';')
// tdSql.checkData(0,0,1)

// tdSql.query('select count(*) from tb2 where ts2 = 1623340800300000001;')
// tdSql.checkData(0,0,1)

// tdSql.query('select count(*) from tb2 where ts2 between 1623340800000000000 and 1623340800450000000;')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb2 where ts2 between \'2021-06-11 0:00:00.299999999\' and \'2021-06-11 0:00:00.300000001\';')
// tdSql.checkData(0,0,3)

// tdSql.query('select count(*) from tb2 where ts2 <> 1623513600999999999;')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb2 where ts2 <> \'2021-06-11 0:00:00.100000001\';')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb2 where ts2 <> \'2021-06-11 0:00:00.100000000\';')
// tdSql.checkData(0,0,6)

// tdSql.query('select count(*) from tb2 where ts2 != 1623513600999999999;')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb2 where ts2 != \'2021-06-11 0:00:00.100000001\';')
// tdSql.checkData(0,0,5)

// tdSql.query('select count(*) from tb2 where ts2 != \'2021-06-11 0:00:00.100000000\';')
// tdSql.checkData(0,0,6)

// c1.execute('insert into tb2 values(now + 500000000b, 6, now +2d);')
// tdSql.query('select * from tb2;')
// tdSql.checkRows(7)

// tdLog.debug('testing ill nanosecond format handling')
// c1.execute('create table tb3 (ts timestamp, speed int);')

// tdSql.error('insert into tb3 values(16232544001500000, 2);')
// c1.execute('insert into tb3 values(\'2021-06-10 0:00:00.123456\', 2);')
// tdSql.query('select * from tb3 where ts = \'2021-06-10 0:00:00.123456000\';')
// tdSql.checkRows(1)

// c1.execute('insert into tb3 values(\'2021-06-10 0:00:00.123456789000\', 2);')
// tdSql.query('select * from tb3 where ts = \'2021-06-10 0:00:00.123456789\';')
// tdSql.checkRows(1)

// # check timezone support 

// c1.execute('use db;')
// c1.execute('create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);')
// c1.execute('insert into tb1 using st tags("2021-06-10 0:00:00.123456789" , 1 ) values("2021-06-10 0:00:00.123456789+07:00" , 1.0);' )
// tdSql.query("select first(*) from tb1;")
// tdSql.checkData(0,0,1623258000123456789)
// c1.execute('insert into tb1 using st tags("2021-06-10 0:00:00.123456789" , 1 ) values("2021-06-10T0:00:00.123456789+06:00" , 2.0);' )
// tdSql.query("select last(*) from tb1;")
// tdSql.checkData(0,0,1623261600123456789)

// c1.execute('create database usdb precision "us";')
// c1.execute('use usdb;')
// c1.execute('create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);')
// c1.execute('insert into tb1 using st tags("2021-06-10 0:00:00.123456" , 1 ) values("2021-06-10 0:00:00.123456+07:00" , 1.0);' )
// res = tdSql.getResult("select first(*) from tb1;")
// print(res)
// if res == [(datetime.datetime(2021, 6, 10, 1, 0, 0, 123456), 1.0)]:
//     tdLog.info('check timezone pass about us database')

// c1.execute('create database msdb precision "ms";')
// c1.execute('use msdb;')
// c1.execute('create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);')
// c1.execute('insert into tb1 using st tags("2021-06-10 0:00:00.123" , 1 ) values("2021-06-10 0:00:00.123+07:00" , 1.0);' )
// res = tdSql.getResult("select first(*) from tb1;")
// print(res)
// if res ==[(datetime.datetime(2021, 6, 10, 1, 0, 0, 123000), 1.0)]:
//     tdLog.info('check timezone pass about ms database')










// c1.execute('create database if not exists ' + dbname + ' precision "ns"');
// c1.execute('use ' + dbname)
// c1.execute('create table if not exists tstest (ts timestamp, _int int);');
// c1.execute('insert into tstest values(1625801548423914405, 0)');
// // Select
// console.log('select * from tstest');
// c1.execute('select * from tstest');

// var d = c1.fetchall();
// console.log(c1.fields);
// let ts = d[0][0];
// console.log(ts);

// if (ts.taosTimestamp() != 1625801548423914405) {
//   throw "nanosecond not match!";
// }
// if (ts.getNanoseconds() % 1000000 !== 914405) {
//   throw "nanosecond precision error";
// }
// setTimeout(function () {
//   c1.query('drop database nodejs_ns_test;');
// }, 200);

// setTimeout(function () {
//   conn.close();
// }, 2000);


