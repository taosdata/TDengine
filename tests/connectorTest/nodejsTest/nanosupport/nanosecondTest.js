const taos = require('td2.0-connector');
var conn = taos.connect({host:"localhost", user:"root", password:"taosdata", config:"/etc/taos",port:6030})
var c1 = conn.cursor();


function checkData(sql,row,col,data){


    console.log(sql)
    c1.execute(sql)
    var d = c1.fetchall();
    let checkdata = d[row][col];
    if (checkdata == data)  {
        
        console.log('check pass')
    }
    else{
        console.log('check failed')
        console.log('checked is :',checkdata) 
        console.log("expected is :",data)
        

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
checkData(sql,3,0,'2021-06-10 00:00:00.299999999') 
checkData(sql,4,0,'2021-06-10 00:00:00.300000000')
checkData(sql,5,0,'2021-06-10 00:00:00.300000001')
checkData(sql,6,0,'2021-06-10 00:00:00.999999999') 
checkData(sql,0,1,1) 
checkData(sql,1,1,8)
checkData(sql,2,1,2)
checkData(sql,5,1,5)



// us basic case

c1.execute('reset query cache')
c1.execute('drop database if exists usdb')
c1.execute('create database usdb precision "us";')
c1.execute('use usdb');
c1.execute('create table tb (ts timestamp, speed int)')
c1.execute('insert into tb values(\'2021-06-10 00:00:00.100001\', 1);')
c1.execute('insert into tb values(1623254400150000, 2);')
c1.execute('import into tb values(1623254400300000, 3);')
c1.execute('import into tb values(1623254400299999, 4);')
c1.execute('insert into tb values(1623254400300001, 5);')
c1.execute('insert into tb values(1623254400999999, 7);')
c1.execute('insert into tb values(1623254400123789, 8);')
sql = 'select * from tb;'

console.log('*******************************************')

//check data about insert data
checkData(sql,0,0,'2021-06-10 00:00:00.100001')
checkData(sql,1,0,'2021-06-10 00:00:00.123789') 
checkData(sql,2,0,'2021-06-10 00:00:00.150000')
checkData(sql,3,0,'2021-06-10 00:00:00.299999') 
checkData(sql,4,0,'2021-06-10 00:00:00.300000')
checkData(sql,5,0,'2021-06-10 00:00:00.300001')
checkData(sql,6,0,'2021-06-10 00:00:00.999999') 
checkData(sql,0,1,1) 
checkData(sql,1,1,8)
checkData(sql,2,1,2)
checkData(sql,5,1,5)

console.log('*******************************************')

// ms basic case

c1.execute('reset query cache')
c1.execute('drop database if exists msdb')
c1.execute('create database msdb precision "ms";')
c1.execute('use msdb');
c1.execute('create table tb (ts timestamp, speed int)')
c1.execute('insert into tb values(\'2021-06-10 00:00:00.101\', 1);')
c1.execute('insert into tb values(1623254400150, 2);')
c1.execute('import into tb values(1623254400300, 3);')
c1.execute('import into tb values(1623254400299, 4);')
c1.execute('insert into tb values(1623254400301, 5);')
c1.execute('insert into tb values(1623254400789, 7);')
c1.execute('insert into tb values(1623254400999, 8);')
sql = 'select * from tb;'

console.log('*******************************************')
console.log('this is area about checkdata result')
//check data about insert data
checkData(sql,0,0,'2021-06-10 00:00:00.101')
checkData(sql,1,0,'2021-06-10 00:00:00.150') 
checkData(sql,2,0,'2021-06-10 00:00:00.299')
checkData(sql,3,0,'2021-06-10 00:00:00.300') 
checkData(sql,4,0,'2021-06-10 00:00:00.301')
checkData(sql,5,0,'2021-06-10 00:00:00.789')
checkData(sql,6,0,'2021-06-10 00:00:00.999')
checkData(sql,0,1,1) 
checkData(sql,1,1,2)
checkData(sql,2,1,4)
checkData(sql,5,1,7) 

console.log('*******************************************')

// offfical query result to show 
// console.log('this is area about fetch all data')
// var query = c1.query(sql)
// var promise = query.execute();
// promise.then(function(result) {
//   result.pretty(); 
// });

console.log('*******************************************')
c1.execute('use db')

sql2 = 'select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400100000002;'
checkData(sql2,0,0,1)

sql3 = 'select count(*) from tb where ts > \'2021-06-10 0:00:00.100000001\' and ts < \'2021-06-10 0:00:00.160000000\';'
checkData(sql3,0,0,2)

sql4 = 'select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400150000000;'
checkData(sql4,0,0,2)

sql5 = 'select count(*) from tb where ts > \'2021-06-10 0:00:00.100000000\' and ts < \'2021-06-10 0:00:00.150000000\';'
checkData(sql5,0,0,2)

sql6 = 'select count(*) from tb where ts > 1623254400400000000;'
checkData(sql6,0,0,1)

sql7 = 'select count(*) from tb where ts < \'2021-06-10 00:00:00.400000000\';'
checkData(sql7,0,0,6)

sql8 = 'select count(*) from tb where ts > now + 400000000b;'
c1.execute(sql8)

sql9 = 'select count(*) from tb where ts >= \'2021-06-10 0:00:00.100000001\';'
checkData(sql9,0,0,7)

sql10 = 'select count(*) from tb where ts <= 1623254400300000000;'
checkData(sql10,0,0,5)

sql11 = 'select count(*) from tb where ts = \'2021-06-10 0:00:00.000000000\';'
c1.execute(sql11)

sql12 = 'select count(*) from tb where ts = 1623254400150000000;'
checkData(sql12,0,0,1)

sql13 = 'select count(*) from tb where ts = \'2021-06-10 0:00:00.100000001\';'
checkData(sql13,0,0,1)

sql14 = 'select count(*) from tb where ts between 1623254400000000000 and 1623254400400000000;'
checkData(sql14,0,0,6)

sql15 = 'select count(*) from tb where ts between \'2021-06-10 0:00:00.299999999\' and \'2021-06-10 0:00:00.300000001\';'
checkData(sql15,0,0,3)

sql16 = 'select avg(speed) from tb interval(5000000000b);'
checkData(sql16,0,0,'2021-06-10 00:00:00.000000000')

sql17 = 'select avg(speed) from tb interval(100000000b)'
checkData(sql17,0,1,3.6666666666666665)
checkData(sql17,1,1,4.000000000)

checkData(sql17,2,0,'2021-06-10 00:00:00.300000000')
checkData(sql17,3,0,'2021-06-10 00:00:00.900000000')

console.log("print break ")

// sql18 = 'select avg(speed) from tb interval(999b)'
// c1.execute(sql18)

console.log("print break2 ")
sql19 = 'select avg(speed) from tb interval(1u);'
checkData(sql19,2,1,2.000000000)
checkData(sql19,3,0,'2021-06-10 00:00:00.299999000')

sql20 = 'select avg(speed) from tb interval(100000000b) sliding (100000000b);'
checkData(sql20,2,1,4.000000000)
checkData(sql20,3,0,'2021-06-10 00:00:00.900000000')

sql21 = 'select last(*) from tb;'
checkData(sql21,0,0,'2021-06-10 00:00:00.999999999')

sql22 = 'select first(*) from tb;'
checkData(sql22,0,0,'2021-06-10 00:00:00.100000001')

// timezone support 

console.log('testing nanosecond support in other timestamps')

c1.execute('create table tb2 (ts timestamp, speed int, ts2 timestamp);')
c1.execute('insert into tb2 values(\'2021-06-10 0:00:00.100000001\', 1, \'2021-06-11 0:00:00.100000001\');')
c1.execute('insert into tb2 values(1623254400150000000, 2, 1623340800150000000);')
c1.execute('import into tb2 values(1623254400300000000, 3, 1623340800300000000);')
c1.execute('import into tb2 values(1623254400299999999, 4, 1623340800299999999);')
c1.execute('insert into tb2 values(1623254400300000001, 5, 1623340800300000001);')
c1.execute('insert into tb2 values(1623254400999999999, 7, 1623513600999999999);')

sql23 = 'select * from tb2;'
checkData(sql23,0,0,'2021-06-10 00:00:00.100000001')
checkData(sql23,1,0,'2021-06-10 00:00:00.150000000')
checkData(sql23,2,1,4)
checkData(sql23,3,1,3)
checkData(sql23,4,2,'2021-06-11 00:00:00.300000001')
checkData(sql23,5,2,'2021-06-13 00:00:00.999999999')

sql24 = 'select count(*) from tb2 where ts2 >= \'2021-06-11 0:00:00.100000001\';'
checkData(sql24,0,0,6)

sql25 = 'select count(*) from tb2 where ts2 <= 1623340800400000000;'
checkData(sql25,0,0,5)

sql26 = 'select count(*) from tb2 where ts2 = \'2021-06-11 0:00:00.300000001\';'
checkData(sql26,0,0,1)

sql27 = 'select count(*) from tb2 where ts2 = 1623340800300000001;'
checkData(sql27,0,0,1)

sql28 = 'select count(*) from tb2 where ts2 between 1623340800000000000 and 1623340800450000000;'
checkData(sql28,0,0,5)

sql29 = 'select count(*) from tb2 where ts2 between \'2021-06-11 0:00:00.299999999\' and \'2021-06-11 0:00:00.300000001\';'
checkData(sql29,0,0,3)

sql30 = 'select count(*) from tb2 where ts2 <> 1623513600999999999;'
checkData(sql30,0,0,5)

sql31 = 'select count(*) from tb2 where ts2 <> \'2021-06-11 0:00:00.100000001\';'
checkData(sql31,0,0,5)

sql32 = 'select count(*) from tb2 where ts2 != 1623513600999999999;'
checkData(sql32,0,0,5)

sql33 = 'select count(*) from tb2 where ts2 != \'2021-06-11 0:00:00.100000001\';'
checkData(sql33,0,0,5)

c1.execute('insert into tb2 values(now + 500000000b, 6, now +2d);')

sql34 = 'select count(*) from tb2;'
checkData(sql34,0,0,7)


// check timezone support 

c1.execute('use db;')
c1.execute('create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);')
c1.execute('insert into stb1 using st tags("2021-06-10 0:00:00.123456789" , 1 ) values("2021-06-10T0:00:00.123456789+07:00" , 1.0);' )
sql35 = 'select first(*) from stb1;'
checkData(sql35,0,0,'2021-06-10 01:00:00.123456789')

c1.execute('use usdb;')
c1.execute('create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);')
c1.execute('insert into stb1 using st tags("2021-06-10 0:00:00.123456" , 1 ) values("2021-06-10T0:00:00.123456+07:00" , 1.0);' )
sql36 = 'select first(*) from stb1;'
checkData(sql36,0,0,'2021-06-10 01:00:00.123456')

c1.execute('use msdb;')
c1.execute('create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);')
c1.execute('insert into stb1 using st tags("2021-06-10 0:00:00.123456" , 1 ) values("2021-06-10T0:00:00.123456+07:00" , 1.0);' )
sql36 = 'select first(*) from stb1;'
checkData(sql36,0,0,'2021-06-10 01:00:00.123')







