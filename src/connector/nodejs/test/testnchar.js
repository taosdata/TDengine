const taos = require('../tdengine');
var conn = taos.connect({ host: "localhost" });
var c1 = conn.cursor();


function checkData(data, row, col, expect) {
	let checkdata = data[row][col];
	if (checkdata == expect) {
		// console.log('check pass')
	}
	else {
		console.log('check failed, expect ' + expect + ', but is ' + checkdata)
	}
}

c1.execute('drop database if exists testnodejsnchar')
c1.execute('create database testnodejsnchar')
c1.execute('use testnodejsnchar');
c1.execute('create table tb (ts timestamp, value float, text binary(200))')
c1.execute("insert into tb values('2021-06-10 00:00:00', 24.7, '中文10000000000000000000000');") -
c1.execute('insert into tb values(1623254400150, 24.7, NULL);')
c1.execute('import into tb values(1623254400300, 24.7, "中文3中文10000000000000000000000中文10000000000000000000000中文10000000000000000000000中文10000000000000000000000");')
sql = 'select * from tb;'

console.log('*******************************************')

c1.execute(sql);
data = c1.fetchall();
console.log(data)
//check data about insert data
checkData(data, 0, 2, '中文10000000000000000000000')
checkData(data, 1, 2, null)
checkData(data, 2, 2, '中文3中文10000000000000000000000中文10000000000000000000000中文10000000000000000000000中文10000000000000000000000')