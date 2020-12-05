const taos = require('td2.0-connector');


var host = null;
var port = 6030;
for(var i = 2; i < global.process.argv.length; i++){
	var key = global.process.argv[i].split("=")[0];
	var value = global.process.argv[i].split("=")[1];
	
	if("host" == key){
		host = value;
	}
	if("port" == key){
		port = value;
	}
}

if(host == null){
	console.log("Usage: node nodejsChecker.js host=<hostname> port=<port>");
	process.exit(0);
}

// establish connection
var conn = taos.connect({host:host, user:"root", password:"taosdata",port:port});
var cursor = conn.cursor(); 
// create database
executeSql("create database if not exists test", 0);
// use db
executeSql("use test", 0);
// drop table
executeSql("drop table if exists test.weather", 0);
// create table
executeSql("create table if not exists test.weather(ts timestamp, temperature float, humidity int)", 0);
// insert
executeSql("insert into test.weather (ts, temperature, humidity) values(now, 20.5, 34)", 1);
// select
executeQuery("select * from test.weather");
// close connection
conn.close();

function executeQuery(sql){
	var start = new Date().getTime();
	var promise = cursor.query(sql, true);
	var end = new Date().getTime();
	promise.then(function(result){
		printSql(sql, result != null,(end - start));
		result.pretty();
	});
}

function executeSql(sql, affectRows){
	var start = new Date().getTime();
	var promise = cursor.execute(sql);
	var end = new Date().getTime();
	printSql(sql, promise == affectRows, (end - start));
}

function printSql(sql, succeed, cost){
	console.log("[ "+(succeed ? "OK" : "ERROR!")+" ] time cost: " + cost + " ms, execute statement ====> " + sql);
}
