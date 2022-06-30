/* This example is to show the preferred way to use the td-connector */
/* To run, enter node path/to/node-example.js */
// Get the td-connector package
const taos = require('td2.0-connector');

/* We will connect to TDengine by passing an object comprised of connection options to taos.connect and store the
 * connection to the variable conn
 */
/*
 * Connection Options
 * host: the host to connect to
 * user: the use to login as
 * password: the password for the above user to login
 * config: the location of the taos.cfg file, by default it is in /etc/taos
 * port: the port we connect through
 */
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0});

// Initialize our TDengineCursor, which we use to interact with TDengine
var c1 = conn.cursor();

// c1.query(query) will return a TaosQuery object, of which then we can execute. The execute function then returns a promise
// Let's create a database named db
try {
	c1.execute('create database if not exists db;');
  //var query = c1.query('create database if not exists db;');
  //query.execute();
}
catch(err) {
  conn.close();
  throw err;
}

// Now we will use database db. As this query won't return any results,
// we can simplify the code and directly use the c1.execute() function. No need for a TaosQuery object to wrap around the query
try {
  c1.execute('use db;');
}
catch (err) {
  conn.close();
  throw err;
}

// Let's create a table called weather
// which stores some weather data like humidity, AQI (air quality index), temperature, and some notes as text
// We can also immedietely execute a TaosQuery object by passing true as the secodn argument
// This will then return a promise that we can then attach a callback function to
try {
  var promise = c1.query('create table if not exists weather (ts timestamp, humidity smallint, aqi int, temperature float, notes binary(30));', true);
  promise.then(function(){
    console.log("Table created!");
  }).catch(function() {
    console.log("Table couldn't be created.")
  });
}
catch (err) {
  conn.close();
  throw err;
}

// Let's get the description of the table weather
// When using a TaosQuery object and then executing it, upon success it returns a TaosResult object, which is a wrapper around the
// retrieved data and allows us to easily access data and manipulate or display it.
try {
  c1.query('describe db.weather;').execute().then(function(result){
    // Result is an instance of TaosResult and has the function pretty() which instantly logs a prettified version to the console
    result.pretty();
  });
}
catch (err) {
  conn.close();
  throw err;
}


Date.prototype.Format = function(fmt){
    var o = {
    	'M+': this.getMonth() + 1,
    	'd+': this.getDate(),
    	'H+': this.getHours(),
    	'm+': this.getMinutes(),
        's+': this.getSeconds(),
    	'S+': this.getMilliseconds()
	};
	if (/(y+)/.test(fmt)) {
		fmt = fmt.replace(RegExp.$1, (this.getFullYear() + '').substr(4 - RegExp.$1.length));
    }
    for (var k in o) {
		if (new RegExp('(' + k + ')').test(fmt)) {
			fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (('00' + o[k]).substr(String(o[k]).length)));
        }
    }
    return fmt;
}


// Let's try to insert some random generated data to test with
// We will use the bind function of the TaosQuery object to easily bind values to question marks in the query
// For Timestamps, a normal Datetime object or TaosTimestamp or milliseconds can be passed in through the bind function
let stime = new Date();
let interval = 1000;
try {
  for (let i = 0; i < 1000; i++) {
    stime.setMilliseconds(stime.getMilliseconds() + interval);
    
    //console.log(stime.Format('yyyy-MM-dd HH:mm:ss.SSS'));    

    let insertData = [stime,
                      parseInt(Math.random()*100),
                      parseInt(Math.random()*300),
                      parseFloat(Math.random()*10 + 30),
                      "Note"];
    //c1.execute('insert into db.weather values(' + insertData.join(',') + ' );');
    
    //var query = c1.query('insert into db.weather values(?, ?, ?, ?, ?);').bind(insertData);
    //query.execute();
    c1.execute('insert into db.weather values(\"'+stime.Format('yyyy-MM-dd HH:mm:ss.SSS')+'\",'+parseInt(Math.random() * 100)+','+parseInt(Math.random() * 300)+','+parseFloat(Math.random()*10 + 30)+',"Note");');
  }
}catch (err) {
  conn.close();
  throw err;
}

// Now let's look at our newly inserted data
var retrievedData;
try {
  c1.query('select * from db.weather limit 5 offset 100;', true).then(function(result){
    //result.pretty();
	console.log('=========>'+JSON.stringify(result));
    // Neat!
  });

}
catch (err) {
  conn.close();
  throw err;
}

// Let's try running some basic functions
try {
  c1.query('select count(*), avg(temperature), max(temperature), min(temperature), stddev(temperature) from db.weather;', true)
  .then(function(result) {
    result.pretty();
  })
}
catch(err) {
  conn.close();
  throw err;
}

conn.close();

// Feel free to fork this repository or copy this code and start developing your own apps and backends with NodeJS and TDengine!
