/* This example is to show how to use the td-connector through the cursor only and is a bit more raw.
 * No promises, object wrappers around data, functions that prettify the data, or anything.
 * The cursor will generally use callback functions over promises, and return and store the raw data from the C Interface.
 * It is advised to use the td-connector through the cursor and the TaosQuery class amongst other higher level APIs.
*/

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

// c1.execute(query) will execute the query
// Let's create a database named db
try {
  c1.execute('create database if not exists db;');
}
catch(err) {
  conn.close();
  throw err;
}

// Now we will use database db
try {
  c1.execute('use db;');
}
catch (err) {
  conn.close();
  throw err;
}

// Let's create a table called weather
// which stores some weather data like humidity, AQI (air quality index), temperature, and some notes as text
try {
  c1.execute('create table if not exists weather (ts timestamp, humidity smallint, aqi int, temperature float, notes binary(30));');
}
catch (err) {
  conn.close();
  throw err;
}

// Let's get the description of the table weather
try {
  c1.execute('describe db.weather');
}
catch (err) {
  conn.close();
  throw err;
}

// To get results, we run the function c1.fetchall()
// It only returns the query results as an array of result rows, but also stores the latest results in c1.data
try {
  var tableDesc = c1.fetchall(); // The description variable here is equal to c1.data;
  console.log(tableDesc);
}
catch (err) {
  conn.close();
  throw err;
}

// Let's try to insert some random generated data to test with

let stime = new Date();
let interval = 1000;

// Timestamps must be in the form of "YYYY-MM-DD HH:MM:SS.MMM" if they are in milliseconds
//                                   "YYYY-MM-DD HH:MM:SS.MMMMMM" if they are in microseconds
// Thus, we create the following function to convert a javascript Date object to the correct formatting
function convertDateToTS(date) {
  let tsArr = date.toISOString().split("T")
  return "\"" + tsArr[0] + " " + tsArr[1].substring(0, tsArr[1].length-1) + "\"";
}

try {
  for (let i = 0; i < 10000; i++) {
    stime.setMilliseconds(stime.getMilliseconds() + interval);
    let insertData = [convertDateToTS(stime),
                      parseInt(Math.random()*100),
                      parseInt(Math.random()*300),
                      parseFloat(Math.random()*10 + 30),
                      "\"random note!\""];
    c1.execute('insert into db.weather values(' + insertData.join(',') + ' );');
  }
}
catch (err) {
  conn.close();
  throw err;
}

// Now let's look at our newly inserted data
var retrievedData;
try {
  c1.execute('select * from db.weather;')
  retrievedData = c1.fetchall();

  // c1.fields stores the names of each column retrieved
  console.log(c1.fields);
  console.log(retrievedData);
  // timestamps retrieved are always JS Date Objects
  // Numbers are numbers, big ints are big ints, and strings are strings
}
catch (err) {
  conn.close();
  throw err;
}

// Let's try running some basic functions
try {
  c1.execute('select count(*), avg(temperature), max(temperature), min(temperature), stddev(temperature) from db.weather;')
  c1.fetchall();
  console.log(c1.fields);
  console.log(c1.data);
}
catch(err) {
  conn.close();
  throw err;
}

conn.close();

// Feel free to fork this repository or copy this code and start developing your own apps and backends with NodeJS and TDengine!
