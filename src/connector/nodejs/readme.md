# TDengine Node.js connector
[![minzip](https://img.shields.io/bundlephobia/minzip/td-connector.svg)](https://github.com/taosdata/TDengine/tree/master/src/connector/nodejs) [![NPM](https://img.shields.io/npm/l/td-connector.svg)](https://github.com/taosdata/TDengine/#what-is-tdengine)

This is the Node.js library that lets you connect to [TDengine](https://www.github.com/taosdata/tdengine).

## Installation

To get started, just type in the following to install the connector through [npm](https://www.npmjs.com/)

```cmd
npm install td-connector
```

To interact with TDengine, we make use of the [node-gyp](https://github.com/nodejs/node-gyp) library. To install, you will need to install the following depending on platform (the following instructions are quoted from node-gyp)

### On Unix

- `python` (`v2.7` recommended, `v3.x.x` is **not** supported)
- `make`
- A proper C/C++ compiler toolchain, like [GCC](https://gcc.gnu.org)

### On macOS

- `python` (`v2.7` recommended, `v3.x.x` is **not** supported) (already installed on macOS)

- Xcode

  - You also need to install the

    ```
    Command Line Tools
    ```

     via Xcode. You can find this under the menu

    ```
    Xcode -> Preferences -> Locations
    ```

     (or by running

    ```
    xcode-select --install
    ```

     in your Terminal)

    - This step will install `gcc` and the related toolchain containing `make`

### On Windows

#### Option 1

Install all the required tools and configurations using Microsoft's [windows-build-tools](https://github.com/felixrieseberg/windows-build-tools) using `npm install --global --production windows-build-tools` from an elevated PowerShell or CMD.exe (run as Administrator).

#### Option 2

Install tools and configuration manually:

- Install Visual C++ Build Environment: [Visual Studio Build Tools](https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools) (using "Visual C++ build tools" workload) or [Visual Studio 2017 Community](https://visualstudio.microsoft.com/pl/thank-you-downloading-visual-studio/?sku=Community) (using the "Desktop development with C++" workload)
- Install [Python 2.7](https://www.python.org/downloads/) (`v3.x.x` is not supported), and run `npm config set python python2.7` (or see below for further instructions on specifying the proper Python version and path.)
- Launch cmd, `npm config set msvs_version 2017`

If the above steps didn't work for you, please visit [Microsoft's Node.js Guidelines for Windows](https://github.com/Microsoft/nodejs-guidelines/blob/master/windows-environment.md#compiling-native-addon-modules) for additional tips.

To target native ARM64 Node.js on Windows 10 on ARM, add the  components "Visual C++ compilers and libraries for ARM64" and "Visual  C++ ATL for ARM64".

## Usage

To use the connector, first request the library ```td-connector```. Running the function ```taos.connect``` with the connection options passed in as an object will return a TDengine connection object. A cursor needs to be intialized in order to interact with TDengine from node.

```javascript
const taos = require('td-connector');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
var c1 = conn.cursor(); // Initializing a new cursor
```

We can now start executing queries through the ```cursor.execute``` function.

```javascript
c1.execute('show databases;')
```

We can get the results of the queries by doing the following

```javascript
var data = c1.fetchall();
console.log(c1.fieldNames); // Returns the names of the columns/fields
console.log(data); // Logs all the data from the query as an array of arrays, each of which represents a row and data[row_number] is sorted in order of the fields
```

## Example

The following is an example use of the connector showing how to make a table with weather data, insert random data, and then retrieve it.

```javascript
// Get the td-connector package
const taos = require('td-connector');

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
  c1.execute('create database db;');
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

  // c1.fieldNames stores the names of each column retrieved
  console.log(c1.fieldNames);
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
  console.log(c1.fieldNames);
  console.log(c1.data);
}
catch(err) {
  conn.close();
  throw err;
}

conn.close();

// Feel free to fork this repository or copy this code and start developing your own apps and backends with NodeJS and TDengine!

```

## Contributing to TDengine

Please follow the [contribution guidelines](https://github.com/taosdata/TDengine/blob/master/CONTRIBUTING.md) to contribute to the project.

## License

[GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html)
