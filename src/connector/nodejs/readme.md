# TDengine Node.js connector
[![minzip](https://img.shields.io/bundlephobia/minzip/td-connector.svg)](https://github.com/taosdata/TDengine/tree/master/src/connector/nodejs) [![NPM](https://img.shields.io/npm/l/td-connector.svg)](https://github.com/taosdata/TDengine/#what-is-tdengine)

This is the Node.js library that lets you connect to [TDengine](https://www.github.com/taosdata/tdengine). It is built so that you can use as much of it as you want or as little of it as you want through providing an extensive API. If you want the raw data in the form of an array of arrays for the row data retrieved from a table, you can do that. If you want to wrap that data with objects that allow you easily manipulate and display data such as using a prettifier function, you can do that!

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

The following is a short summary of the basic usage of the connector, the  full api and documentation can be found [here](http://docs.taosdata.com/node)

### Connection

To use the connector, first require the library ```td-connector```. Running the function ```taos.connect``` with the connection options passed in as an object will return a TDengine connection object. The required connection option is ```host```, other options if not set, will be the default values as shown below.

A cursor also needs to be initialized in order to interact with TDengine from Node.js.

```javascript
const taos = require('td-connector');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
var cursor = conn.cursor(); // Initializing a new cursor
```

Close a connection

```javascript
conn.close();
```

### Queries

We can now start executing simple queries through the ```cursor.query``` function, which returns a TaosQuery object.

```javascript
var query = cursor.query('show databases;')
```

We can get the results of the queries through the ```query.execute()``` function, which returns a promise that resolves with a TaosResult object, which contains the raw data and additional functionalities such as pretty printing the results.

```javascript
var promise = query.execute();
promise.then(function(result) {
  result.pretty(); //logs the results to the console as if you were in the taos shell
});
```

You can also query by binding parameters to a query by filling in the question marks in a string as so. The query will automatically parse what was binded and convert it to the proper format for use with TDengine
```javascript
var query = cursor.query('select * from meterinfo.meters where ts <= ? and areaid = ?;').bind(new Date(), 5);
query.execute().then(function(result) {
  result.pretty();
})
```

The TaosQuery object can also be immediately executed upon creation by passing true as the second argument, returning a promise instead of a TaosQuery.
```javascript
var promise = cursor.query('select * from meterinfo.meters where v1 = 30;', true)
promise.then(function(result) {
  result.pretty();
})
```

If you want to execute queries without objects being wrapped around the data, use ```cursor.execute()``` directly and ```cursor.fetchall()``` to retrieve data if there is any.
```javascript
cursor.execute('select count(*), avg(v1), min(v2) from meterinfo.meters where ts >= \"2019-07-20 00:00:00.000\";');
var data = cursor.fetchall();
console.log(cursor.fields); // Latest query's Field metadata is stored in cursor.fields
console.log(cursor.data); // Latest query's result data is stored in cursor.data, also returned by fetchall.
```

### Async functionality

Async queries can be performed using the same functions such as `cursor.execute`, `TaosQuery.query`, but now with `_a` appended to them.

Say you want to execute an two async query on two separate tables, using `cursor.query`, you can do that and get a TaosQuery object, which upon executing with the `execute_a` function, returns a promise that resolves with a TaosResult object.

```javascript
var promise1 = cursor.query('select count(*), avg(v1), avg(v2) from meter1;').execute_a()
var promise2 = cursor.query('select count(*), avg(v1), avg(v2) from meter2;').execute_a();
promise1.then(function(result) {
  result.pretty();
})
promise2.then(function(result) {
  result.pretty();
})
```

## Example

An example of using the NodeJS connector to create a table with weather data and create and execute queries can be found [here](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example.js) (The preferred method for using the connector)

An example of using the NodeJS connector to achieve the same things but without all the object wrappers that wrap around the data returned to achieve higher functionality can be found [here](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example-raw.js)

## Contributing to TDengine

Please follow the [contribution guidelines](https://github.com/taosdata/TDengine/blob/master/CONTRIBUTING.md) to contribute to the project.

## License

[GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html)
