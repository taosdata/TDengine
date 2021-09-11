# TDengine Nodejs Restful

This is the Node.js library that lets you connect to [TDengine](https://www.github.com/taosdata/tdengine) though
restful. This restful can help you access the TDengine from different platform.

## Install

### On Linux

### On macOs

### On Windows

## Usage

### Connection

```javascript
import taoRest  from ''
var connRest = taoRest({})
```
close a connection 
```javascript
connRest.close()
```
query
```javascript
let data = connRest.Query("sql")
console.log ("data:"+data)
```

## Example
An example of using the NodeJS Restful connector to create a table with weather data and create and execute queries can be found [here](https://github.com/taosdata/TDengine/tree/master/tests/examples/node-rest/rest-node-example.js) 

An example of using the NodeJS Restful connector to achieve the same things but without all the object wrappers that wrap around the data returned to achieve higher functionality can be found [here](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example-raw.js)

## Contributing to TDengine

Please follow the [contribution guidelines](https://github.com/taosdata/TDengine/blob/master/CONTRIBUTING.md) to contribute to the project.

## License

[GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html)
