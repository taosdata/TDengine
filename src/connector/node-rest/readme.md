# TDengine Nodejs Restful

This is the Node.js library that lets you connect to [TDengine](https://www.github.com/taosdata/tdengine) though
restful. This restful can help you access the TDengine from different platform.

## Install
To get started, just type in the following to install the connector through [npm](https://www.npmjs.com/)

```cmd
npm install td-rest-connector
```

## Usage

### Connection

```javascript
import taoRest  from 'TDengineRest'
var connRest = taoRest({host:'127.0.0.1',user:'root',pass:'taosdata',port:6041})
```

query
```javascript
(async()=>{
  data = await connRest.query("show databases");
  data.toString();
  }
)()
```

## Example
An example of using the NodeJS Restful connector to create a table with weather data and create and execute queries can be found [here](https://github.com/taosdata/TDengine/tree/master/tests/examples/node-rest/show-database.js) 

## Contributing to TDengine

Please follow the [contribution guidelines](https://github.com/taosdata/TDengine/blob/master/CONTRIBUTING.md) to contribute to the project.

## License

[GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html)
