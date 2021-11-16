const taos = require('../tdengine');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:10});
var c1 = conn.cursor();
let stime = new Date();
let interval = 1000;
c1.execute('use td_connector_test');
let sub = c1.subscribe({
  restart: true,
  sql: "select AVG(_int) from td_connector_test.all_Types;",
  topic: 'all_Types',
  interval: 1000
});

c1.consumeData(sub, (data, fields) => {
  console.log(data);
});