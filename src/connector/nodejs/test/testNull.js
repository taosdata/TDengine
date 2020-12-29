const taos = require('../tdengine');
var conn = taos.connect({host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 6030});
var c1 = conn.cursor();


c1.query('select * from test.weather', true).then(function (result) {
    result.pretty();
});

conn.close();