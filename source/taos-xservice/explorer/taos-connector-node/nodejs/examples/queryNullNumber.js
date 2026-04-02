const taos = require('../tdengine');

var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
var c1 = conn.cursor();

function executeUpdate(sql) {
    console.log(sql);
    c1.execute(sql);
}
function queryData(sql) {
    let p = c1.query(sql);
    p.execute()
        .then((result) =>
            result.pretty()
        )
        .catch((err) => {
            console.log(err)
            c1.close()
        })
}
const database = "create database if not exists test";
const createTable = "create table testFollowNull (ts timestamp,i1 tinyint,i2 smallint,i4 int,i8 bigint,ui1 tinyint unsigned,ui2 smallint unsigned,ui4 int unsigned,ui8 bigint unsigned,f4 float,d8 double,ts8 timestamp,bnr binary(200),nchr nchar(300));"
const insert1 = "insert into testFollowNull values(now,null,null,null,null,null,null,null,null,null,null,null,null,null)"
const insert2 = " insert into testFollowNull values(now+1s,-1,-2,-4,-8,1,2,3,4,3.14,3.141292653,now+1s,'bnry_1','nchar_1')"
const use = "use test";
const select = "select * from test.testFollowNull"
const drop = "drop database test"
try {
    executeUpdate(database)
    executeUpdate(use)
    executeUpdate(createTable)
    executeUpdate(insert1)
    executeUpdate(insert2)
    queryData(select)
} finally {
    executeUpdate(drop)
   c1.close();
}



