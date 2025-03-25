const CTaosInterface = require('../nodetaos/cinterface')
const ref =require('ref-napi')

var c_interface = new CTaosInterface();
var conn = c_interface.connect(host="127.0.0.1",user = "root", password = "taosdata",db="p")

// var res = c_interface.query(conn,"create table if not exists b (ts timestamp,i int)")
// c_interface.freeResult(res);


// res = c_interface.query(conn,"select * from b")
// c_interface.fetchRawBlock(res);
// c_interface.freeResult(res);

res = c_interface.query(conn,"select bin,nchr from t1;")// bnr,nchr, t_bnr
c_interface.fetchRawBlock(res);
c_interface.freeResult(res);



c_interface.close(conn);