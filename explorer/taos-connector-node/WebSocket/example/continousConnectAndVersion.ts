import { connect } from "../index";

let ws = connect("ws://root:taosdata@127.0.0.1:6041/rest/ws")

ws.version()
.then((e) =>{console.log(e)} )
.then(()=>ws.version())
.then((e) =>{console.log(e)} )
.then(()=>ws.connect())
.then((e) =>{console.log(e)} )
.then(()=>ws.connect('test'))
.then((e) =>{console.log(e);ws.close()} )
.catch(e=>{console.log(e);ws.close();})



