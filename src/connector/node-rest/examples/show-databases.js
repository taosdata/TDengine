import taos from "../src"
// import taos from "taos-rest"

let option = {
	host:'u195',
	port:'6041',
	pass:'taosdata',
	user:'root'
}
let conn = taos.connect(option);
console.log("url showDataBases:" + conn._apiUrl()) ;

(async () => {
	data = await conn.query("show databases").catch(err => {
		console.log(err);
	});
	console.log(data);
})()