import taos from "../src"

let conn = taos.connect();

(async () => {
	data = await conn.query("show databases");
	console.log(data);
})()