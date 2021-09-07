import fetch from 'node-fetch'

export class Taos {
	host = "localhost"
	port = 6041
	user = "root"
	pass = "taosdata"
	https = false

	_apiUrl() {
		return (this.https ? "https" : "http") + "://" +
			this.host + ":" + this.port + "/rest/sql";
	}
	_token() {
		return 'Basic ' + Buffer.from(this.user + ':' + this.pass).toString('base64');
	}
	async query(sql) {
		let res = await fetch(this._apiUrl(), {
			method: 'post',
			body: sql,
			headers: { 'Authorization': this._token() }
		});
		return res.json();
	}
}
export const connect = (opts) => {
	return new Taos(opts)
}
export default {
	connect
}