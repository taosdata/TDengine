import fetch  from 'node-fetch'

export class Taos {
  host = "localhost"
  port = 6041
  user = "root"
  pass = "taosdata"
  https = false

  constructor(options) {
    console.log("in constructor")
    this.host = options['host']
    this.port = options['port']
    this.user = options['user']
    this.pass = options['pass']
  }

  _apiUrl() {
    return (this.https ? "https" : "http") + "://" +
      this.host + ":" + this.port + "/rest/sql/";
  }

  _token() {
    return 'Basic ' + Buffer.from(this.user + ':' + this.pass).toString('base64');
  }

  async query(sql) {
    let res = await fetch(this._apiUrl(), { //'http://u195:6041/rest/sql '
      method: 'post',
      body: sql,
      headers: {'Authorization': this._token()}
    });
    return res.json();
  }
}

export const connect = (options) => {
  console.log("call index.js connect")
  return new Taos(options)
}
// const taos = {
//   connect
// }
// export default taos

export default {
  connect
}