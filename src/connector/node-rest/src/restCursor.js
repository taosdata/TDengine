import fetch from 'node-fetch'
import {TDengineRestResultSet} from '../src/restResult'

export class TDengineRestCursor {
  constructor(connection) {
    this._connection = null;
    this.data = [];
    this.http = false
    if (connection != null) {
      this._connection = connection
    } else {
      throw new Error("A TDengineRestConnection object is required to be passed to the TDengineRestCursor")
    }
  }

  _apiUpl() {
    // console.log((this.http ? "https" : "http") + "://" + this._connection.host + ":" + this._connection.port + this._connection.path)
    return (this.http ? "https" : "http") + "://" + this._connection.host + ":" + this._connection.port + this._connection.path
  }

  _token() {
    return 'Basic ' + Buffer.from(this._connection.user + ":" + this._connection.pass).toString('base64')
  }

  async query(sql) {
    try {
      let response = await fetch(this._apiUpl(), {
        method: 'POST',
        body: sql,
        headers: {'Authorization': this._token()}
      })
      if (response.status >= 200 && response.status < 300) {

        return new TDengineRestResultSet(await response.json())
      } else {
        throw new Error(response.statusText)
      }
    } catch (e) {
      console.log("Request Failed " + e)
    }
  }

}