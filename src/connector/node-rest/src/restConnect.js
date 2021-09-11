import {TDengineRestCursor} from '../src/restCursor'

/**
 *
 */
export class TDengineRestConnection {
  constructor(options) {
    this.host = 'localhost'
    this.port = '6041'
    this.user = 'root'
    this.pass = 'taosdata'
    this.path = '/rest/sqlt/'
    this._initConnection(options)
    return this
  }

  /**
   * this is a private function that 
   * @param options
   * @private
   */
  _initConnection(options) {
    if (options['host']) {
      this.host = options['host']
    }
    if (options['port']) {
      this.port = options['port']
    }
    if (options['user']) {
      this.user = options['user']
    }
    if (options['pass']) {
      this.pass = options['pass']
    }
    if (options['path']) {
      this.path = options['path']
    }
  }

  cursor() {
    return new TDengineRestCursor(this)
    console.log("return a cursor object user query sql")
  }
}





