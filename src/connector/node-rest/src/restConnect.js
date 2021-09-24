import {TDengineRestCursor} from '../src/restCursor'

/**
 *this class collect basic information that can be used to build
 * a restful connection.
 */
export class TDengineRestConnection {
  /**
   * constructor,give variables some default values
   * @param options
   * @returns {TDengineRestConnection}
   */
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
   * used to init the connection info using  the input options
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

  /**
   * cursor will return an object of TDengineRestCursor, which can send restful(http) request and get
   * the response from server.
   * @returns {TDengineRestCursor}
   */
  cursor() {
    return new TDengineRestCursor(this)
  }
}





