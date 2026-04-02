import { TDengineCursor } from './cursor'
import { Uri, User } from './options'

/**
 * Options used to connect with REST(taosAdapter)
 * Need to set options with 'host','path','port','user','passwd'.
 * connWith is optional attribute for further use.
 */
export interface Options extends Uri, User {
    connWith?: 'rest' | 'taosc'
}

/**
 * Create connect with TDengine,actually pass options 
 * to `Cursor` which send and receive HTTP request.
 */
export class TDConnect {

    _connOption: Options;

    constructor(connOption: Options) {
        this._connOption = connOption
    }

    cursor(): TDengineCursor {
        return new TDengineCursor(this._connOption);
    }

}
