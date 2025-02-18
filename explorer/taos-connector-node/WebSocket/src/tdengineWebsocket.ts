
import { execute } from './wsQuery'
import { TaosResult } from './taosResult'
import { WSInterface } from './wsQueryInterface'
import { WSConnResponse } from './wsQueryResponse'

 
export class TDengineWebSocket {
    _wsInterface: WSInterface
    _data: Array<any> = []
    _meta: Array<any> = []

    constructor(url: string) {
        this._wsInterface = new WSInterface(new URL(url))
    }

    connect(database?:string):Promise<WSConnResponse> {
        return this._wsInterface.connect(database)
    }

    state(){
        return this._wsInterface.getState();
    }

    /**
     * return client version.
     */
    version(): Promise<string> {
        return this._wsInterface.version()
    }

    query(sql:string):Promise<TaosResult>{
        return execute(sql,this._wsInterface)
    }
    close() {
        this._wsInterface.close();
    }

} 