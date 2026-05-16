import {TDengineWebSocket} from './src/tdengineWebsocket'
import { TaosResult } from './src/taosResult';


let connect = (url:string)=>{
    return new TDengineWebSocket(url)
}

export{connect, TaosResult}