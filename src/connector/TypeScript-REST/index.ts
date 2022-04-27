
import {TDConnect,Options} from './src/connect';
let options:Options = {
    host : '127.0.0.1',
    port : 6041,
    path : '/rest/sql',
    user : 'root',
    passwd : 'taosdata'
}
let connect = function connect(option:Options){
    return new TDConnect(option);
}

export {options,connect}