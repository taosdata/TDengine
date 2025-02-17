
import { TDConnect, Options } from './src/connect';
let options: Options = {
    path: '/rest/sql/',
    scheme: 'http',
    user: 'root',
    passwd: 'taosdata',
    host: "127.0.0.1",
}
let connect = function connect(option: Options) {
    // console.log("index.options:"+JSON.stringify(option));
    return new TDConnect(option);
}

export { options, connect }
