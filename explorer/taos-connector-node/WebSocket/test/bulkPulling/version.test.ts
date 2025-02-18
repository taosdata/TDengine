import { connect } from "../../index"
const DSN = 'ws://root:taosdata@127.0.0.1:6041/rest/ws'
// const DSN = 'ws://root:taosdata@182.92.127.131:6041/rest/ws'
var ws = connect(DSN)

describe('TDWebSocket.version()', () => {
    test('check version', () => {
        return ws.version()
        .then((version) => {
            // console.log(version)
            expect(version.charAt(0)).toEqual('3')
        })
        .then(()=>{ws.close()})
    })
})
afterAll(() => {
    //close websocket and clear data
    ws.close()
})