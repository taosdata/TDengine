// import {describe, expect, test} from '@jest/globals';
import { connect } from "../../index"
const DSN = 'ws://root:taosdata@127.0.0.1:6041/rest/ws'
// const DSN = 'ws://root:taosdata@182.92.127.131:6041/rest/ws'
var ws = connect(DSN)

describe('TDWebSocket.connect() success ', () => {
    test('normal connect', async() => {
        let connRes = await ws.connect()
        expect(connRes.action).toBe('conn')
    })

    test.skip('connect fails with error', async() => {
        expect.assertions(1)
        try {
            await  ws.connect("jest");
        }catch(e){
            expect(e).toMatch('Invalid database name')
        }
    })

})

afterEach(() => {
    //close websocket and clear data
    ws.close()
})