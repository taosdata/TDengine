// import {describe, expect, test} from '@jest/globals';
import { connect } from "../../index";
const DSN =
  "ws://localhost:8085/rest/ws?token=da082dedcca5cbb69a7c719142f194e44ad9472c";
// const DSN = 'ws://root:taosdata@182.92.127.131:6041/rest/ws'
var ws = connect(DSN);
testCon();
async function testCon() {
  try {
    await ws.connect();
  } catch (Err) {
    console.log(Err);
  }
}

// describe("TDWebSocket.connect() success ", () => {
//   test("normal connect", async () => {

//     expect(connRes.action).toBe("conn");
//   });

//   test.skip("connect fails with error", async () => {
//     expect.assertions(1);
//     try {
//       await ws.connect("jest");
//     } catch (e) {
//       expect(e).toMatch("Invalid database name");
//     }
//   });
// });

// afterEach(() => {
//   //close websocket and clear data
//   ws.close();
// });
