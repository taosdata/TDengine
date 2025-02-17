const taos = require('../../tdengine');

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);

// This is a taos connection
let conn;
// This is a Cursor
let c1;

describe("test getServerInfo()", () => {
    test(`name:test TDengineCursor.getClientInfo()()` +
        `author:${author};` +
        `desc:test getClientInfo;` +
        `filename:${fileName};` +
        `result:${result}`, () => {
            conn = taos.connect({ host: 'localhost' });
            let serverVersion = undefined;
            c1 = conn.cursor()
            serverVersion = c1.getServerInfo();

            // assert result data
            expect(serverVersion).toBeDefined();
            conn.close();
        })
})