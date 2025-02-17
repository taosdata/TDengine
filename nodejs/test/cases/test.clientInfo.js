const taos = require('../../tdengine');

const author = 'xiaolei';
const result = 'passed';
const fileName = __filename.slice(__dirname.length + 1);

describe("test getClientInfo()", () => {
    test(`name:test TDengineConnection.getClientInfo()()` +
        `author:${author};` +
        `desc:test getClientInfo;` +
        `filename:${fileName};` +
        `result:${result}`, () => {
            let conn = taos.connect({});
            let clientVersion = undefined; 
            clientVersion = conn.getClientInfo();
            console.log(clientVersion);
            // assert result data
            expect(clientVersion).toBeDefined();
            conn.close();
        })
})