"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.execute = execute;
const taosResult_1 = require("./taosResult");
function execute(sql, wsInterface) {
    return __awaiter(this, void 0, void 0, function* () {
        let taosResult;
        let wsQueryResponse = yield wsInterface.query(sql);
        try {
            taosResult = new taosResult_1.TaosResult(wsQueryResponse);
            if (wsQueryResponse.is_update == true) {
                return taosResult;
            }
            else {
                while (true) {
                    let wsFetchResponse = yield wsInterface.fetch(wsQueryResponse);
                    // console.log("[wsQuery.execute.wsFetchResponse]==>\n")
                    // console.log(wsFetchResponse)
                    // console.log(typeof BigInt(8))
                    // console.log(typeof wsFetchResponse.timing)
                    if (wsFetchResponse.completed == true) {
                        break;
                    }
                    else {
                        taosResult.setRows(wsFetchResponse);
                        let tmp = yield wsInterface.fetchBlock(wsFetchResponse, taosResult);
                        taosResult = tmp;
                    }
                }
                return taosResult;
            }
        }
        finally {
            wsInterface.freeResult(wsQueryResponse);
        }
    });
}
