"use strict";
/**
 * define ws Response type|class, for query?
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.WSConnResponse = exports.WSFetchBlockResponse = exports.WSFetchResponse = exports.WSQueryResponse = exports.WSVersionResponse = void 0;
class WSVersionResponse {
    constructor(msg) {
        this.version = msg.version;
        this.code = msg.code;
        this.message = msg.message;
        this.action = msg.action;
    }
}
exports.WSVersionResponse = WSVersionResponse;
class WSQueryResponse {
    constructor(msg) {
        this.code = msg.code;
        this.message = msg.message;
        this.action = msg.action;
        this.req_id = msg.req_id;
        this.timing = BigInt(msg.timing);
        this.id = BigInt(msg.id);
        this.is_update = msg.is_update;
        this.affected_rows = msg.affected_rows;
        this.fields_count = msg.fields_count;
        this.fields_names = msg.fields_names;
        this.fields_types = msg.fields_types;
        this.fields_lengths = msg.fields_lengths;
        this.precision = msg.precision;
    }
}
exports.WSQueryResponse = WSQueryResponse;
class WSFetchResponse {
    constructor(msg) {
        this.code = msg.code;
        this.message = msg.message;
        this.action = msg.action;
        this.req_id = msg.req_id;
        this.timing = BigInt(msg.timing);
        this.id = BigInt(msg.id);
        this.completed = msg.completed;
        this.length = msg.length;
        this.rows = msg.rows;
    }
}
exports.WSFetchResponse = WSFetchResponse;
class WSFetchBlockResponse {
    constructor(msg) {
        this.timing = new DataView(msg, 0, 8).getBigUint64(0, true);
        this.id = new DataView(msg, 8, 8).getBigUint64(0, true);
        this.data = msg.slice(16);
    }
}
exports.WSFetchBlockResponse = WSFetchBlockResponse;
class WSConnResponse {
    constructor(msg) {
        this.code = msg.code;
        this.message = msg.message;
        this.action = msg.action;
        this.req_id = msg.req_id;
        this.timing = BigInt(msg.timing);
    }
}
exports.WSConnResponse = WSConnResponse;
