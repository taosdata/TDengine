/**
 * define ws Response type|class, for query?
 */


export class WSVersionResponse {

    version: string;
    code: number;
    message: string;
    action: string;

    constructor(msg: any) {

        this.version = msg.version;
        this.code = msg.code;
        this.message = msg.message;
        this.action = msg.action;
    }
}

export class WSQueryResponse {
    code: number;
    message: string;
    action: string;
    req_id: number;
    timing: bigint;
    id: bigint;
    is_update: boolean;
    affected_rows: number;
    fields_count: number | null;
    fields_names: Array<string> | null;
    fields_types: Array<number> | null;
    fields_lengths: Array<number> | null;
    precision: number;

    constructor(msg: any) {
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

export class WSFetchResponse {
    code: number;
    message: string;
    action: string;
    req_id: number;
    timing: bigint;
    id: bigint;
    completed: boolean;
    length: Array<number>;
    rows: number;

    constructor(msg: any) {
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

export class WSFetchBlockResponse {

    id: bigint
    data: ArrayBuffer
    timing: bigint
    constructor(msg: ArrayBuffer) {
        this.timing = new DataView(msg, 0, 8).getBigUint64(0, true)
        this.id = new DataView(msg, 8, 8).getBigUint64(0, true)
        this.data = msg.slice(16)
    }
}

interface IWSConnResponse {
    code: number;
    message: string;
    action: string;
    req_id: number;
    timing: bigint;
}

export class WSConnResponse {
    code: number;
    message: string;
    action: string;
    req_id: number;
    timing: bigint;

    constructor(msg: IWSConnResponse) {
        this.code = msg.code;
        this.message = msg.message;
        this.action = msg.action;
        this.req_id = msg.req_id;
        this.timing = BigInt(msg.timing);
    }
}
