interface User {
    user: string;
    passwd: string;
}
interface Uri {
    host: '127.0.0.1';
    path: "/rest/sqlt" | '/rest/sqlutc' | '/rest/sql';
    port: 6041;
}
interface IResult {
    status: string;
    head?: Array<string>;
    column_meta?: Array<Array<any>>;
    data?: Array<Array<any>>;
    rows?: number;
    command?: string;
    code?: number;
    desc?: string;
}
interface meta {
    columnName: string;
    code: number;
    size: number;
    typeName?: string;
}
declare class Result {
    constructor(res: IResult, commands?: string);
    getResult(): Result;
    getStatus(): string;
    getHead(): Array<any> | undefined;
    getMeta(): Array<meta> | undefined;
    getData(): Array<Array<any>> | undefined;
    getAffectRows(): number | undefined;
    getCommand(): string | undefined;
    getErrCode(): number | undefined;
    getErrStr(): string | undefined;
    toString(): void;
}
declare class TDengineCursor {
    field: Array<any>;
    data: Array<any>;
    _rowCount: number;
    _uri: Uri;
    _user: User;
    constructor(options: any);
    query(sql: string, pure?: boolean): Promise<Result>;
}
/**
 * Options used to connect with REST(taosAdapter)
 * Need to set options with 'host','path','port','user','passwd'.
 * connWith is optional attribute for further use.
 */
interface Options extends Uri, User {
    connWith?: 'rest' | 'taosc';
}
/**
 * Create connect with TDengine,actually pass options
 * to `Cursor` which send and receive HTTP request.
 */
declare class TDConnect {
    _connOption: Options;
    constructor(connOption: Options);
    cursor(): TDengineCursor;
}
export let options: Options;
export let connect: (option: Options) => TDConnect;
