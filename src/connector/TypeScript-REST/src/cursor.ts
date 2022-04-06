import { Uri, User } from './options'
import { TDResRequest } from './request'
import { Result } from './result'


export class TDengineCursor {
    field: Array<any>;
    data: Array<any>
    _rowCount: number;
    _uri: Uri;
    _user: User;

    constructor(options: any) {
        this._uri = {
            host: options.host,
            path: options.path,
            port: options.port,

        }
        this._user = {
            user: options.user,
            passwd: options.passwd,
        }
        this._rowCount = 0;
        this.field = [];
        this.data = [];

    }

    async query(sql: string, pure = true): Promise<Result> {
        let req = new TDResRequest(this._uri, this._user);
        let response = await req.request(sql);
        let res_json = await response.json();

        if (pure == false) {
            return new Result(res_json, sql);
        } else {
            return new Result(res_json);
        }

    }
}