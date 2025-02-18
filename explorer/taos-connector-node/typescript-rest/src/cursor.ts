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
            url: options.url,
            port: options.port,
            scheme: options.scheme,
            query: options.query,
            fragment: options.fragment,
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
        if (response.status == 200) {
            let res_json = await response.json();
            if (pure == false) {
                return new Result(res_json, sql);
            } else {
                return new Result(res_json);
            }

        } else if (response.status == 400) {
            new Error("invalid parameters.")
        } else if (response.status == 401) {
            throw new Error("Authentication failed.")
        } else if (response.status == 404) {
            throw new Error("interface not exists.")
        } else if (response.status == 500) {
            throw new Error("internal error.")
        } else if (response.status == 503) {
            throw new Error("insufficient system resource.")
        }
        throw new Error("http request failed.");
    }
}