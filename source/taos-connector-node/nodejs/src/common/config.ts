import { MinStmt2Version } from "./constant";

export class WSConfig {
    private _user: string | undefined | null;
    private _password: string | undefined | null;
    private _db: string | undefined | null;
    private _url: string;
    private _timeout: number | undefined | null;
    private _token: string | undefined | null;
    private _timezone: string | undefined | null;
    private _userApp: string | undefined | null;
    private _userIp: string | undefined | null;
    private _minStmt2Version: string;
    private _bearerToken: string | undefined | null;

    constructor(url: string, minStmt2Version?: string) {
        this._url = url;
        if (!minStmt2Version) {
            this._minStmt2Version = MinStmt2Version;
        } else {
            this._minStmt2Version = minStmt2Version;
        }
    }

    public getToken(): string | undefined | null {
        return this._token;
    }

    public setToken(token: string) {
        this._token = token;
    }

    public getUser(): string | undefined | null {
        return this._user;
    }

    public setUser(user: string) {
        this._user = user;
    }

    public getPwd(): string | undefined | null {
        return this._password;
    }

    public setPwd(pws: string) {
        this._password = pws;
    }

    public getDb(): string | undefined | null {
        return this._db;
    }

    public setDb(db: string) {
        this._db = db;
    }

    public getUrl(): string {
        return this._url;
    }

    public setUrl(url: string) {
        this._url = url;
    }

    public getTimeOut(): number | undefined | null {
        return this._timeout;
    }

    public setTimeOut(ms: number) {
        this._timeout = ms;
    }

    public getTimezone(): string | undefined | null {
        return this._timezone;
    }

    public setTimezone(timezone: string) {
        this._timezone = timezone;
    }

    public getUserApp(): string | undefined | null {
        return this._userApp;
    }

    public setUserApp(userApp: string) {
        this._userApp = userApp;
    }

    public getUserIp(): string | undefined | null {
        return this._userIp;
    }

    public setUserIp(userIp: string) {
        this._userIp = userIp;
    }

    public getBearerToken(): string | undefined | null {
        return this._bearerToken;
    }

    public setBearerToken(token: string) {
        this._bearerToken = token;
    }

    public getMinStmt2Version() {
        return this._minStmt2Version;
    }
}
