
import { Uri, User, FetchOptions } from "./options";
import fetch from 'node-fetch';

export class TDResRequest {
    uri: Uri;
    options: FetchOptions;
    user: User;
    queryParams = '';
    hashFragment = '';

    constructor(uri: Uri, user: User) {
        this.uri = uri;
        this.user = user;

        // if uri.url exists,and that is a cloud url(means token exist)
        if ((uri.query && uri.query['token']) || (uri.query && uri.query['token'])) {
            this.options = {
                method: 'POST',
                body: '',
                headers: {},
            }
        } else {
            this.options = {
                method: 'POST',
                body: '',
                headers: { 'Authorization': this._token() },
            }
        }
    }

    _makeUrl(): string {
        let url = '';
        if (this.uri.url) {
            this._constructUrlWithInput();
        } else {
            //do nothing 
        }
        if (this.uri.port) {
            url = `${this.uri.scheme}://${this.uri.host}:${this.uri.port}${this.uri.path}`
        } else {
            url = `${this.uri.scheme}://${this.uri.host}${this.uri.path}`
        }
        if (this.uri.query) {
            url += '?'
            Object.keys(this.uri.query).forEach(
                key => {
                    if (this.uri.query && (this.uri.query[key] || this.uri.query[key])) {
                        url += key + "=" + this.uri.query[key] + "&"
                    }
                }
            )
            // remove last "&"
            url = url.slice(0, url.length - 1);
            // console.log("query param:"+url)
        }
        if (this.queryParams) {
            url += this.queryParams;
        }

        if ((this.uri.fragment) || (this.uri.fragment)) {
            if (this.uri.fragment.slice(0, 1) == '#') {
                url += this.uri.fragment
            } else {
                url += '#' + this.uri.fragment;
            }
        }
        if (this.hashFragment) {
            url += this.hashFragment;
        }

        //console.log(`url:${url}`);
        return url;
    }
    // if user input url
    _constructUrlWithInput() {
        if (this.uri.url) {
            let urlObj = new URL(this.uri.url);
            if (urlObj.protocol) {
                this.uri.scheme = urlObj.protocol.slice(0, urlObj.protocol.length - 1);;
            }
            if (urlObj.hostname) {
                this.uri.host = urlObj.hostname;
            }
            if (urlObj.port) {
                this.uri.port = parseInt(urlObj.port);
            }
            if (urlObj.pathname != '/') {
                this.uri.path = urlObj.pathname;
            }
            if (urlObj.search) {
                this.queryParams = urlObj.search;
            }
            if (urlObj.hash) {
                this.hashFragment = urlObj.hash;
            }
        }
    }

    _token(): string {
        return `Basic ${Buffer.from(`${this.user.user}:${this.user.passwd}`).toString('base64')}`
    }
    _body(command: string): void {
        this.options.body = command;
    }

    request(command: string): Promise<any> {
        this._body(command);
        // console.log(this._makeUrl());
        // console.log(JSON.stringify(this.options));
        return fetch(this._makeUrl(), this.options);
    }
}


