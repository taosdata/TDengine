import { Agent } from "http";

export interface FetchOptions {
    // These properties are part of the Fetch Standard
    method: string;
     // request body. can be null, a string, a Buffer, a Blob, or a Node.js Readable stream
    body: string;
    // request headers.
    headers: { [key: string]: string };//Record<string, string>;
    // set to `manual` to extract redirect headers, `error` to reject redirect
    redirect?: "error" | "follow" | "manual";
    // pass an instance of AbortSignal to optionally abort requests
    signal?: AbortSignal| null | undefined; 
    
     // The following properties extensions
    follow?: number|undefined;         
    // req/res timeout in ms, it resets on redirect. 0 to disable (OS limit applies). Signal is recommended instead.
    timeout?: number|undefined,         
    // support gzip/deflate content encoding. false to disable
    compress?: boolean|undefined;
    // maximum response body size in bytes. 0 to disable
    size?: number|undefined,            
    agent?:Agent|undefined
}

export interface User {
    user?: string;
    passwd?: string;
}

export interface Uri {
    scheme: string;
    url?:string;
    host?: string|undefined|null;
    path: '/rest/sql/'|string;
    port?: number|undefined|null;
    query?: {[key:string]:string};
    fragment?: string|undefined|null;
}



