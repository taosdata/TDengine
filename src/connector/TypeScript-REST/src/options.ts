export interface FetchOptions {
    method: 'POST';
    body: string;
    headers: Record<string, string>
}

export interface User {
    user: string;
    passwd: string;
}

export interface Uri {
    host: '127.0.0.1'|string;
    path: "/rest/sqlt" | '/rest/sqlutc' | '/rest/sql';
    port: 6041;
}