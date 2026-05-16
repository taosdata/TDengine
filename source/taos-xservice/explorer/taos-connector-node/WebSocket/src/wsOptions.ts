export interface User {
    user?: string;
    passwd?: string;
}

export interface Uri {
    scheme: string;
    url?: string;
    host?: string | undefined | null;
    path: '/rest/sql/' | string;
    port?: number | undefined | null;
    query?: { [key: string]: string };
    fragment?: string | undefined | null;
}
