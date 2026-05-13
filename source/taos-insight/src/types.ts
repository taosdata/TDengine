import {DataQuery, DataSourceJsonData} from '@grafana/data'

export interface Query extends DataQuery {
    queryType?: string
    sql: string
    timeShiftPeriod?: number | string
    timeShiftUnit?: string
    expression?: string
}

export const DEFAULT_QUERY: Partial<Query> = {
    sql: 'show databases'
};

/**
 * These are options configured for each DataSource instance
 */
export interface DataSourceOptions extends DataSourceJsonData {
    isLoadAlerts?: boolean;
    folderUidSuffix?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface SecureJsonData {
    url?: string
    user?: string
    password?: string
    token?: string
    basicAuth?: string
    basicAuthPassword?: string
}

export type ChangeOptions<T> = {
    propertyName: keyof T;
    runQuery: boolean
    defaultValue?: String
}
