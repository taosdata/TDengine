import {
    DataQueryRequest,
    DataSourceInstanceSettings,
    MetricFindValue,
    ScopedVars,
} from '@grafana/data'
import {BackendSrv, DataSourceWithBackend, getBackendSrv, getTemplateSrv, TemplateSrv} from '@grafana/runtime'
import {DataSourceOptions, Query} from './types';
import _, {uniqBy} from "lodash";
import data from './alert_rules.json'
import { checkGrafanaVersion, getFolderUid } from './utils'

const timeFromMacroPattern = /\$__timeFrom(?:\s*\(\s*\))?/g;
const timeToMacroPattern = /\$__timeTo(?:\s*\(\s*\))?/g;
const timeFilterMacroPattern = /\$__timeFilter\s*\(\s*([^)]+?)\s*\)/g;

export class DataSource extends DataSourceWithBackend<Query, DataSourceOptions> {
    baseUrl: string
    backendSrv: BackendSrv
    template: TemplateSrv
    lastGeneratedSql = ''
    serverVersion = 0
    isLoadAlerts?: boolean
    folderUidSuffix?: string

    constructor(instanceSettings: DataSourceInstanceSettings<DataSourceOptions>) {
        super(instanceSettings);
        this.baseUrl = instanceSettings.url!
        this.backendSrv = getBackendSrv()
        this.template = getTemplateSrv()
        this.isLoadAlerts = instanceSettings.jsonData.isLoadAlerts;
        this.folderUidSuffix = instanceSettings.jsonData.folderUidSuffix;
    }

    filterQuery(query: Query): boolean {
        return !query.hide;
    }

    getAlertFolder(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            let uid = getFolderUid(`${this.uid}`)
            this.backendSrv.get(`/api/folders/${uid}`, {}, "", {showErrorAlert:false}).then((response) => {
                resolve(true)
               
            }).catch((e: any) => {
                console.error(e)
                if(e.status === 404) {
                    resolve(false)
                }else {
                    reject(e)
                }
            })
        })
    }

    createAlertFolder(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            let req = {uid: getFolderUid(`${this.uid}`), title: this.name + '-alert-' + this.folderUidSuffix}
            this.backendSrv.post("/api/folders", req).then(response=>{
                resolve(true)
            }).catch((e: any) => {
                console.error(e)
                if(e.status === 409) {
                    resolve(true)
                }else{
                    reject(e)
                }

            })
        });
    }

    async getAlerts(ruleGroup: string): Promise<boolean>{
        try{
            let uid = getFolderUid(`${this.uid}`)
            let path = `/api/v1/provisioning/folder/${uid}/rule-groups/${ruleGroup}`;
            let response = await this.backendSrv.get(path, {}, "", {showErrorAlert:false});
            if (!!response && response.rules.length > 0) {
                return true;
            }
            return false;
        }catch(e: any) {
            if(e.status === 404) {
                return false;
            }
            console.error(e);
            throw e;
        }
    }

    async loadAlerts(ruleGroup: string, data: any): Promise<boolean>{
        try{
            let uid = getFolderUid(`${this.uid}`)
            let path = `/api/v1/provisioning/folder/${uid}/rule-groups/${ruleGroup}`;
            let response = await this.backendSrv.put(path, data, {
                headers: {
                    'X-Disable-Provenance': 'true'
                }
            });

            if (!!response) {
                return true;
            }

            return false;
        }catch(e: any) {
            console.error(e);
            let err = {
                status: "error",
                message: "Failed to load alarm rule directory, reason: " + e.message,
                title: "Failed"
            };
            throw err;
        }
    }

    modifyAlertDataSource(ruleGroup: any) {
        ruleGroup.folderUid = getFolderUid(`${this.uid}`);
        let count = ruleGroup.rules.length;
        for (let index = 0; index < count; index++) {
            ruleGroup.rules[index].folderUID = ruleGroup.folderUid;
            ruleGroup.rules[index].data[0].datasourceUid = this.uid;
            ruleGroup.rules[index].data[0].model.datasource.uid = this.uid;
        }
    }

    sendInitAlert(): Promise<void> {
        return new Promise(async (resolve, reject) => {
            try {
                let bSuport = checkGrafanaVersion();
                if (bSuport) {
                    let bOk = await this.getAlertFolder();
                    if (!bOk) {
                        bOk = await this.createAlertFolder();
                    }

                    if (bOk) {
                        bOk = await this.getAlerts("alert_1m");
                        if (!bOk) {
                            this.modifyAlertDataSource(data.alert_1m);
                            await this.loadAlerts("alert_1m", data.alert_1m);
                        }

                        bOk = await this.getAlerts("alert_5m");
                        if (!bOk) {
                            this.modifyAlertDataSource(data.alert_5m);
                            await this.loadAlerts("alert_5m", data.alert_5m);
                        }

                        bOk = await this.getAlerts("alert_30s");
                        if (!bOk) {
                            this.modifyAlertDataSource(data.alert_30s);
                            await this.loadAlerts("alert_30s", data.alert_30s);
                        }

                        bOk = await this.getAlerts("alert_90s");
                        if (!bOk) {
                            this.modifyAlertDataSource(data.alert_90s);
                            await this.loadAlerts("alert_90s", data.alert_90s);
                        }

                        bOk = await this.getAlerts("alert_180s");
                        if (!bOk) {
                            this.modifyAlertDataSource(data.alert_180s);
                            await this.loadAlerts("alert_180s", data.alert_180s);
                        }

                        bOk = await this.getAlerts("alert_24h");
                        if (!bOk) {
                            this.modifyAlertDataSource(data.alert_24h);
                            await this.loadAlerts("alert_24h", data.alert_24h);
                        }
                        
                    }
                }
                resolve();
            } catch(e) {
                console.error(e);
                reject(e);
            }
        })
    }

    testDatasource() { // save & test button
        return this.request('show databases').then((response: any) => {
            if (!!response && response.status === 200 && !_.get(response, 'data.code')) {
                if (this.isLoadAlerts === true) {
                    return this.sendInitAlert().then(()=>{
                        return {status: "success", message: "TDengine Data source is working", title: "Success"};
                    }).catch((e: any) => {
                        return {status: "error", message: "TDengine Data source is working, but alert rules load failed, reason:" + e.message, title: "Failed"};
                    });
                } else {
                    return {status: "success", message: "TDengine Data source is working", title: "Success"};
                }

            }
            // TDengine error response format: {code: 855, desc: "Authentication failure"}
            return {
                status: "error",
                message: "TDengine Data source is not working, reason: " + (response.data?.desc || response.data?.message || 'Unknown error'),
                title: "Failed"
            };
        });
    }

    metricFindQuery(query: any, options: any): Promise<MetricFindValue[]> {
        return this.request(this.generateSql(query, options)).then(res => {
            if (!res?.data?.data) {
                return [];
            }

            const column_meta = res.data.column_meta || [];
            const hasTextAndValue = column_meta.length === 2 &&
                ((column_meta[0][0] === "__text" && column_meta[1][0] === "__value")
                    || (column_meta[1][0] === "__text" && column_meta[0][0] === "__value"));

            let values: MetricFindValue[] = [];
            if (hasTextAndValue) {
                let text_index = 0;
                let string_value = false;
                if (res.data.column_meta[1][0] === "__text") {
                    text_index = 1
                }
                let value_type = res.data.column_meta[1 - text_index][1].toUpperCase();
                if (value_type === "NCHAR" || value_type === "BINARY" || value_type === "VARCHAR") {
                    string_value = true
                }

                for (let i = 0; i < res.data.data.length; i++) {
                    let text = '' + res.data.data[i][text_index];
                    let value = res.data.data[i][1 - text_index]

                    if (string_value) {
                        value = "'" + value + "'";
                    }

                    values.push({text: text, value: value})
                }
            } else {
                for (let i = 0; i < res.data.data.length; i++) {
                    values.push({text: '' + res.data.data[i]});
                }
            }

            return uniqBy(values, "text");
        });
    }

    getGenerateSql(): string {
        return this.lastGeneratedSql
    }

    applyTemplateVariables(query: Query, scopedVars: ScopedVars): Query {
        const deprecatedError = this.getDeprecatedQueryError([query]);
        if (deprecatedError) {
            throw deprecatedError;
        }

        const queryType = query.queryType || 'SQL';
        const sql = this.interpolateSql(query.sql, scopedVars);
        this.lastGeneratedSql = sql;

        return {
            ...query,
            queryType,
            sql,
        };
    }

    request(sql: string) {
        if (!sql) {
            return new Promise<void>((resolve, reject) => {
                resolve();
            });
        }
        if (this.serverVersion === 0) {
            return this.backendSrv.datasourceRequest({
                url: this.baseUrl + "/sql",
                data: "select server_version()",
                method: 'POST',
            }).then((res: any) => {
                // Check if response contains error
                if (res.data?.code !== 0) {
                    return res;  // Return error response as-is
                }
                // Check if response has data
                if (!res.data?.data || !res.data.data[0]) {
                    return res;
                }
                if (res.data.data[0][0].startsWith("3")) {
                    this.serverVersion = 3;
                    return this.querySql(sql);
                } else {
                    this.serverVersion = 2;
                    return this.querySqlUtc(sql);
                }
            }).catch(err => {
                return err;
            });
        } else if (this.serverVersion === 3) {
            return this.querySql(sql);
        } else {
            return this.querySqlUtc(sql);
        }
    }

    querySql(sql: string) {
        return this.backendSrv.datasourceRequest({
            url: this.baseUrl + "/sql",
            data: sql,
            method: 'POST',
        }).then((result) => {
            result.data = this.convertResult(result.data);
            return result;
        }).catch(err => {
            console.error("catch error: ", err);
            return err;  // Return the error so it can be handled by testDatasource
        });
    }

    querySqlUtc(sql: string) {
        return this.backendSrv.datasourceRequest({
            url: this.baseUrl + "/sqlutc",
            data: sql,
            method: 'POST',
        });
    }

    convertResult(src: any) {
        let dist = {
            status: "",
            code: undefined,
            desc: undefined,
            column_meta: undefined,
            data: undefined,
            rows: 0,
        }
        if (src.code === 0) {
            dist.status = "succ";
            dist.column_meta = src.column_meta;
            // @ts-ignore
            dist.column_meta.forEach(element => {
                if (element[1] === "TIMESTAMP") {
                    element[1] = 9;
                }
            });
            dist.data = src.data;
            dist.rows = src.rows;
        } else {
            dist.status = "error";
            dist.code = src.code;
            dist.desc = src.desc;
        }

        return dist;
    }

    generateSql(sql: string, options: DataQueryRequest<Query>) {
        if (!sql || sql.length === 0) {
            return sql;
        }

        let queryStart = "now-1h";
        let queryEnd = "now";
        let intervalMs = "20000";
        if (!!options) {
            if (!!options.range && !!options.range.from) {
                queryStart = options.range.from.toISOString();
            }

            if (!!options.range && !!options.range.to) {
                queryEnd = options.range.to.toISOString();
            }

            if (!!options.intervalMs) {
                intervalMs = options.intervalMs.toString();
            }

            sql = this.applyTemplateReplacement(sql, options.scopedVars);
        }

        intervalMs += "a";
        const fromValue = queryStart.indexOf("now") < 0 ? "'" + queryStart + "'" : queryStart;
        const toValue = queryEnd.indexOf("now") < 0 ? "'" + queryEnd + "'" : queryEnd;

        sql = sql.replace(/\$from/g, fromValue);
        sql = sql.replace(/\$begin/g, fromValue);
        sql = sql.replace(/\$to/g, toValue);
        sql = sql.replace(/\$end/g, toValue);

        sql = sql.replace(timeFromMacroPattern, fromValue);
        sql = sql.replace(timeToMacroPattern, toValue);
        sql = sql.replace(timeFilterMacroPattern, (_match, column) => {
            return `(${column} >= ${fromValue} AND ${column} <= ${toValue})`;
        });
        if (sql.includes("$__timeFilter")) {
            throw new Error('macro $__timeFilter requires a column argument, e.g. $__timeFilter(ts)');
        }

        sql = sql.replace(/\$interval/g, intervalMs);

        return sql;
    }

    private interpolateSql(sql: string, scopedVars: ScopedVars): string {
        if (!sql || sql.length === 0) {
            return sql;
        }
        return this.applyTemplateReplacement(sql, scopedVars);
    }

    private applyTemplateReplacement(sql: string, scopedVars?: ScopedVars): string {
        let interpolatedSql = this.template.replace(sql, scopedVars, 'csv').replace(/^\s+|\s+$/gm, '');
        const allVariables = this.template.getVariables()

        // Helper function to escape special regex characters in variable names
        const escapeRegex = (str: string): string =>
            str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

        for (const variable of allVariables) {
            if ("current" in variable && variable.current && variable.current.value) {
                const value = String(variable.current.value);
                // Use regex with a negative lookahead so we don't match variable names
                // as prefixes of longer identifiers (e.g. avoid matching $db inside $dbname or $db2).
                const pattern = new RegExp(`\\$${escapeRegex(variable.name)}(?![0-9A-Za-z_])`, 'g');
                interpolatedSql = interpolatedSql.replace(pattern, value);
            }
        }

        return interpolatedSql;
    }

    private getDeprecatedQueryError(targets: Query[]): Error | undefined {
        for (const target of targets) {
            if (target.hide === true) {
                continue;
            }

            if (target.queryType && target.queryType !== 'SQL') {
                return new Error(
                    `Query ${target.refId || ''} uses deprecated queryType "${target.queryType}". ` +
                    'Use Grafana Expressions/Math for arithmetic instead.'
                );
            }

            if (this.hasDeprecatedTimeShift(target)) {
                return new Error(
                    `Query ${target.refId || ''} uses deprecated plugin timeShift. ` +
                    'Use the panel Query options "Time shift" instead.'
                );
            }
        }

        return undefined;
    }

    private hasDeprecatedTimeShift(target: Query): boolean {
        if (typeof target.timeShiftUnit === 'string' && target.timeShiftUnit.trim().length > 0) {
            return true;
        }

        if (target.timeShiftPeriod === undefined || target.timeShiftPeriod === null) {
            return false;
        }

        if (typeof target.timeShiftPeriod === 'string') {
            return target.timeShiftPeriod.trim().length > 0;
        }

        return Number(target.timeShiftPeriod) !== 0;
    }
}
