import { config, getBackendSrv } from '@grafana/runtime';

export async function deleteAlerts(datasourceName: string): Promise<void> {
    try {
        
        let folderUid = getFolderUid(datasourceName);
        await deletGroupAlerts("alert_1m", folderUid);
        await deletGroupAlerts("alert_5m", folderUid);
        await deletGroupAlerts("alert_30s", folderUid);
        await deletGroupAlerts("alert_90s", folderUid);
        await deletGroupAlerts("alert_180s", folderUid);
        await deletGroupAlerts("alert_24h", folderUid);
        await deleteFolder(folderUid);
        // updateDataSourceOptions(datasourceName);
    } catch (error) {
        console.error('Error in async function:', error);
        throw error;
    }
}

// async function updateDataSourceOptions(datasourceUid: string) {
//     try {
//         let path = `/api/datasources/uid/${datasourceUid}`;
//         let response = await getBackendSrv().get(path);
//         response.jsonData.isLoadAlerts = false;
//         response = getBackendSrv().put(path, response);
//         console.log(response);
//     } catch(e) {
//         console.error(e);
//         throw e;
//     }
// }

function deletGroupAlerts(ruleGroup: string, datasourceName: string): Promise<boolean> {
    return new Promise(async (resolve, reject) => {
        let path = `/api/v1/provisioning/folder/${datasourceName}/rule-groups/${ruleGroup}`;
        getBackendSrv().delete(path, {}, {responseType :'text', showErrorAlert:false}).then(response => {resolve(true)}).catch(error => {
            console.error(error);
            resolve(false);
        })
    })
}

async function deleteFolder(folderUid: string): Promise<boolean> {
    return new Promise(async (resolve, reject) => {
        let path = `/api/folders/${folderUid}`
        getBackendSrv().delete(path, {}, {responseType :'text', showErrorAlert:false}).then(response => {resolve(true)}).catch(error => {
            console.error(error);
            resolve(false);
        })
    })
}

export function getFolderUid(datasourceUid: string): string {
    return `alert-${datasourceUid}-`;
}

export function checkGrafanaVersion(): boolean {
    const version = config.buildInfo.version;
    const versionParts = version.split(".");
    if (versionParts.length > 0) {
        const majorVersion = parseInt(versionParts[0], 10);
        if (majorVersion >= 11) {
            return true;
        }
    }
    return false;
}

export function getCurrentTime(): string {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

