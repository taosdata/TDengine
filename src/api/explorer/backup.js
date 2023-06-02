import { request } from "@/utils/request";
let language=window.navigator.language.includes('en')?'en':'zh'
//获取backup列表
export function getBackupList(id) {
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?lang=${language}&detail=true&labels=type::backup,cluster-id::${id}`,
        method: "get"
    });
}

//添加backup
export function addBackupData(clusterID,data) {
    return request({
        baseURL:process.env.VUE_APP_X_API,
        headers:{
            "Content-Type":"application/json"
        },
        url: `/tasks?lang=${language}&labels=type::backup,cluster-id::${clusterID}`,
        method: "post",
        data
    });
}

//编辑backup
export function editBackup(id,data){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks/${id}`,
        method: "patch",
        data
    });
}

//删除backup
export function deleteBackup(id){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks/${id}`,
        method: "delete"
    });
}