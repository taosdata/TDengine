import { request } from "@/utils/request";

//获取backup列表
export function getBackupList(id) {
    
    return request({
        url: `/tasks?detail=true&labels=type::backup,cluster-id::${id}`,
        method: "get",
        headers:{
            myHeader:process.env.VUE_APP_X_API
        }
    });
}

//添加backup
export function addBackupData(clusterID,data) {
    return request({
        headers:{
            myHeader:process.env.VUE_APP_X_API,
            "Content-Type":"application/json"
        },
        url: `/tasks?labels=type::backup,cluster-id::${clusterID}`,
        method: "post",
        data
    });
}

//编辑backup
export function editBackup(id,data){
    return request({
        headers:{
            myHeader:process.env.VUE_APP_X_API
        },
        url: `/tasks/${id}`,
        method: "pust",
        data
    });
}

//删除backup
export function deleteBackup(id){
    return request({
        headers:{
            myHeader:process.env.VUE_APP_X_API
        },
        url: `/tasks/${id}`,
        method: "delete"
    });
}