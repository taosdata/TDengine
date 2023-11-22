
import { request } from "@/utils/request";
import i18n from '@/lang/index'
let language=i18n.locale.includes('zh')?'zh':'en'
export function getTask(id,type){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?lang=${i18n.locale}&detail=true&labels=type::${type},cluster-id::${id}`,
        method: "get"
    });
}

export function getRunningTask(){
    let id = localStorage.getItem("local_clusterID")
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?lang=${i18n.locale}&detail=true&labels=type::datain,cluster-id::${id}&in_scheduler=true`,
        method: "get"
    });
}


export function getUIData(){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/ds/in?lang=${i18n.locale}`,
        method:'get'
    })
}

export function AddSource(data){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:'/tasks',
        method:'post',
        headers:{
            "Content-Type":"application/json"
        },
        data
    })
}

export function EditSource(data,id){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/tasks/${id}`,
        method:'patch',
        headers:{
            "Content-Type":"application/json"
        },
        data
    })
}
//获取ua的nodes或者da的tags
export function getUaAndDaData(data){
    let language=i18n.locale.includes('zh')?'zh':'en'
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/ds/in/sets`,
        method:'post',
        headers:{
            "Content-Type":"application/json"
        },
        data: {
            ...data,
            lang: language
        }
    })
}

export function refreshTask(id){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/tasks/${id}?detail=true&lang=${language}`,
        method:'get',
        headers:{
            "Content-Type":"application/json"
        }
        
    })
}

export function uploadFile(file){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/upload`,
        method:'post',
        headers:{
            "Content-Type":"multipart/form-data"
        }
        
    })
}

export function getCSVColumns(path,type,hasheader){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/filemeta?file_path=${path}&file_type=${type}&has_header=${hasheader}`,
        method:'get',
        headers:{
            "Content-Type":"multipart/form-data"
        }
    })
}

export function getAgentActivities(agentId){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/agents/${agentId}/activities`,
        method:'get'
    })
}

export function getTaskActivities(taskId){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/tasks/${taskId}/activities`,
        method:'get'
    })
}

export function getMetrics(taskId){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/tasks/${taskId}/metrics`,
        method:'get'
    })
}

export function validateTask(data,agentid) {
   return request({
    baseURL:process.env.VUE_APP_X_API,
    url:`/ds/in/validate?dsn=${encodeURIComponent(data)}`+(agentid?`&via=${agentid}`:''),
    method:'get',
   })
}

export function getFileStream(filepath){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/download?file_path=${filepath}`,
        method:'get',
        responseType:'blob'
    })
}

export function downlaodAllNodes(data,agentid){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/ds/in/download/all_data_sets?from=${data}`+(agentid?`&via=${agentid}`:''),
        method:'get',
        responseType: 'blob',
    })
}
export function getMetricsDesc(data){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/metrics/description?lang=${i18n.locale}`,
        method:'get',
    })
}