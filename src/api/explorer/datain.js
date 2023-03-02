import { request } from "@/utils/request";

export function getDatain(id){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?detail=true&labels=type::datain,cluster-id::${id}`,
        method: "get"
    });
}

export function getUIData(){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:'/ds/in',
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