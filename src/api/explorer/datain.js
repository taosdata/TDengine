import { request } from "@/utils/request";
let language=window.navigator.language.includes('en')?'en':'zh'
export function getDatain(id){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?lang=${language}&detail=true&labels=type::datain,cluster-id::${id}`,
        method: "get"
    });
}


export function getUIData(){
    let language=window.navigator.language.includes('en')?'en':'zh'
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url:`/ds/in?lang=${language}`,
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
    let language=window.navigator.language.includes('en')?'en':'zh'
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
        url:`/tasks/${id}?detail=true`,
        method:'get',
        headers:{
            "Content-Type":"application/json"
        }
        
    })
}