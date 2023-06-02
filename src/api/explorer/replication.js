import { request } from "@/utils/request";
let language=window.navigator.language.includes('en')?'en':'zh'
//获取replication列表
export function getReplicationList(id) {
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?lang=${language}&detail=true&labels=type::replication,cluster-id::${id}`,
        method: "get"
    });
}

export function addReplicationData(id,data){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?lang=${language}&detail=true&labels=type::replication,cluster-id::${id}`,
        method: "post",
        headers: {
            "Content-Type": "application/json",
          },
          data
    });
}