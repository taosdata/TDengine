import { request } from "@/utils/request";

//获取replication列表
export function getReplicationList(id) {
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?detail=true&labels=type::replication,cluster-id::${id}`,
        method: "get"
    });
}

export function addReplicationData(id,data){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/tasks?detail=true&labels=type::replication,cluster-id::${id}`,
        method: "post",
        headers: {
            "Content-Type": "application/json",
          },
          data
    });
}