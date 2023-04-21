import { request } from "@/utils/request";

export function getAgentsData(clusterid,userid){
    return request({
        baseURL:process.env.VUE_APP_X_API,
        url: `/agents?cluster_id=${clusterid}&user_id=${userid}`,
        method: "get"
    });
}