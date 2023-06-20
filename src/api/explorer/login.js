import { request } from "@/utils/request";

//获取cluster的url和dashboard的url
export function getUrls() {
    return request({
        baseURL:process.env.VUE_APP_EXPLORER_API,
        url: `/profile`,
        method: "get"
    });
}
export function fetchApiByCluster(url, token, data) {
    return request({ 
        baseURL:'',
        url: `/rest/sql`,
        method: "post",
        headers: {
            Authorization: token,
            "Content-Type":"text/plain"
        },
        data
    });
}

