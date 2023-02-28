import { request } from "@/utils/request";

//获取cluster的url和dashboard的url
export function getUrls() {
    return request({
        url: `/profile`,
        method: "get",
        headers: {
            myHeader: process.env.VUE_APP_EXPLORER_API
        }
    });
}