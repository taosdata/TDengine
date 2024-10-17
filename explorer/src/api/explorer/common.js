import { request } from "@/utils/request";

//执行开始方法
export function excuteStart(id) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}/start`,
        method: "post",
        headers: {
            "Content-Type": "application/json",
        },
    });
}

//执行停止方法
export function excuteStop(id) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}/stop`,
        method: "post",
        headers: {
            "Content-Type": "application/json",
        },
    });
}

export function excuteDel(id) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}`,
        method: "delete"
    });
}