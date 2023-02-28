import { request } from "@/utils/request";

//执行开始方法
export function excuteStart(id) {
    return request({
        url: `/tasks/${id}/start`,
        method: "post"
    });
}

//执行停止方法
export function excuteStop(id) {
    return request({
        url: `/tasks/${id}/stop`,
        method: "post"
    });
}