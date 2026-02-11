import { request } from "@/utils/request.ts";
import { getLocalLang } from "@/utils";
import pathDetector from '@/utils/pathDetector';

export function getTaskList(appId: string) {
    return request({
        url: `/replication/${appId}/tasks`,
    });
}

//获取replication列表
export function getReplicationList() {
    const language = getLocalLang()
    return request({
        baseURL: pathDetector.getXApiBasePath(),
        url: `/tasks?lang=${language}&detail=true&labels=type::replication`,
        method: "get"
    });
}

export function addReplicationData(data: Recordable) {
    const language = getLocalLang()
    return request({
        baseURL: pathDetector.getXApiBasePath(),
        url: `/tasks?lang=${language}&detail=true&labels=type::replication`,
        method: "post",
        headers: {
            "Content-Type": "application/json",
        },
        data
    });
}



