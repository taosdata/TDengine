import { request } from "@/utils/request.ts";
import { getLocalLang } from "@/utils";
import pathDetector from '@/utils/pathDetector';

export function getTaskList(appId: string) {
    return request({
        url: `/replication/${appId}/tasks`,
    });
}

//获取replication列表
export function getReplicationList(id: string | number) {
    const language = getLocalLang()
    return request({
        baseURL: pathDetector.getXApiBasePath(),
        url: `/tasks?lang=${language}&detail=true&labels=type::replication,cluster-id::${id}`,
        method: "get"
    });
}

export function addReplicationData(id: string | number, data: Recordable) {
    const language = getLocalLang()
    return request({
        baseURL: pathDetector.getXApiBasePath(),
        url: `/tasks?lang=${language}&detail=true&labels=type::replication,cluster-id::${id}`,
        method: "post",
        headers: {
            "Content-Type": "application/json",
        },
        data
    });
}



