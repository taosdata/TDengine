import { request } from "@/utils/request";
import { executeSQLByToken } from "@/api/gateway/console";
import { HIDEDB } from "@/const";
export function getTaskList(appId) {
  return request({
    url: `/replication/${appId}/tasks`,
  });
}

export function createTask(data, appId) {
  return request({
    url: `/replication/${appId}/task/create`,
    method: "post",
    data,
  });
}

export function startTask(appId, taskId) {
  return request({
    url: `/replication/${appId}/task/start/${taskId}`,
    method: "post",
  });
}

// 停止任务
export function stopTask(appId, taskId) {
  return request({
    url: `/replication/${appId}/task/stop/${taskId}`,
    method: "post",
  });
}

export function deleteTask(appId, taskId) {
  return request({
    url: `/replication/${appId}/task/${taskId}`,
    method: "delete",
  });
}

// 获取目标库列表
export function getTargetDBList(token) {
  return executeSQLByToken("show databases;", token).then(data => data.filter(item => !HIDEDB.includes(item.name)));
}
