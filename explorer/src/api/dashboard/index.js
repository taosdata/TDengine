import { request } from "@/utils/request";
import store from "@/store";
// 获取集群信息
export function getAppDetail() {
  return request({
    url: "/app/detail",
    params: { app_id: store.state.app.current_cluster.id },
  });
}
// 获取当前磁盘使用情况
export function getDiskUsage(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/disk-used",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}
export function getStorage(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/disk-used/range",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

// 获取cpu
export function getCpu(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/cpu/range",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

// 内存使用
export function getMemory(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/mem/range",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

//获取网络
export function getNet(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/net/range",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

// 获取插入响应时间
export function getInserRes(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/response-time-insert",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

// 获取查询响应时间
export function getQueryRes(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/response-time-query",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

// 获取用户集群信息
export function getClusterInfo(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/user-cluster",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

// 获取io信息
export function getIO(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/io/range",
    params: {
      app_id: id,
      container_id: store.state.app.current_cluster.name,
      ...params,
    },
  });
}

// qps插入
export function getQPSInsert(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/qps-insert",
    params: {
      app_id: id,
      ...params,
    },
  });
}

// qps查询
export function getQPSQuery(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/qps-query",
    params: {
      app_id: id,
      ...params,
    },
  });
}

// 带宽
export function getBandWidth(params) {
  const id = store.state.app.current_cluster.id;
  return request({
    url: "/monitor/bandwidth/range",
    params: {
      app_id: id,
      ...params,
    },
  });
}

// 流入
export function getIngress(params) {
  return request({
    url: "/monitor/ingress/range",
    params: {
      app_id: store.state.app.current_cluster.id,
      ...params,
    },
  });
}

// 流出
export function getEgress(params) {
  return request({
    url: "/monitor/egress/range",
    params: {
      app_id: store.state.app.current_cluster.id,
      ...params,
    },
  });
}
