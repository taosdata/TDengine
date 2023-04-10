import { request } from "@/utils/request";
import { jsonToObj } from "@/utils";
import store from "@/store";
import { Message } from "element-ui";
export function getClusterListReq() {
  return new Promise((resolve, reject) => {
    request({
      url: "/app/list",
      method: "get",
    })
      .then(res => {
        resolve(res.content);
      })
      .catch(res => {
        reject(res);
      });
  });
}

// 获取集群拉取状态
export function getClusterStatus(appId) {
  return request({
    url: "/monitor/cluster/status/" + appId,
  });
}

// 获取时区信息
export function getTimeZone() {
  return request({
    url: `/data/${store.getters.appId}/time-zone`,
  });
}

// 启动集群
export function startCluster(data) {
  const { regionId, id } = data;
  return request({
    url: `/region/${regionId}/cluster/start/${id}`,
    method: "put",
    data,
  });
}

// 停止集群
export function stopCluster(data) {
  const { regionId, id } = data;
  return request({
    url: `/region/${regionId}/cluster/suspend/${id}`,
    method: "put",
    data,
  });
}

// 创建集群
export function createCluster(data) {
  let { regionId } = data;
  return request({
    url: `/region/${regionId}/app`,
    method: "post",
    data,
  });
}

// 修改集群别名
export function changeCluster(data) {
  const { regionId } = data;
  return request({
    url: `/region/${regionId}/app/alias`,
    method: "post",
    data,
  });
}

// 重置token
export function resetToken(cluster) {
  const { region_id } = cluster;
  return request({
    url: `/region/${region_id}/apptoken/reset/${cluster.token.id}`,
    method: "put",
  });
}

// 获取计费方案
export function getPlan(params) {
  return request({
    url: "/billing/PricePlan/list",
    params,
  });
}

// 创建订单
export function createOrder(data) {
  return request({
    url: "/billing/UserOrder/create/plan",
    method: "post",
    data,
  });
}

// dataIn的csv文件上传
export function uploadCsv(data) {
  let { appId, dbName, tbName } = data;
  let csvData = new FormData();
  csvData.append("data", data.data);
  return request({
    url: `/rest/upload?db=${dbName}&table=${tbName}`,
    method: "post",
    data: csvData,
    headers: {
      "Content-Type": "multipart/form-data",
    },
  })
    .then(data => {
      
      // const currentData = jsonToObj(data);
      const currentData=data
      
      if (currentData.code != 0) return Promise.reject(currentData);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}
