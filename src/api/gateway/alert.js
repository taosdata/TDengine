import { request } from "@/utils/request";

// 获取通知列表
export function getAlertList(params, noRefreshToken = false) {
  return request({
    url: "/alert/list",
    params,
    noRefreshToken,
  });
}

// 修改通知状态
export function changeAlert(data) {
  return request({
    url: "/alert",
    method: "put",
    data,
  });
}

// 查询通知详情
export function getAlertDetail(params) {
  return request({
    url: "/alert",
    params,
  });
}
