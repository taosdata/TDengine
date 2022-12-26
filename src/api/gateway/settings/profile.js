import { request } from "@/utils/request";

// 获取职业类型列表
export function getPositionListReq() {
  return request({
    url: "/dict/position",
    method: "get",
  });
}

// 获取行业类型列表
export function getIndustryListReq() {
  return request({
    url: "/dict/industry",
    method: "get",
  });
}

export function getCountryListReq() {
  return request({
    url: "/dict/country",
    method: "get",
  });
}
