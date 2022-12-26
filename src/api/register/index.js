import { request } from "@/utils/request";

export function getCountryList(params) {
  return request({
    url: "/dict/country",
    params,
  });
}

export function getProfessionList() {
  return request({
    url: "/dict/industry",
  });
}

export function getPositionList() {
  return request({
    url: "/dict/position",
  });
}

export function getCloudRegion() {
  return request({
    url: "/dict/cloud",
  });
}

//完善用户信息
export function putUserInfo(data) {
  return request({
    url: "/user",
    method: "put",
    data,
  });
}
