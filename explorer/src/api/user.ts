import { request } from "@/utils/request.ts";

// 获取用户列表
export function getUserList(params) {
  return request({
    url: "/user/invitees",
    params,
  });
}



