import { request } from "@/utils/request";

// 邀请用户
export function inviteUser(data) {
  return request({
    url: "/user/invite",
    method: "post",
    data,
  });
}

// 删除用户
export function delUser(id) {
  return request({
    url: "/user/standard/delete/" + id,
    method: "delete",
  });
}

// 获取用户列表
export function getUserList(params) {
  return request({
    url: "/user/invitees",
    params,
  });
}

// 添加用户权限
export function addUserPermission(data) {
  return request({
    url: "/user/invitees/privilege",
    method: "post",
    data,
  });
}

// 修改用户权限
export function updateUserPermission(data) {
  return request({
    url: "/user/invitees/privilege",
    method: "put",
    data,
  });
}

// 删除user权限
export function deleteUserPermission(params) {
  return request({
    url: "/user/invitees/privilege",
    method: "delete",
    params,
    paramsSerializer(params) {
      return params.id.map(item => `id=${item}`).join("&");
    },
  });
}

// 获取用户权限
export function getUserPermission(id) {
  return request({
    url: "/user/invitee",
    params: { invitee_id: id },
  });
}

// 用户启用
export function enableUser(id) {
  return request({
    url: "/user/standard/enable/" + id,
    method: "put",
  });
}

// 用户禁用
export function disableUser(id) {
  return request({
    url: "/user/standard/disable/" + id,
    method: "put",
  });
}



