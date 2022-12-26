import { request } from "@/utils/request.js";

// 登录
export function loginReq(data) {
  return request({
    url: "/auth/login",
    method: "post",
    data,
  });
}
// 注册
export function register(data) {
  return request({
    url: "/auth/register",
    method: "post",
    data,
  });
}

// 获取用户信息
export function getUserInfoReq() {
  return request({
    url: "/user",
  });
}
// 修改用户信息
export function putUserInfo(data, email) {
  return request({
    url: "/user/" + email,
    method: "put",
    data,
  });
}

// 忘记密码发送邮件
export function sendEmail(email) {
  return request({
    url: "/auth/recover",
    data: { email },
    method: "post",
  });
}

// 忘记密码，更新密码
export function updatePassword(data) {
  return request({
    url: "/auth/forget-password",
    method: "post",
    data,
  });
}

//重新发送邮件
export function resendEamil(email) {
  return request({
    url: "/user/reactivate",
    data: { email },
    method: "post",
  });
}

// 退出
export function logout() {
  return request({
    url: "/user/logout",
    method: "post",
  });
}

// 用户激活
export function activite(params) {
  return request({
    url: "/auth/activate",
    params,
  });
}

// 修改密码
export function change(data) {
  return request({
    url: "/user/reset",
    method: "put",
    data,
  });
}
