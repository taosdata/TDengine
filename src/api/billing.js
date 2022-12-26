import { request } from "utils/request";

// 获取用量
export function getUsage(params) {
  return request({
    url: "/billing/usage/page",
    params,
  });
}

// 收据查询
export function getReceipt(params) {
  return request({
    url: "/billing/payment/receipt-page",
    params,
  });
}

// 获取计费看板信息
export function getBillingOverview(params) {
  return request({
    url: "/billing/payment/overview",
    params,
  });
}

// 获取支付方式信息
export function getPaymentMethod() {
  return request({
    url: "/billing/PaymentMethod/info",
  });
}

// 新增支付方式
export function addPaymentMethod(data) {
  return request({
    url: "/billing/PaymentMethod/create",
    method: "post",
    data,
  });
}

// 修改支付方式
export function updatePaymentMethod(data) {
  return request({
    url: "/billing/PaymentMethod/update",
    method: "post",
    data,
  });
}

// 获取支付通知邮箱
export function getPaymentEmail() {
  return request({
    url: "/billing/UserBillAccount/email/list",
  });
}

// 新增支付通知邮箱
export function addPaymentEmail(data) {
  return request({
    url: "/billing/UserBillAccount/email",
    method: "post",
    data,
  });
}

// 删除支付通知邮箱
export function deletePaymentEmail(id) {
  return request({
    url: "/billing/UserBillAccount/email/remove/" + id,
    method: "delete",
  });
}

// 获取收据地址
export function getReceiptUrl(params) {
  return request({
    url: "/billing/payment/external-receipt",
    params,
  });
}
