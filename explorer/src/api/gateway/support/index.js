import { request } from "@/utils/request";

export function getIssueListReq(params) {
  return new Promise((resolve, reject) => {
    request({
      url: "/issues",
      method: "get",
      params,
    })
      .then(res => {
        let data = res.content;
        let total = parseInt(res.total);
        resolve({
          total: total,
          data: data,
        });
      })
      .catch(err => {
        reject(err);
      });
  });
}

export function getIssueTypeListReq() {
  return request({
    url: "/dict/issuetype",
    method: "get",
  });
}

export function createNewIssueReq(data) {
  return request({
    url: "/issues",
    method: "post",
    data,
  });
}

// 查询指定工单
export function queryIssue(id) {
  return request({
    url: "/issues/" + id,
  });
}

// 查看附件
export function viewFile(id) {
  return request({
    url: "/files/" + id,
    responseType: "blob",
  });
}

// 获取工单的评论
export function queryComments(id) {
  return request({
    url: "/issues/" + id + "/comments",
  }).then(res => {
    return res.content || [];
  });
}

// 创建工单评论
export function createComment(data, id) {
  return request({
    url: "/issues/" + id + "/comments",
    method: "post",
    data,
  });
}

// 上传附件
export function uploadFile(data) {
  return request({
    url: "/files",
    method: "post",
    data,
    headers: {
      "Content-Type": "multipart/form-data",
    },
  });
}
