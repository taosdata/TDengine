import { request } from "@/utils/request";

export function getActivityListReq(params) {
  return new Promise((resolve, reject) => {
    request({
      url: "/monitor/log",
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
