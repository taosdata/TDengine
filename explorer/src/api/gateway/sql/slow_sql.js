import { request } from "@/utils/request";

export function getSlowSqlListReq(params) {
  return new Promise((resolve, reject) => {
    request({
      url: "/data/slow-sql",
      params,
    })
      .then(res => {
        let data = res.content;
        let total = parseInt(res.total);
        resolve({
          total,
          data,
        });
      })
      .catch(err => {
        reject(err);
      });
  });
}
