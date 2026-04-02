import { request } from "@/utils/request.ts";

// dataIn的csv文件上传
export function uploadCsv(data) {
  const { appId, dbName, tbName } = data;
  const csvData = new FormData();
  csvData.append("data", data.data);
  return request({
    url: `/rest/upload?db=${dbName}&table=${tbName}`,
    method: "post",
    data: csvData,
    headers: {
      "Content-Type": "multipart/form-data",
    },
  })
    .then((data) => {
      // const currentData = jsonToObj(data);
      const currentData = data;

      if (currentData.code != 0) return Promise.reject(currentData);
    })
    .catch((err) => {
      return Promise.reject(err);
    });
}
