import { request } from "@/utils/request";
import store from "@/store";
import JSONbig from "json-bigint";
import { compHeadAndData, getLocalTimezone } from "@/utils";
import { stringify } from 'qs';
export function sendSQLReq(sqlStr, composeData = false, appId = store.getters.appId) {
  return request({
    baseURL: process.env.VUE_APP_BASE_URL,
    url: `/rest/sql?tz=${getLocalTimezone()}`,
    method: 'post',
    headers: {
      "Content-Type": "text/plain"
    },
    transformResponse: [function (data) {
      try {
        return JSONbig.parse(data);
      } catch (error) {
        return data;
      }
    }],
    data: sqlStr
  }).then(data => {
    let cData = JSON.parse(JSON.stringify(data))
    if (cData.code == 0) return composeData ? compHeadAndData(cData.column_meta, cData.data) : cData;
    return Promise.reject(cData?.desc ? cData : { desc: data || "Service Unavailable, please try again later!" });
  }).catch(err => {
    return Promise.reject(err);
  });
}

export function modifyUserPassword(username, sqlStr) {
  return request({
    baseURL: process.env.VUE_APP_BASE_URL,
    url: `/api/-/password/${username}?tz=${getLocalTimezone()}`,
    method: 'post',
    headers: {
      "Content-Type": "text/plain"
    },
    data: sqlStr
  });
}

export function executeDBOperations(sql, appId = store.getters.appId) {
  return request({
    baseURL: process.env.VUE_APP_BASE_URL,
    url: `/rest/sql?tz=${getLocalTimezone()}`,
    method: "post",
    headers: {
      "Content-Type": "text/plain"
    },
    data: sql
  })
    .then(data => {
      data = JSON.parse(JSON.stringify((data)));
      if (data.code == 0) return compHeadAndData(data.column_meta, data.data);
      return Promise.reject(data);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}
export async function getPaginationData(countSql, dataSql, currentPage, pageSize, handleDataFn, appId = store.getters.appId, slimit) {
  // 查询数量
  const count = await sendSQLReq(countSql, false, appId)
    .then(({ data }) => {
      return data?.[0]?.[0] || 0;
    })
    .catch(err => {
      return 0;
    });
  if (!count || !currentPage || !pageSize) return [[], 0];
  const startIndex = (currentPage - 1) * pageSize;
  // 查询数据
  if (dataSql.endsWith(";")) {
    dataSql = dataSql.slice(0, -1);
  }
  // if(type==='union'){//查询database下所有stable和所有table的
  //   dataSql= '( '+dataSql + `limit ${startIndex},${pageSize}`+ ')  union '+normalSql
  // }
  let data = await sendSQLReq(`${dataSql} ${slimit ? 'slimit': 'limit'} ${startIndex},${pageSize};`, true, appId).catch(err => {
   
    return [];
  });
  if (typeof handleDataFn === "function") {
    data = handleDataFn(data);
  }
  return [data, count];
}

// 通过token执行sql
export function executeSQLByToken(sql, token) {
  return request({
    baseURL: process.env.VUE_APP_BASE_URL,
    url: `/rest/sql/token/${token}`,
    method: "post",
    headers: {
      "Content-Type": "text/plain"
    },
    data: {
      sql,
    },
  })
    .then(data => {
      // data = jsonToObj(data);
      data = JSON.parse(JSON.stringify((data)));
      if (data.code == 0) return compHeadAndData(data.column_meta, data.data);
      return Promise.reject(data);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}


// 获取个人/共享收藏列表
export function getFavorites(data) {
  return request({
    baseURL: process.env.VUE_APP_EXPLORER_API,
    url: `/favorites/sql?${stringify(data)}`,
    method: 'get',
    headers: {
      "Content-Type": "text/plain"
    },
  });
}

// 添加个人/共享收藏
export function addFavorite(data) {
  return request({
    baseURL: process.env.VUE_APP_EXPLORER_API,
    url: "/favorites/sql",
    method: "post",
    data,
  });
}

// 删除个人/共享收藏
export function delFavorite(id) {
  return request({
    baseURL: process.env.VUE_APP_EXPLORER_API,
    url: "/favorites/sql/" + id,
    method: "delete",
  });
}

// 添加/取消/共享收藏 修改描述
export function manageFavorite(id, data) {
  return request({
    baseURL: process.env.VUE_APP_EXPLORER_API,
    url: "/favorites/sql/" + id,
    method: "patch",
    data
  });
}

