import { request } from "@/utils/request";
import store from "@/store";
import { compHeadAndData, getLocalTimezone } from "@/utils";
export function sendSQLReq(sqlStr, composeData = false, appId = store.getters.appId) {
  return request({
    baseURL:'http://192.168.0.201:6060',
    url: `/rest/sql?tz=${getLocalTimezone()}`,
    method: 'post',
    headers: {
      "Content-Type": "text/plain"
    },
    data: sqlStr
  }).then(data => {
    let cData = JSON.parse(JSON.stringify(data))
    if (cData.code == 0) return composeData ? compHeadAndData(cData.column_meta, cData.data) : cData;
    return Promise.reject(cData?.desc ? cData : { desc: data || "Service Unavailable, please try again later!" });
  }).catch(err => {
    return Promise.reject(err);
  });
}

export function executeDBOperations(sql, appId = store.getters.appId) {
  return request({
    baseURL:'http://192.168.0.201:6060',
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
export async function getPaginationData(countSql, dataSql, currentPage, pageSize, handleDataFn, appId = store.getters.appId) {
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
  let data = await sendSQLReq(`${dataSql} limit ${startIndex},${pageSize};`, true, appId).catch(err => {
   
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
    baseURL:'http://192.168.0.201:6060',
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


// 获取个人收藏列表
export function getFavorites(sql) {
  return request({
    baseURL:'http://192.168.0.201:6060',
    url: '/rest/sql',
    method: 'post',
    headers: {
      "Content-Type": "text/plain"
    },
    data: sql
  });
}

// 添加个人收藏
export function addFavorite(sql) {
  return request({
    url: "/data/favorite",
    method: "post",
    data: {
      app_id: store.getters.appId,
      sql,
    },
  });
}

// 删除个人收藏
export function delFavorite(id) {
  return request({
    url: "/data/favorite/" + id,
    method: "delete",
  });
}

// 获取共享收藏列表
export function getSharedFavorites(sql) {
  return request({
    baseURL:'http://192.168.0.201:6060',
    url: '/rest/sql',
    headers: {
      "Content-Type": "text/plain"
    },
    method: 'post',
    data: sql
  });
}

// 添加共享收藏
export function addSharedFavorite(sql) {
  return request({
    url: "/data/shared_favorite",
    method: "post",
    data: {
      app_id: store.getters.appId,
      sql,
    },
  });
}

// 删除共享收藏
export function delSharedFavorite(id) {
  return request({
    url: "/data/shared_favorite/" + id,
    method: "delete",
  });
}
