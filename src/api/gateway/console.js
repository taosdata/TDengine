import { request } from "@/utils/request";
import { jsonToObj } from "@/utils";
import store from "@/store";
import { compHeadAndData } from "@/utils";
import { Message } from "element-ui";
export function sendSQLReq(sqlStr, composeData = false, appId = store.getters.appId) {
  return request({
    url: "/data/sql/" + appId,
    method: "post",
    timeout: 120000,
    data: { sql: sqlStr },
  }).then(data => {
    let cData = jsonToObj(data);
    if (cData.code == 0) return composeData ? compHeadAndData(cData.column_meta, cData.data) : cData;
    return Promise.reject(cData?.desc ? cData : { desc: data || "Service Unavailable, please try again later!" });
  });
}

export function executeDBOperations(sql, appId = store.getters.appId) {
  return request({
    url: `/private/data/sql/${appId}`,
    method: "post",
    data: {
      sql,
    },
  })
    .then(data => {
      data = jsonToObj(data);
      if (data.code == 0) return compHeadAndData(data.column_meta, data.data);
      return Promise.reject(data);
    })
    .catch(err => {
      Message.error(err.desc);
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
      Message.error(err.desc);
      return 0;
    });
  if (!count || !currentPage || !pageSize) return [[], 0];
  const startIndex = (currentPage - 1) * pageSize;
  // 查询数据
  if (dataSql.endsWith(";")) {
    dataSql = dataSql.slice(0, -1);
  }
  let data = await sendSQLReq(`${dataSql} limit ${startIndex},${pageSize};`, true, appId).catch(err => {
    Message.error(err.desc);
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
    url: `/data/sql/token/${token}`,
    method: "post",
    data: {
      sql,
    },
  })
    .then(data => {
      data = jsonToObj(data);
      if (data.code == 0) return compHeadAndData(data.column_meta, data.data);
      return Promise.reject(data);
    })
    .catch(err => {
      err.desc && Message.error(err.desc);
      return Promise.reject(err);
    });
}


// 获取个人收藏列表
export function getFavorites() {
  return request({
    url: "/data/favorite",
    params: { app_id: store.getters.appId },
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
export function getSharedFavorites() {
  return request({
    url: "/data/shared_favorite",
    params: { app_id: store.getters.appId },
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
