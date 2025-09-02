import { request } from '@/utils/request.ts';
import JSONbig from 'json-bigint';
import { compHeadAndData, getLocalTimezone } from '@/utils';
import { stringify } from 'qs';
export function sendSQLReq(sqlStr: string, composeData = false, alert = true) {
  return request({
    baseURL: import.meta.env.VITE_APP_BASE_URL,
    url: `/api/-/rest/sql?tz=${getLocalTimezone()}`,
    method: 'post',
    headers: {
      'Content-Type': 'text/plain'
    },
    transformResponse: [
      function (data, headers) {
        console.log(headers);
        try {
          console.log('Got response', data);
          return JSONbig.parse(data);
        } catch (error) {
          return data;
        }
      }
    ],
    data: sqlStr
  })
    .then(cData => {
      if (cData.code == 0) return composeData ? compHeadAndData(cData.column_meta, cData.data) : cData;
      if (alert) {
        ElMessage.error(cData.desc);
      }
      return Promise.reject(cData?.desc || 'Service Unavailable, please try again later!');
    })
    .catch(err => {
      return Promise.reject(err);
    });
}

export function executeDBOperations(sql: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_BASE_URL,
    url: `/api/-/rest/sql?tz=${getLocalTimezone()}`,
    method: 'post',
    headers: {
      'Content-Type': 'text/plain'
    },
    data: sql
  })
    .then(data => {
      data = JSON.parse(JSON.stringify(data));
      if (data.code == 0) return compHeadAndData(data.column_meta, data.data);
      return Promise.reject(data);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}
export async function getPaginationData(
  countSql: string,
  dataSql: string,
  currentPage,
  pageSize,
  handleDataFn,
  slimit
) {
  // 查询数量
  const count = await sendSQLReq(countSql, false)
    .then(({ data }) => {
      return data?.[0]?.[0] || 0;
    })
    .catch(() => {
      return 0;
    });
  if (!count || !currentPage || !pageSize) return [[], 0];
  const startIndex = (currentPage - 1) * pageSize;
  // 查询数据
  if (dataSql.endsWith(';')) {
    dataSql = dataSql.slice(0, -1);
  }
  let data = await sendSQLReq(`${dataSql} ${slimit ? 'slimit' : 'limit'} ${startIndex},${pageSize};`, true).catch(
    () => {
      return [];
    }
  );
  if (typeof handleDataFn === 'function') {
    data = handleDataFn(data);
  }
  return [data, count];
}

// 通过token执行sql
export function executeSQLByToken(sql: string, token: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_BASE_URL,
    url: `/api/-/rest/sql/token/${token}`,
    method: 'post',
    headers: {
      'Content-Type': 'text/plain'
    },
    data: {
      sql
    }
  })
    .then(data => {
      // data = jsonToObj(data);
      data = JSON.parse(JSON.stringify(data));
      if (data.code == 0) return compHeadAndData(data.column_meta, data.data);
      return Promise.reject(data);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}

// 获取个人/共享收藏列表
export function getFavorites(data: Recordable) {
  return request({
    baseURL: import.meta.env.VITE_APP_EXPLORER_API,
    url: `/favorites/sql?${stringify(data)}`,
    method: 'get',
    headers: {
      'Content-Type': 'text/plain'
    }
  });
}

// 添加个人/共享收藏
export function addFavorite(data: Recordable) {
  return request({
    baseURL: import.meta.env.VITE_APP_EXPLORER_API,
    url: '/favorites/sql',
    method: 'post',
    data
  });
}

// 删除个人/共享收藏
export function delFavorite(id: string | number) {
  return request({
    baseURL: import.meta.env.VITE_APP_EXPLORER_API,
    url: '/favorites/sql/' + id,
    method: 'delete'
  });
}

// 添加/取消/共享收藏 修改描述
export function manageFavorite(id: string | number, data: Recordable) {
  return request({
    baseURL: import.meta.env.VITE_APP_EXPLORER_API,
    url: '/favorites/sql/' + id,
    method: 'patch',
    data
  });
}
