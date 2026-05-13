
import { getPaginationData } from "@/api/explorer";
import { request } from "@/utils/request";

export function getSlowSqlLogs(params) {
  const { currentPage, pageSize, conditions, deDuplication, sortBy } = params;
  let dataSql = `
    SELECT
      ${deDuplication ? 'LAST_ROW(start_ts) as start_ts,' : 'start_ts,'}
      db, 
      ip, 
      \`user\`, 
      sql, 
      query_time, 
      rows_num 
      FROM log.taos_slow_sql_detail 
      ${conditions ? 'WHERE' + conditions : ''}
      ${deDuplication ? 'PARTITION by sql,db' : ''}
      ORDER BY ${sortBy ? `query_time ${sortBy}` : 'start_ts DESC'}
      `
  // 去除整行的空格
  dataSql = dataSql.replace(/^\s*$(?:\r\n?|\n)/gm, '')
  const countSql = `select count(*) from (${dataSql})`
  return getPaginationData(countSql, dataSql, currentPage, pageSize);
}

export function getSlowSqlStatistics(params) {
  const { currentPage, pageSize, conditions, orderSql } = params;
  const slimit = orderSql ? false : true;
  let dataSql = `
    SELECT 
      sql, 
      db, 
      count(*) as query_count, 
      cast(avg(query_time) as int) as avg_query_time,
      max(query_time) as max_query_time,
      cast(avg(rows_num) as int) as avg_rows_num, 
      max(rows_num) as max_rows_num 
    from log.taos_slow_sql_detail 
    ${conditions ? 'WHERE' + conditions : ''}
    PARTITION by sql, db
    ${orderSql}
    `;
  dataSql = dataSql.replace(/^\s*$(?:\r\n?|\n)/gm, '');
  const countSql = `select count(*) from (${dataSql});`;

  return getPaginationData(countSql, dataSql, currentPage, pageSize, null, null, slimit);
}

export function getSlowSqlListReq(params) {
  return new Promise((resolve, reject) => {
    request({
      url: "/data/slow-sql",
      params,
    })
      .then(res => {
        const data = res.content;
        const total = parseInt(res.total);
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