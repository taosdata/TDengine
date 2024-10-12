
import { getPaginationData } from "@/api/gateway/console";

export function getDataConfig(lang) {
  if (lang && lang === "en") {
    return [
      {
        name: "slowLogScope",
        display: "slowLogScope",
        type: "select",
        choices: ["ALL", "QUERY", "INSERT", "OTHERS", "NONE"],
        description: "指定启动记录哪些类型的慢 sql \n",
        value: "QUERY",
        multiple: true,
      },
      {
        name: "slowLogThreshold",
        display: "slowLogThreshold",
        type: "number",
        description: "指定慢 sql 门限值，大于等于门限值认为是慢 sql int32_t，单位 s",
      },
      {
        name: "slowLogMaxLen",
        display: "slowLogMaxLen",
        type: "number",
        description: "指定记录 SQL 语句的最大长度 int32_t，单位 byte",
        value: 4096,
      },
      {
        name: "monitorInterval",
        display: "monitorInterval",
        type: "number",
        description: "监控数据上报间隔",
        value: 30,
      },
      {
        name: "monitor",
        display: "monitor",
        type: "switch",
        description: "是否打开监控开关",
        placeholder: "5",
      },
      {
        name: "monitorFqdn",
        display: "monitorFqdn",
        type: "input",
        description: "taoskeeper 的地址\n",
        value: "",
      },
      {
        name: "monitorPort",
        display: "monitorPort",
        type: "input",
        description: "taoskeeper 的端口\n",
        value: "",
      },
    ];
  } else {
    return [
      {
        name: "slowLogScope",
        display: "slowLogScope",
        type: "select",
        choices: ["ALL", "QUERY", "INSERT", "OTHERS", "NONE"],
        description: "指定启动记录哪些类型的慢 sql \n",
        value: "",
        multiple: true,
      },
      {
        name: "slowLogThreshold",
        display: "slowLogThreshold",
        type: "number",
        description:
          "指定慢 sql 门限值，大于等于门限值认为是慢 sql int32_t，单位 s",
      },
      {
        name: "slowLogMaxLen",
        display: "slowLogMaxLen",
        type: "number",
        description: "指定记录 SQL 语句的最大长度 int32_t，单位 byte",
        value: 4096,
      },
      {
        name: "monitorInterval",
        display: "monitorInterval",
        type: "number",
        description: "监控数据上报间隔",
        value: 30,
      },
      {
        name: "monitor",
        display: "monitor",
        type: "switch",
        description: "是否打开监控开关",
        placeholder: "5",
      },
      {
        name: "monitorFqdn",
        display: "monitorFqdn",
        type: "input",
        description: "taoskeeper 的地址\n",
        value: "",
      },
      {
        name: "monitorPort",
        display: "monitorPort",
        type: "input",
        description: "taoskeeper 的端口\n",
        value: "",
      },
    ];
  }
}

export function getSlowSqlLogs(params) {
  let { currentPage, pageSize, conditions, deDuplication, sortBy } = params;
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
  let { currentPage, pageSize, conditions, orderSql  } = params;
  const slimit = orderSql ? false: true;
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