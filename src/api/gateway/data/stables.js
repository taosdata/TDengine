import { handleDataKey } from "./dbs";
import { sendSQLReq, getPaginationData } from "@/api/gateway/console";
import { Message } from "element-ui";
import { VariableTableColumnType } from "@/const";
//获取数据库下所有普通表
export function getAllNormalTables(params,dbName){
  let { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_tables where db_name='${dbName}'  `;
  const dataSql = `select * from information_schema.ins_tables where db_name='${dbName}'  and stable_name is  null`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize, data => handleDataKey(data, "table", dbName));
}
export function getStableListReq(params, dbName) {
  let { currentPage, pageSize } = params;
  const countSql = `select count(*) from information_schema.ins_stables where db_name='${dbName}'  `;
  const dataSql = `select * from information_schema.ins_stables where db_name='${dbName}' `;
  // const normalTableCount=`select count(*) from information_schema.ins_tables where db_name='${dbName}';`
  // const normalTableSql=`select * from information_schema.ins_tables where db_name='${dbName}'`
  return getPaginationData(countSql, dataSql, currentPage, pageSize, data => handleDataKey(data, "stable", dbName));
}

export function deleteStableReq(payload) {
  let { selected_db, stableName } = payload;
  return sendSQLReq(`DROP STABLE \`${selected_db}\``+'.'+`\`${stableName}\`;`).catch(err => {
    
    return Promise.reject(err);
  });
}

export function getStableStructReq(payload) {
  let { selected_db, stableName } = payload;
  return sendSQLReq(`DESCRIBE  \`${selected_db}\`` +'.'+`\`${stableName}\`;`, true)
    .then(list => {
      let ts_field_name = list[0]?.field;
      let columns = [];
      let tags = [];
      for (let i = 1; i < list.length; i++) {
        const item = list[i];
        if (item.note == "TAG") {
          tags.push({ type: handleBinaryType(item.type, item.length), field: item.field, value: "" });
        } else {
          columns.push({ type: handleBinaryType(item.type, item.length), field: item.field, value: "" });
        }
      }
      return {
        ts_field_name: ts_field_name,
        columns: columns,
        tags: tags,
      };
    })
    .catch(err => {
      return Promise.reject(err);
    });
}

//处理超级表的字段的binary类型
export function handleBinaryType(type, length) {
  type = type?.toUpperCase();
  return VariableTableColumnType.includes(type) ? `${type}(${length})` : type;
}

export function createStableReq(payload) {
  let { selected_db, stable_form } = payload;
  let { name, columns, tags, ts_field_name, rollup,varcharLength=8,ncharLength=8 } = stable_form;
  let rollupValue = "";
  if (rollup.length) {
    rollupValue = `rollup (${rollup})`;
  }
  return sendSQLReq(
    `CREATE STABLE \`${selected_db}\`.\`${name}\` (\`${ts_field_name}\` TIMESTAMP,${columns
      .map(item => `\`${item.field}\` ${item.type==='VARCHAR'?'VARCHAR('+`${item.varcharLength}`+')':item.type==='NCHAR'?
      'NCHAR('+`${item.ncharLength}`+')':item.type}`)
      .join(",")}) TAGS (${tags.map(item => `\`${item.field}\` ${item.type==='VARCHAR'?'VARCHAR('+`${item.varcharLength}`+')':item.type==='NCHAR'?
      'NCHAR('+`${item.ncharLength}`+')':item.type}`).join(",")}) ${rollupValue};`
  ).catch(err => {
    return Promise.reject(err);
  });
}

// TODO 待定修改
// 修改超级表的表结构
export function changeStableStruct(data, stableName) {
  let { operation, first_field = "", second_field = "" } = data;
  let sql = "";
  sql = `ALTER STABLE  ${stableName} ${operation} \`${first_field}\` ${second_field};`;
  return sendSQLReq(sql).catch(err => {
    return Promise.reject(err);
  });
}

//根据云服务添加的topic-wizard
export function searchStable(prefix, dbname) {
  return sendSQLReq(
    `select * from information_schema.ins_stables where db_name='${dbname}' and  stable_name LIKE '%${prefix}%' limit 100`,
    true
  );
}

// 判断超级表是否已经存在
export function isStableExist(stbName, dbName) {
  return sendSQLReq(
    `select * from information_schema.ins_stables where db_name='${dbName.toLowerCase()}' and  stable_name='${stbName.toLowerCase()}'`
  ).then(res => res.rows);
}
