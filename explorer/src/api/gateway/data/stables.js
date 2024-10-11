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
  let { selected_db, stableName, type } = payload;
  return sendSQLReq(`DESCRIBE  \`${selected_db}\`` +'.'+`\`${stableName}\`;`, true)
    .then(list => {
      let ts_field_name = list[0]?.field;
      let encode = list[0]?.encode;
      let compress = list[0]?.compress;
      let level = list[0]?.level;
      let columns = [];
      let tags = [];
      for (let i = type ? 0 : 1; i < list.length; i++) {
        const item = list[i];
        if (item.note == "TAG") {
          tags.push({ 
            ...item, 
            type: handleBinaryType(item.type, item.length), 
            type_old: handleBinaryType(item.type, item.length),
            length_old: item.length,
            field_old: item.field, 
            encode_old: item.encode, 
            compress_old: item.compress,
            level_old: item.level,
            value: "" 
          });
        } else {
          columns.push({ 
            ...item, 
            primaryKey: item.note == 'PRIMARY KEY', 
            type: handleBinaryType(item.type, item.length), 
            type_old: handleBinaryType(item.type, item.length),
            length_old: item.length,
            field: item.field, 
            encode_old: item.encode, 
            compress_old: item.compress,
            level_old: item.level,
            value: "" 
          });
        }
      }
      return {
        ts_field_name: ts_field_name,
        encode,
        compress,
        level,
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
  let { name, columns, tags, ts_field_name, rollup } = stable_form;
  let rollupValue = "";
  if (rollup.length) {
    rollupValue = `rollup (${rollup})`;
  }
  return sendSQLReq(
    `CREATE STABLE \`${selected_db}\`.${name} (${columns
      .map(item => `${item.field} ${VariableTableColumnType.includes(item.type) ? item.type+'('+`${item.length}`+')'
      :item.type}${item.encode ? ' ENCODE ' + `'${item.encode}'` : ''}
      ${item.compress ? ' COMPRESS ' + `'${item.compress}'` : ''}${item.level ? ' LEVEL ' + `'${item.level}'` : ''}
      ${item.primaryKey ? ' PRIMARY KEY': ''}`)
      .join(",")}) TAGS (${tags.map(item => `${item.field} ${VariableTableColumnType.includes(item.type) ? item.type+'('+`${item.length}`+')':
      item.type}`).join(",")}) ${rollupValue};`
  ).catch(err => {
    return Promise.reject(err);
  });
}

// TODO 待定修改
// 修改超级表的表结构
// 一次只能改类型长度或者修改压缩方法
export function changeStableStruct(data, stableName) {
  let { operation, first_field = "", second_field = "" } = data;
  let sql = "";
  sql = `ALTER STABLE  ${stableName} ${operation} ${first_field} ${second_field};`;
  return sendSQLReq(sql).catch(err => {
    return Promise.reject(err);
  });
}

export function changeStableStructOther(data, stableName) {
  let { operation, first_field = "", second_field = "", encode = "", compress = "", level = "", isVariable } = data;
  let sql = "";
  if (operation.startsWith('add')) {
    sql = `ALTER STABLE  ${stableName} ${operation} ${first_field} ${second_field} ${encode ? ' ENCODE ' + `'${encode}'` : ''}${compress ? ' COMPRESS ' + `'${compress}'` : ''}${level ? ' LEVEL ' + `'${level}'` : ''};`;
  } else {
    sql = `ALTER STABLE  ${stableName} ${operation} ${first_field} ${encode ? ' ENCODE ' + `'${encode}'` : ''}${compress ? ' COMPRESS ' + `'${compress}'` : ''}${level ? ' LEVEL ' + `'${level}'` : ''};`;
  }
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
