import { handleDataKey } from "./dbs";
import { sendSQLReq, getPaginationData } from "@/api/gateway/console";
import { Message } from "element-ui";
import { handleBinaryType } from "./stables";
import { VariableTableColumnType } from "@/const";
// 获取超级表下属子表
export async function getTableListReq(params) {
  let { currentPage, pageSize, selected_stb, selected_db } = params;
  const parent = `${selected_db}.${selected_stb}`;
  const where = `where db_name='${selected_db}' and stable_name='${selected_stb}'`;
  const countSql = `select count(*) from information_schema.ins_tables ${where}`;
  const dataSql = `select * from information_schema.ins_tables ${where}`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize, data => handleDataKey(data, "table", parent));
}

export function searchTable(prefix, dbname) {
  // return sendSQLReq(
  //   `select * from information_schema.ins_tables where db_name='${dbname}' and stable_name is NOT NULL and table_name LIKE '${prefix}%' limit 100`,
  //   true
  // );
  return sendSQLReq(
    `select * from information_schema.ins_tables where db_name='${dbname}'  and table_name LIKE '${prefix}%' limit 100`,
    true
  );
}

export function deleteTableReq(payload) {
  let { selected_db, tableName } = payload;
  return sendSQLReq(`DROP TABLE \`${selected_db}\``+'.'+`\`${tableName}\`;`).catch(err => {
   
    return Promise.reject(err);
  });
}

export function createTableReq(payload) {
  let { selected_db, table_form } = payload;
  let { name, stbTmpl, tags,columns } = table_form;
  // 以超级表为模版创建表
  if (tags && tags.length > 0) { //创建超级表的子表
    return sendSQLReq(
      `CREATE TABLE \`${selected_db}\``+'.'+`\`${name}\` USING \`${selected_db}\``+'.'+`\`${stbTmpl}\` (${tags.map(item => `\`${item.field}\``).join(",")}) TAGS (${tags
        .map(item => handleStringTagValue(item))
        .join(",")});`
    ).catch(err => {
      return Promise.reject(err);
    });
  } else {
    return sendSQLReq(`CREATE TABLE \`${selected_db}\``+'.'+`\`${name}\` (${columns.map(item => `\`${item.field}\` ${item.type==='VARCHAR'?'VARCHAR('+`${item.varcharLength}`+')':item.type==='NCHAR'?
    'NCHAR('+`${item.ncharLength}`+')':item.type}`).join(",")});`).catch(err => {
      
      return Promise.reject(err);
    });
  }


}

// 修改表结构
export function changeTableStruct(data, tableName) {
  let { operation, first_field = "", second_field = "" } = data;
  let sql = "";
  sql = `ALTER TABLE   ${tableName} ${operation} ${first_field} ${second_field};`;
  return sendSQLReq(sql).catch(err => {
    return Promise.reject(err);
  });
}

// 获取表的tag value
export function getTagValue(tags, database, stable_name, table_name) {
  if (!tags.length) return Promise.resolve({});
 let sql=`SELECT DISTINCT tbname,${tags.map(item => `\`${item.field}\``).join(",")} from \`${database}\``;
 if(stable_name){
  sql+=`.`+`\`${stable_name}\``
 }
 sql+=`  where tbname='${table_name}';`
  return sendSQLReq(
     sql
  )
    .then(data => {
      //  = data.data?.[0] || {};
       let result=data.data?.[0]? data.data.map((db) => {
        return Object.fromEntries(
          data.column_meta.map((item, index) => {
            return [item[0], db[index]];
          })
        );
      }):{};
      // Object.keys(result).forEach(key => {
      //   result[key] = result[key] + "";
      // });
      return result;
    })
    .catch(err => {
      return {};
    });
}

export function getMatrixStructReq(payload) {
  let { selected_db, selected_tb } = payload;
  return sendSQLReq(`DESCRIBE \`${selected_db}\``+'.'+`\`${selected_tb}\`;`, true)
    .then(res => handleColumnData(res, "tag"))
    .catch(() => []);
}
export function handleColumnData(data) {
  let res = [];
  data.map(item => {
    let result = {};
    result.name = item.field;
    result.field = item.field;
    // 此处不展示标签，在表格详细信息中进行展示
    if (item.note) {
      result.typeName = "tag";
    } else {
      result.typeName = "column";
    }
    result.type = handleBinaryType(item.type, item.length);
    result.dataType = handleBinaryType(item.type, item.length);
    result["node-key"] = result.name + result.dataType;
    result.leaf = true;
    res.push(result);
  });
  return res;
}

function handleStringTagValue(tag) {
  if (VariableTableColumnType.some(item => tag?.type?.startsWith(item))) {
    return `'${tag.value}'`;
  } else {
    return tag.value;
  }
}


export function getTableStructReq(payload) {
  let { selected_db, tableName } = payload;
  return sendSQLReq(`DESCRIBE \`${selected_db}\``+'.'+`\`${tableName}\`;`, true)
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
