import { sendSQLReq } from "@/api/gateway/console";
import { Message } from "element-ui";
import { DBFILED, HIDEDB } from "@/const";
import { request } from "@/utils/request";
import store from "@/store";
// import { jsonToObj } from "@/utils";
import { executeDBOperations } from "@/api/gateway/console";
// 用于查询数据库查重
let dbCache = [];

export async function getDBListReq(appId) {
  /**
   * 先使用show databses获取列表
   */
  return sendSQLReq(`show databases;`, true, appId)
    .then(data => {
      return (dbCache = handleDataKey(
        data.filter(item => !HIDEDB.includes(item.name)),
        "database"
      ));
    })
    .catch(err => {
      err.desc && Message.error(err.desc);
      return (dbCache = []);
    });
  /**
   * 先使用show databses获取列表
   * 使用describe获取数据库结构信息
   */
  // const dbList = await sendSQLReq(`show databases;`, true, appId)
  //   .then(data => {
  //     return (dbCache = handleDataKey(
  //       data.filter(item => !HIDEDB.includes(item.name)),
  //       "database"
  //     ));
  //   })
  //   .catch(err => {
  //     Message.error(err.desc);
  //     return (dbCache = []);
  //   });
  // const dbStruct = await Promise.all(dbList.map(item => sendSQLReq(`DESCRIBE ${item.name};`, true, appId)))
  //   .then(data => {
  //     console.log(data);
  //   })
  //   .catch(err => {
  //     Message.error(err.desc);
  //     return (dbCache = []);
  //   });
  // return dbStruct;
}

export function getDBStruct(dbName) {
  return executeDBOperations(`SELECT * FROM information_schema.ins_databases where name='${dbName}';`)
    .then(data => data[0] || {})
    .catch(() => ({}));
}
export function deleteDBReq(payload, appId = store.getters.appId) {
  let { dbName } = payload;
  return request({
    // url: `/private/data/sql/dropdb/${appId}/${dbName}`,
    url: '/rest/sql',
    data: `DROP DATABASE ${dbName};`,

    method: "post",
  })
    .then(data => {
      // data = jsonToObj(data);
      data = JSON.parse(JSON.stringify(data))
      if (data.code == 0) return data;
      return Promise.reject(data);
    })
    .catch(err => {
      err.desc && Message.error(err.desc);
      return Promise.reject(err);
    });
}

export function createDB(data, name, appId = store.getters.appId) {
  return request({
    // url: `/private/data/sql/createdb/${appId}/${name}`,
    url: '/rest/sql',
    data:
      `CREATE DATABASE \`${name}\`  ${Object.keys(DBFILED)
        .map(item => {
          let value = data[item];
          const isString = DBFILED[item]?.type == "string";
          if (isString && !value) return "";
          if (isString) {
            value = `'${value}'`;
          }
          return item + " " + value;
        })
        .join(" ")};`,

    method: "post",
  })
    .then(data => {
      // data = jsonToObj(data);
      data = JSON.parse(JSON.stringify(data))
      if (data.code == 0) return data;
      return Promise.reject(data);
    })
    .catch(err => {
      err.desc && Message.error(err.desc);
      return Promise.reject(err);
    });
}
export function updateDB(data, name) {
  return executeDBOperations(
    `ALTER DATABASE ${name} ${Object.keys(data)
      .map(key => {
        let value = data[key];
        if (DBFILED[key]?.type == "string") {
          value = `'${value}'`;
        }
        return key + " " + value;
      })
      .join(" ")};`
  );
}
// 检查数据库是否存在
export function checkDBName(dbName) {
  return dbCache.some(item => item.name == dbName);
}

export function handleDataKey(data, type, parent = "") {

  return data.map(item => {
    item.typeName = item.rollup ? 'table' : type;
    if (!item.name) {
      item.name = item[type + "_name"];
      // item.name=item.databaseName
    }
    item.parent = parent;
    if ((type == "database" && item.name == "log") || parent.startsWith("log.") || parent == "log") {
      item.noOperate = true;
    }
    item["node-key"] = item.name + type + parent + Math.random();

    return item;
  });
}
