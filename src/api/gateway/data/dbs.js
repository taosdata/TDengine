import { sendSQLReq } from "@/api/gateway/console";
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
    // baseURL:'',
    url: '/rest/sql',
    data: `DROP DATABASE \`${dbName}\`;`,
    headers: {
      "Content-Type":"text/plain"
  },
    method: "post",
  })
    .then(data => {
      // data = jsonToObj(data);
      data = JSON.parse(JSON.stringify(data))
      if (data.code == 0) return data;
      return Promise.reject(data);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}

export function createDB(data, name, appId = store.getters.appId) {
  return request({
    // baseURL:'',
    url: '/rest/sql',
    headers: {
      "Content-Type":"text/plain"
  },
    data:
      `CREATE DATABASE \`${name}\`  ${Object.keys(DBFILED)
        .map(item => {
          let value = data[item];
          const isString = DBFILED[item]?.type == "string";
          if(value == undefined) return ""
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
      return Promise.reject(err);
    });
}
export function updateDB(data, name) {
  return executeDBOperations(
    `ALTER DATABASE ${name} ${Object.keys(data)
      .map(key => {
        let value = data[key];
        if(value == undefined) return ""
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

//数据库管理
export function disableDBUser(userId, dbname, appId = store.getters.appId) {
  return request({
    url: `/region/${store.getters.region}/db/privilege/disable/${appId}/${dbname}/user/${userId}`,
    method: "put",
  });
}
export function disableDBUserGroup(groupId, dbname, appId = store.getters.appId) {
  return request({
    url: `/region/${store.getters.region}/db/privilege/disable/${appId}/${dbname}/group/${groupId}`,
    method: "put",
  });
}
export function enableDBUserRole(data, appId = store.getters.appId) {
  return request({
    url: `/region/${store.getters.region}/db/privilege/active/user/${appId}`,
    data,
    method: "post",
  });
}
export function enableDBUserGroup(groupId, dbname, appId = store.getters.appId) {
  return request({
    url: `/region/${store.getters.region}/db/privilege/active/${appId}/${dbname}/group/${groupId}`,
    method: "put",
  });
}
export function deleteDBUser(userId, dbname, appId = store.getters.appId) {
  return request({
    url: `/region/${store.getters.region}/db/privilege/delete/${appId}/${dbname}/user/${userId}`,
    method: "put",
  });
}
export function deleteDBUserGroup(groupId, dbname, appId = store.getters.appId) {
  return request({
    url: `/region/${store.getters.region}/db/privilege/delete/${appId}/${dbname}/group/${groupId}`,
    method: "put",
  });
}
// 获取用户组数据库级别关联资源
export function getDatabaseGroupResource(group_id, databaseId, appId = store.getters.appId) {
  return request({
    url: `/group/${group_id}/resources/${appId}`,
  }).then(data => data.filter(item => item.databaseId == databaseId));
}
// 修改group角色
export function apendInstanceGroupResource(data, id, appId = store.getters.appId) {
  return request({
    url: `/group/${appId}/roles/${id}`,
    method: "put",
    data: JSON.stringify(data),
  });
}
// 获取app下的所有用户
export function getAppUser(params, appId = store.getters.appId) {
  return request({
    url: `/region/${store.getters.region}/app/${appId}/users`,
    params,
  });
}

// 获取用户组实例级别关联资源
export function getInstanceGroupResource(group_id, appId = store.getters.appId) {
  return request({
    url: `/group/${group_id}/resources/${appId}`,
  });
}

export function getOrganizationResource(user_id) {
  return request({
    url: `/user/${user_id}/resources`,
  });
}
export function getInstanceResource(user_id, app_id = store.getters.appId) {
  return request({
    url: `/user/${user_id}/resources/` + app_id,
  });
}
export function getDBResource(user_id, databaseId, app_id = store.getters.appId) {
  return request({
    url: `/user/${user_id}/resources/` + app_id,
  }).then(data => data.filter(item => item.databaseId == databaseId));
}
// 获取group列表
export function getGroupList(params) {
  return request({
    url: "/group",
    params,
  });
}
// 获取当前实例下的用户组
export function getInstanceGroup(params, appId = store.getters.appId) {
  return request({
    url: `/app/${appId}/groups`,
    params,
  });
}
// 获取当前组织下的已经分配了的user-role和group-role列表
export function getOrganizationUserAndGroupRoleList(params, orgId = store.getters.orgId) {
  return request({
    url: "/org/grants/" + orgId,
    params,
  });
}
export function getGrantList(params) {
  return request({
    url: "/role/grant-list",
    params,
  });
}
export function getUnGrantList(params) {
  return request({
    url: `/role/no-granted-list/${params.type}/${params.id}`,
  });
}
// 创建group
export function createGroup(data) {
  return request({
    url: "/group",
    method: "post",
    data,
  });
}

// 修改group
export function updateGroup(data, id) {
  return request({
    url: "/group/" + id,
    method: "put",
    data,
  });
}
export function apendOrganizationGroupResource(data, id) {
  return request({
    url: "/group/roles/" + id,
    method: "put",
    data: JSON.stringify(data),
  });
}
// 获取group用户列表
export function getGroupUserList(id) {
  return request({
    url: "/group/user/" + id,
    // params,
  });
}
// 删除group用户
export function disableOrganizationGroupUser(group_id, user_id) {
  return request({
    url: `/group/role/${group_id}/${user_id}`,
    method: "delete",
  });
}
// 获取用户组组织级别关联资源
export function getOrganizationGroupResource(group_id) {
  return request({
    url: `/group/${group_id}/resources`,
  });
}
