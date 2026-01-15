import { sendSQLReq } from '@/api/explorer';
import { DBFILED, HIDEDB, DBCustomedFiled } from '@/const.ts';
import { request } from '@/utils/request.ts';
import { executeDBOperations } from '@/api/explorer';
import { trimEnd, trimStart } from 'lodash-es';

/**
 * 先使用show databases获取列表
 */
export async function getDBListReq() {
  try {
    const data: Recordable[] = await sendSQLReq<Recordable[]>(`show databases;`, true);
    return handleDataKey(
      data.filter((item: Recordable) => !HIDEDB.includes(item.name)),
      'database'
    );
  } catch (error) {
    return [];
  };
}

export async function getStables(database: string) {
  const databaseName = formatWithBackticks(database)
  try {
    const result = await sendSQLReq(`show  ${databaseName}.stables`);
    return Array.from(result.data).flat(1);
  } catch (error) {
    console.log(error);
    return [];
  }
}

export async function getDatabaseVariables(key?: string) {
  try {
    const result = await sendSQLReq('show variables');
    if (!key) {
      return result.data;
    }
    const hit = result.data.find((item: any) => item[0] == key);
    if (hit) {
      return hit[1];
    }
    return null;
  } catch (error) {
    console.log(error);
    return null;
  }
}

function parseUsage(usage: string) {
  if (!usage) return 'n/a';
  const parts = usage.split('=');
  if (parts.length !== 2) return usage;
  return trimEnd(trimStart(parts[1], '['), ']');
}
export async function getDBStruct(dbName: string) {
  const fields: Recordable = {};
  try {
    const disk_info = await executeDBOperations(`SHOW \`${dbName}\`.disk_info;`);
    if (disk_info && disk_info.length >= 2) {
      const compress_ratio = parseUsage(disk_info[0]._db_usage);
      if (compress_ratio === "NULL") {
        fields.compress_ratio = 'NULL (not flushed or no data)';
      } else {
        fields.compress_ratio = compress_ratio;
      }
      fields.disk_occupied = parseUsage(disk_info[1]._db_usage);
    }
  } catch (error) {
    console.log('No disk_info table or error occurred:', error);
  }
  const data = await executeDBOperations(`SELECT * FROM information_schema.ins_databases where name='${dbName}';`);
  Object.assign(fields, data[0] || {});
  return fields;
}

export function deleteDBReq(dbName: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_BASE_URL,
    url: '/api/-/rest/sql',
    data: `DROP DATABASE \`${dbName}\`;`,
    headers: {
      'Content-Type': 'text/plain'
    },
    method: 'post'
  })
    .then(data => {
      data = JSON.parse(JSON.stringify(data));
      if (data.code == 0) return data;
      return Promise.reject(data);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}

function getDBParamsSql(data: Recordable) {
  const result: string[] = [];
  Object.keys(DBFILED).forEach(item => {
    if (DBCustomedFiled.includes(item)) return;
    let value = data[item];
    const isString = DBFILED[item]?.type == 'string';
    if ((isString && !value) || value == undefined) return;
    if (isString) {
      value = `'${value}'`;
    }
    result.push(item + ' ' + value);
  });
  return result.join(' ');
}

export function createDB(data: Recordable) {
  const name = formatWithBackticks(data.name);
  return request({
    baseURL: import.meta.env.VITE_APP_BASE_URL,
    url: '/api/-/rest/sql',
    headers: {
      'Content-Type': 'text/plain'
    },
    data: `CREATE DATABASE ${name} ${getDBParamsSql(data)};`,
    method: 'post'
  })
    .then(data => {
      data = JSON.parse(JSON.stringify(data));
      if (data.code == 0) return data;
      ElMessage.error(JSON.stringify(data));
      return Promise.reject(data);
    })
    .catch(err => {
      return Promise.reject(err);
    });
}

function formatWithBackticks(name: any): string {
  const strName = String(name);

  if (strName.startsWith('`') && strName.endsWith('`')) {
    return strName;
  }

  return `\`${strName}\``;
}

export function updateDB(data: Recordable) {
  return executeDBOperations(`ALTER DATABASE \`${data.name}\` ${getDBParamsSql(data)};`);
}

export function handleDataKey(data: Array<Recordable>, type: string, parent: string = '') {
  return data.map(item => {
    item.typeName = item.rollup ? 'table' : type;
    if (!item.name) {
      item.name = item[type + '_name'];
      // item.name=item.databaseName
    }
    item.parent = parent;
    if ((type == 'database' && item.name == 'log') || parent.startsWith('log.') || parent == 'log') {
      item.noOperate = true;
    }
    if ((type == 'database' && item.name == 'audit') || parent.startsWith('audit.') || parent == 'audit') {
      item.noOperate = true;
    }
    item['node-key'] = item.name + type + parent;

    return item;
  });
}
