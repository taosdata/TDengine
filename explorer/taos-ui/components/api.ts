import { ExecuteSqlApiFn } from './explorer/model/useExplorer';
import { ElMessage } from 'element-plus';
import { TDengineStringType } from 'constants1/index';
import { escapeSpecialChar, composeType, processStringTagValue, addStrBackquote } from 'utils/tdengine';
import { ColumnStruct, CreateStableForm, CreateTableForm, CreateSubTbForm, TagStruct, CreateVirtualNormalTableForm } from './explorer/components/props';

export const NORMAL_TABLE = "NORMAL_TABLE";
export const VIRTUAL_NORMAL_TABLE = "VIRTUAL_NORMAL_TABLE";

export let executeSqlFn: ExecuteSqlApiFn | undefined;

export function setExecuteSqlFn(fn: ExecuteSqlApiFn) {
  executeSqlFn = fn;
}
export let getDbList: RequestApiFn<Recordable[]> = () => Promise.resolve([]);
export function setGetDbListFn(fn: RequestApiFn<Recordable[]>) {
  getDbList = fn;
}
type treeDataResult = [Recordable[], number];
export async function getPaginationData(
  countSql: string,
  dataSql: string,
  currentPage: number,
  pageSize: number,
  handleDataFn?: AnyFunction
): Promise<treeDataResult> {
  // 查询数量
  const count = await executeSqlFn!(countSql, false)
    .then(({ data }) => {
      return Number(data?.[0]?.[0]) || 0;
    })
    .catch(err => {
      err.desc && ElMessage.error(err.desc);
      return 0;
    });
  if (!count || !currentPage || !pageSize) return [[], 0];
  const startIndex = (currentPage - 1) * pageSize;
  // 查询数据
  if (dataSql.endsWith(';')) {
    dataSql = dataSql.slice(0, -1);
  }
  let data = await executeSqlFn!(`${dataSql} limit ${startIndex},${pageSize};`, true).catch(err => {
    err.desc && ElMessage.error(err.desc);
    return [];
  });
  if (typeof handleDataFn === 'function') {
    data = handleDataFn(data);
  }
  return [data, count];
}

export async function getStableListReq(dbName: string, permissionFn?: (dbName: string) => Promise<Recordable[]>) {
  const hasPermissionStableList = permissionFn ? await permissionFn(dbName) : [];
  const dataSql = `select stable_name, "STABLE" as \`type\`, isvirtual, 0 as total, create_time, last_update  from information_schema.ins_stables where db_name="${dbName}"${getStableConditionStr(hasPermissionStableList)}
   union
   select case when stable_name is NULL THEN \`type\` ELSE stable_name END as stable_name,
     \`type\`, FALSE as isvirtual, count(*) as total, null as create_time, null as last_update
   from information_schema.ins_tables where db_name="${dbName}" group by stable_name, \`type\``;
  const handleDataFn = (data: Recordable[]) => {
    let result = handleStableDataKey(data, dbName);
    if (hasPermissionStableList.length) {
      result = hasPermissionStableList.map(item => {
        return {
          privileges: item.privileges,
          ...(result.find(stable => stable.stable_name === item.stableName) ?? {})
        };
      });
    }
    return result;
  };
  return await getAllData(dataSql, handleDataFn).then(result => {
    if (result[0].length === 0) return result;
    result[1] = result[0].length;
    return getStableTags(
      dbName,
      result[0].map(item => item.name)
    ).then(tags => {
      result[0].forEach(item => (item.tags = tags.filter(tag => tag.stable_name == item.name)));
      return result;
    });
  });
}

export function searchStable(prefix: string, dbname: string) {
  return executeSqlFn!(
    `select * from information_schema.ins_stables where db_name='${dbname}' and  stable_name LIKE '%${prefix}%' order by stable_name limit 100`,
    true
  );
}

function getStableConditionStr(stableList: Recordable[]) {
  if (!stableList?.length) return '';
  return ` and stable_name in (${stableList.reduce((pre, cur) => {
    return pre ? `${pre},'${cur.stableName}'` : `'${cur.stableName}'`;
  }, '')})`;
}

export function getAllData(sql: string, handleDataFn?: AnyFunction): Promise<treeDataResult> {
  return executeSqlFn!(sql, true)
    .then((data): treeDataResult => [handleDataFn ? handleDataFn(data) : data, data.length])
    .catch(err => {
      err.desc && ElMessage.error(err.desc);
      return [[], 0];
    });
}

export function handleStableDataKey(data: Recordable[], dbName: string) {
  const nType = 'stable';
  const result: Recordable = {};
  // remove all 0 or null data
  data.forEach(item => {
    const reusultItem = result[item.stable_name];
    if (!reusultItem) {
      result[item.stable_name] = item;
      item.typeName = nType;
      if (!item.name) {
        item.name = item[nType + '_name'];
      }
      item.parent = dbName;
      item['node-key'] = item.name + nType + dbName;
    } else {
      if (item.create_time != 'NULL') {
        reusultItem.create_time = item.create_time;
      }
      if (item.last_update != 'NULL') {
        reusultItem.last_update = item.last_update;
      }
      if (item.total > 0) {
        {
          reusultItem.total = item.total;
        }
      }
    }
  });

  return Object.keys(result)
    .sort()
    .map(item => {
      return result[item];
    });
}

export function getStableTags(dbName: string, stableList: Recordable[]) {
  return executeSqlFn!(
    `select DISTINCT tag_name,stable_name,db_name,tag_type from information_schema.ins_tags where db_name='${dbName}' and stable_name in (${stableList.reduce(
      (pre, cur) => {
        return pre ? `${pre},'${cur}'` : `'${cur}'`;
      },
      ''
    )});`,
    true
  ).catch(err => {
    err.desc && ElMessage.error(err.desc);
    return [];
  });
}

export function getTagHierachy(dbName: string, stableName: string, tagName: string, tagType: string) {
  const TagValueSQL = `select tag_value, count(*) as total from information_schema.ins_tags where db_name="${dbName}" and stable_name="${stableName}" and tag_name="${tagName}" group by tag_value`;
  return getAllData(TagValueSQL).then(data => {
    return handleTagHierachyData(data[0], dbName, stableName, tagType, tagName);
  });
}

export function handleTagHierachyData(
  data: Recordable[],
  dbName: string,
  stableName: string,
  tagType: string,
  tagName: string
): treeDataResult {
  let tagTree;
  tagType = tagType.replace(/\(\d+\)/, '');
  if (TDengineStringType?.includes(tagType)) {
    const tmpTags: Recordable = {};
    data?.forEach(item => {
      if (!item || !item.tag_value) {
        return;
      }
      const parts: string[] = item.tag_value?.trim().split('.');
      let currentItem = tmpTags;
      let parentName = stableName;
      let parentNodeKey = `${dbName}:${stableName}`;
      parts?.forEach(subItem => {
        if (subItem.length === 0) {
          return false;
        }
        if (!currentItem[subItem]) {
          currentItem[subItem] = {
            name: subItem,
            typeName: 'dimension',
            parent: parentName,
            'node-key': `${parentNodeKey}:${subItem}:${tagName}`,
            total: item.total ? item.total : 0,
            children: {}
          };
        }
        parentName = currentItem[subItem].name;
        parentNodeKey = currentItem[subItem]['node-key'];
        currentItem = currentItem[subItem].children;
      });
    });
    const result = generateTagHierachy(tmpTags);
    tagTree = result?.obj;
  } else {
    tagTree = data?.map(item => {
      if (!item || !item.tag_value) {
        return;
      }
      return {
        name: item.tag_value,
        typeName: 'dimension',
        parent: stableName,
        'node-key': `${dbName}:${stableName}:${item.tag_value}:${tagName}`,
        total: item.total ? item.total : 0
      };
    });
  }
  return [tagTree, tagTree.length];
}

function generateTagHierachy(tmpTags: Recordable) {
  let total = 0;
  const result = Object.keys(tmpTags)
    .sort()
    .map(item => {
      const tmpObj = tmpTags[item];
      if (Object.keys(tmpObj.children).length > 0) {
        const result = generateTagHierachy(tmpObj.children);
        tmpObj.children = result.obj;
        tmpObj.total = result.total;
      } else {
        delete tmpObj.children;
      }
      total += tmpObj.total;
      return tmpObj;
    });
  return { obj: result, total: total };
}
export function deleteStableReq(payload: { dbName: string; stbName: string }) {
  const { dbName, stbName } = payload;
  return executeSqlFn!(`DROP STABLE \`${dbName}\`.\`${stbName}\`;`).catch(err => {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  });
}

export function getStableStructReq(dbName: string, stableName: string) {
  return executeSqlFn!(`DESCRIBE \`${dbName}\`.\`${stableName}\`;`, true)
    .then((list): { columns: ColumnStruct[]; tags: TagStruct[] } => {
      const columns: ColumnStruct[] = [];
      const tags: TagStruct[] = [];
      for (let i = 0; i < list.length; i++) {
        const item = list[i];
        if (item.note == 'TAG') {
          tags.push(item as TagStruct);
        } else {
          columns.push({ ...item, primaryKey: item.note == 'PRIMARY KEY' } as ColumnStruct);
        }
      }
      return {
        columns: columns,
        tags: tags
      };
    })
    .catch(err => {
      err.desc && ElMessage.error(err.desc);
      return Promise.reject(err);
    });
}

export async function getNormalTableStructReq(dbName: string, stableName: string): Promise<ColumnStruct[]> {
  try {
    const list = await executeSqlFn!(`DESCRIBE \`${dbName}\`.\`${stableName}\`;`, true);

    const columns: ColumnStruct[] = [];
    for (let i = 0; i < list.length; i++) {
      const item = list[i];
      columns.push({ ...item, primaryKey: item.note == 'PRIMARY KEY' } as ColumnStruct);
    }
    return columns;
  }
  catch (err: any) {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  };
}


export async function createVirtualNormalTableReq(formData: CreateVirtualNormalTableForm, dbName: string) {
  const { name, columns } = formData;
  console.log("Create virtual normal table with:", formData);
  const columnDefinitions = columns.map((item: any, index) => {
    if (index === 0) {
      // The first column is the primary key, so we add PRIMARY KEY constraint
      return `${escapeName(item.field)} ${composeType(item)}`;
    } else {
      return `${escapeName(item.field)} ${composeType(item)} FROM ${buildVirtualColumn(item)}`;
    }
  }).join(',');
  console.log("Column definitions for virtual normal table:", columnDefinitions);

  try {
    await executeSqlFn!(`CREATE VTABLE \`${dbName}\`.${escapeName(name)} (${columnDefinitions});`);
  }
  catch (err: any) {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  }
}
export async function createNormalTableReq(formData: CreateTableForm, dbName: string) {
  const { name, columns } = formData;
  const columnDefinitions = columns.map(item => {
    return `${addStrBackquote(escapeSpecialChar(item.field))} ${composeType(item)}${item.encode ? ' ENCODE ' + `'${item.encode}'` : ''}${item.compress ? ' COMPRESS ' + `'${item.compress}'` : ''}${item.level ? ' LEVEL ' + `'${item.level}'` : ''}${item.primaryKey ? ' PRIMARY KEY' : ''}`;
  }).join(',');

  try {
    await executeSqlFn!(`CREATE TABLE \`${dbName}\`.${name} (${columnDefinitions});`);
  }
  catch (err: any) {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  }
}
export interface changeNormalTableStructData {
  operation: string;
  first_field: string;
  second_field?: string;
  other?: string;
}
// 修改普通表的表结构
export async function changeNormalTableStruct(data: changeNormalTableStructData, name: string, dbName: string) {
  const { operation, first_field = '', second_field = '', other = '' } = data;
  let sql = '';
  const hasBacktick = first_field.startsWith('`') && first_field.endsWith('`');
  let escaped_first_field = escapeSpecialChar(first_field);
  const escaped_second_field = escapeSpecialChar(second_field);
  if (!hasBacktick) {
    escaped_first_field = `\`${escaped_first_field}\``;
  }
  sql = `ALTER TABLE  \`${dbName}\`.\`${name}\` ${operation} ${escaped_first_field} ${escaped_second_field}${other};`;
  try {
    return await executeSqlFn!(sql);
  } catch (err: any) {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  }
}
export async function createStableReq(formData: CreateStableForm, dbName: string) {
  const { name, columns, tags } = formData;

  try {
    return await executeSqlFn!(
      `CREATE STABLE \`${dbName}\`.${name} (${columns
        .map(
          item =>
            `${addStrBackquote(escapeSpecialChar(item.field))} ${composeType(item)}${item.encode ? ' ENCODE ' + `'${item.encode}'` : ''}${item.compress ? ' COMPRESS ' + `'${item.compress}'` : ''}${item.level ? ' LEVEL ' + `'${item.level}'` : ''
            }${item.primaryKey ? ' PRIMARY KEY' : ''}`
        )
        .join(
          ','
        )}) TAGS (${tags.map(item => `${addStrBackquote(escapeSpecialChar(item.field))} ${composeType(item)}`).join(',')});`
    );
  } catch (err: any) {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  }
}
export interface changeStbStructData {
  operation: string;
  first_field: string;
  second_field?: string;
  other?: string;
}
// 修改超级表的表结构
export function changeStableStruct(data: changeStbStructData, stableName: string, dbName: string) {
  // eslint-disable-next-line prefer-const
  let { operation, first_field = '', second_field = '', other = '' } = data;
  let sql = '';
  const hasBacktick = first_field.startsWith('`') && first_field.endsWith('`');
  first_field = escapeSpecialChar(first_field);
  second_field = escapeSpecialChar(second_field);
  if (!hasBacktick) {
    first_field = `\`${first_field}\``;
  }
  sql = `ALTER STABLE  \`${dbName}\`.\`${stableName}\` ${operation} ${first_field} ${second_field}${other};`;
  return executeSqlFn!(sql).catch(err => {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  });
}
export function handleDataKey(data: Recordable[], type: string, parent = '') {
  return data.map(item => {
    item.typeName = type;
    if (!item.name) {
      item.name = item[type + '_name'];
    }
    item.parent = parent;
    item['node-key'] = item.name + type + parent;
    return item;
  });
}

// 获取超级表下属子表
export async function getTableListReq(params: Recordable) {
  const { currentPage, pageSize, stbName, dbName, filter = '', condition = '' } = params;
  const parent = `${dbName}.${stbName}`;
  let where = `where db_name='${dbName}' and `;
  if (stbName === VIRTUAL_NORMAL_TABLE || stbName === NORMAL_TABLE) {
    where += `\`type\`='${stbName}'`;
  } else {
    where += `stable_name='${stbName}'`;
  }
  console.log("Fetch table list for", where);
  if (filter) {
    where += ` and table_name like '%${filter}%'`;
  }
  if (condition) {
    where += ` and ${condition}`;
  }
  const dataSql = `select * from information_schema.ins_tables ${where}`;
  const handleFn = (data: Recordable[]) => handleDataKey(data, 'table', parent);
  if (!pageSize) return getAllData(dataSql, handleFn);
  const countSql = `select count(*) from information_schema.ins_tables ${where}`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize, handleFn);
}
export function searchTable(prefix: string, dbname: string) {
  return executeSqlFn!(
    `select * from information_schema.ins_tables where db_name='${dbname}' and table_name LIKE '%${prefix}%' order by table_name limit 100`,
    true
  );
}

export function deleteTableReq(payload: { dbName: string; tbName: string }) {
  const { dbName, tbName } = payload;
  return executeSqlFn!(`DROP TABLE \`${dbName}\`.\`${tbName}\`;`).catch(err => {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  });
}

function escapeName(name: string) {
  if (!name) return '';
  if (name.startsWith('`') && name.endsWith('`')) {
    return name;
  }
  return '`' + name + '`';
}

/**
 * 构建虚拟子表的列名
 */
function buildVirtualColumn(column: Recordable) {
  return `${escapeName(column.database)}.${escapeName(column.table)}.${escapeName(column.value)}`;
}

/** 创建虚拟子表或子表 */
export function createTableReq(formData: CreateSubTbForm, dbName: string) {
  const { name, stbTmpl, tags, isVirtual, columns } = formData;
  if (isVirtual) {
    // 以虚拟超级表为模版创建虚拟子表
    return executeSqlFn!(
      `CREATE VTABLE \`${dbName}\`.${name} (${columns.map((item: Recordable) => buildVirtualColumn(item)).join(',')}) USING \`${dbName}\`.\`${stbTmpl}\` (${tags.map((item: Recordable) => `\`${item.field}\``).join(',')}) TAGS (${tags
        .map((item: Recordable) => processStringTagValue(item.type, item.value))
        .join(',')});`
    ).catch(err => {
      err.desc && ElMessage.error(err.desc);
      return Promise.reject(err);
    });
  }
  // 以超级表为模版创建表
  return executeSqlFn!(
    `CREATE TABLE \`${dbName}\`.${name} USING \`${dbName}\`.\`${stbTmpl}\` (${tags.map((item: Recordable) => `\`${item.field}\``).join(',')}) TAGS (${tags
      .map((item: Recordable) => processStringTagValue(item.type, item.value))
      .join(',')});`
  ).catch(err => {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  });
}

export function getSubtbInitStruct(dbName: string, stbName: string) {
  return getStableStructReq(dbName, stbName)
    .then((res: Recordable) => {
      const { columns, tags } = res;
      return {
        name: '',
        stbTmpl: stbName,
        columns: columns.map((item: Recordable) => {
          item.value = '';
          return item;
        }),
        tags: tags.map((item: Recordable) => {
          item.value = '';
          return item;
        })
      };
    })
    .catch(() => ({
      name: '',
      stbTmpl: stbName,
      columns: [],
      tags: []
    }));
}

export async function getValidTablesForVirtualTableRef(dbName: string, type: string, tbPattern: string = '', limit: number = 10) {
  console.log('getValidColumnsForVirtualTableRef', dbName, type);
  const res = await executeSqlFn!(`SELECT table_name FROM information_schema.ins_columns where table_type != 'SUPER_TABLE' and db_name = '${dbName}' and col_type = '${type}' and table_name like '%${tbPattern || ''}%' limit ${limit};`, true);

  console.log('getValidColumnsForVirtualTableRef res', res);
  return res.map((item: Recordable) => {
    return item.table_name;
  });
}

export async function getValidColumnsForVirtualTableRef(dbName: string, tbName: string, type: string) {
  console.log('getValidColumnsForVirtualTableRef', dbName, tbName, type);
  const res: Recordable = await getStableStructReq(dbName, tbName);
  console.log('getValidColumnsForVirtualTableRef res', res);
  const { columns } = res;
  return columns.filter((item: Recordable, index: number) => {
    return index != 0 && // 排除第一个字段
      item.type == type;
  });
}

// 修改表结构
export async function changeTableStruct(data: changeStbStructData, tableName: string, dbName: string) {
  // eslint-disable-next-line prefer-const
  const { operation, first_field = '' } = data;
  const second_field = escapeSpecialChar(data.second_field || '');
  let sql = '';
  sql = `ALTER TABLE  \`${dbName}\`.\`${tableName}\` ${operation} ${first_field} ${second_field};`;
  try {
    await executeSqlFn!(sql);
  } catch (err: any) {
    err.desc && ElMessage.error(err.desc);
    return Promise.reject(err);
  };
}

// 获取表的tag value
export function getTagValue(tags: Recordable[], database: string, table_name: string) {
  if (!tags.length) return Promise.resolve({} as Recordable);
  return executeSqlFn!(
    `SELECT DISTINCT TBNAME, ${tags.map(item => `\`${item.field}\``).join(',')} from \`${database}\`.\`${table_name}\`;`,
    true
  )
    .then(data => {
      const result = data?.[0] || {};
      Object.keys(result).forEach(key => {
        if (key == 'tbname') return;
        result[key] = result[key] + '';
      });
      return result;
    })
    .catch(err => {
      err.desc && ElMessage.error(err.desc);
      return {} as Recordable;
    });
}

// 获取表的tag value
export function getTableTagValueMap(dbname: string, tbname: string) {
  return getSubtbTagAndColumnList(dbname, tbname).then(data =>
    getTagValue(
      data.filter(item => item.typeName === 'tag'),
      dbname,
      tbname
    )
  );
}

export function getSubtbCurrentStruct(dbName: string, stbName: string, tbName: string) {
  return executeSqlFn!(`DESCRIBE \`${dbName}\`.\`${tbName}\`;`, true)
    .then(async res => {
      const tags = res.filter(item => item.note);
      const tagValueObj: Recordable = await getTagValue(tags, dbName, tbName);
      return {
        name: tbName,
        stbTmpl: stbName,
        tags: tags.map((item: Recordable) => {
          item.value = tagValueObj[item.field] || '';
          return item;
        }),
        columns: res.filter(item => !item.note)
      };
    })
    .catch(() => ({
      name: '',
      stbTmpl: stbName,
      tags: [],
      columns: []
    }));
}

export function getSubtbTagAndColumnList(dbName: string, tbName: string) {
  return executeSqlFn!(`DESCRIBE \`${dbName}\`.\`${tbName}\`;`, true)
    .then(res => handleColumnData(res))
    .catch(() => []);
}
export function handleColumnData(data: Recordable[]) {
  return data.map(item => {
    const result: Recordable = {};
    result.name = item.field;
    // 此处不展示标签，在表格详细信息中进行展示
    if (item.note) {
      result.typeName = 'tag';
    } else {
      result.typeName = 'column';
    }
    result.field = item.field;
    result.type = item.type;
    result.length = item.length;
    result.dataType = composeType({
      type: item.type,
      length: item.length
    } as any);
    result['node-key'] = result.name + result.dataType;
    result.leaf = true;
    return result;
  });
}

interface getTbWithTags {
  currentPage: number;
  pageSize: number;
  stbName: string;
  dbName: string;
  tag_value?: string;
  tagName?: string;
  conditions?: string;
  filter?: string;
}

export async function getTableWithTags(params: getTbWithTags) {
  const { currentPage, pageSize, stbName, dbName, tag_value, tagName, conditions = '', filter = '' } = params;
  const parent = `${dbName}.${stbName}`;
  let where = `where db_name='${dbName}' and stable_name='${stbName}'`;
  if (tag_value && tagName) {
    where += ` and tag_value='${escapeSpecialChar(tag_value)}' and tag_name='${tagName}'`;
  } else if (conditions) {
    where += ` and ${conditions}`;
  }
  if (filter) {
    where += ` and table_name like '%${filter}%'`;
  }
  const dataSql = `select distinct table_name,stable_name from information_schema.ins_tags ${where} order by table_name`;
  const handleFn = (data: Recordable[]) => handleDataKey(data, 'table', parent);
  if (!pageSize) return getAllData(dataSql, handleFn);
  const countSql = `select count(*) from information_schema.ins_tags ${where}`;
  return getPaginationData(countSql, dataSql, currentPage, pageSize, handleFn);
}

// 判断超级表是否已经存在
export function isStableExist(stbName: string, dbName: string) {
  return executeSqlFn!(
    `select * from information_schema.ins_stables where db_name='${dbName}' and  stable_name='${stbName}'`
  ).then(res => res.rows);
}
