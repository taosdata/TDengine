import { compareVersion, processStringTagValue } from 'utils/tdengine';
import { t } from 'locales';
import { validTDKeywords, validTableName } from 'utils/validate';
import { CreateSubTbForm } from './props';
import { Reactive } from 'vue';
import Node from 'components/tree/src/model/node';
import { customCompCallback } from '../model/useExplorer';
export function isGte3300(version: string) {
  return compareVersion(version, '>=3.3.0.0');
}

export const columnRule = {
  required: true,
  message: t('common.notEmptyTemp', [t('stb.columnName')]),
  trigger: 'blur'
};

export const virtualColumnRule = [
  {
    required: true,
    message: t('common.notEmptyTemp', [t('stb.columnName')]),
    trigger: 'blur'
  }
];
export const tagRule = {
  required: true,
  message: t('common.notEmptyTemp', [t('stb.tagName')]),
  trigger: 'blur'
};
export const tbNameRule = [
  {
    required: true,
    message: t('common.requiredTemp', [t('common.name')]),
    trigger: 'blur'
  },
  {
    validator: (_: any, value: string, callback: AnyFunction) => {
      if (validTDKeywords(value)) {
        return callback(new Error(t('explorer.tdKewordTip', [value])));
      }
      callback(validTableName(value) ? undefined : new Error(t('common.formatErrorTemp', [t('common.name')])));
    },
    trigger: 'blur'
  }
];

export const stbNameRule = [
  {
    required: true,
    message: t('common.requiredTemp', [t('common.name')]),
    trigger: 'blur'
  },
  {
    validator: (_: any, value: string, callback: AnyFunction) => {
      if (validTDKeywords(value)) {
        return callback(new Error(t('explorer.tdKewordTip', [value])));
      }
      callback(validTableName(value) ? undefined : new Error(t('common.formatErrorTemp', [t('common.name')])));
    },
    trigger: 'blur'
  }
];

export function generateCreateSubTableSql(data: CreateSubTbForm, dbName: string, isVirtual: boolean) {
  console.log("generate sub table", data, isVirtual);
  if (isVirtual) return generateCreateVirtualSubTableSql(data, dbName);
  const { name, stbTmpl, tags } = data;
  return `CREATE TABLE \`${dbName}\`.${name} USING \`${dbName}\`.\`${stbTmpl}\` (${tags.map(item => `\`${item.field}\``).join(',')}) TAGS (${tags
    .map(item => processStringTagValue(item.type, item.value))
    .join(',')});`;
}

export function generateCreateVirtualSubTableSql(data: CreateSubTbForm, dbName: string) {
  const { name, stbTmpl, columns, tags } = data;
  console.log("Virtual subtable", data);
  return `CREATE VTABLE \`${dbName}\`.${name} (${columns.map(item => item.value).join(',')}) USING \`${dbName}\`.\`${stbTmpl}\` (${tags.map(item => `\`${item.field}\``).join(',')}) TAGS (${tags
    .map(item => processStringTagValue(item.type, item.value))
    .join(',')});`;
}

export const updateFavoriteEvent = useEventBus('updateFavorite');
export const setFavoriteEvent = useEventBus('setFavorite');

export const addLogEvent = useEventBus<Recordable>('addLog');
export const changeLogSortEvent = useEventBus('changeLogSort');
export function useTreeHeight() {
  const height = ref('800px');
  let el: HTMLDivElement | null = null;
  onMounted(() => {
    el = document.querySelector('.dbs-tree');
    window.addEventListener('resize', setHeight);
  });
  onUnmounted(() => {
    window.removeEventListener('resize', setHeight);
  });

  function setHeight() {
    if (!el) return (height.value = '800px');
    height.value = el.clientHeight - 100 + 'px';
  }
  return height;
}

export const currentInfoDataProviderKey = Symbol('currentInfoDataProviderKey');
export const sqlExecResultProviderKey = Symbol('sqlExecResultProviderKey');
export type InfoData = Reactive<{
  db: Recordable;
  stb: Recordable;
  tb: Recordable;
  type: 'db' | 'stb' | 'tb';
}>;
export function getCurrentInfoDataProvider(): InfoData {
  return inject(
    currentInfoDataProviderKey,
    reactive({
      db: {},
      stb: {},
      tb: {},
      type: 'db'
    })
  );
}

export const stableAdvancedFilterData = reactive({
  data: {
    enable: false,
    conditionJson: [],
    sql: ''
  },
  key: ''
});

export const applyStbAdvancedEvent = useEventBus<Recordable>('applyStbAdvanced');

export const sqlExecResult = reactive<{
  data: string[][];
  head: {
    field: string;
    type: string;
    length: number;
  }[];
}>({
  data: [],
  head: []
});
function getDataColumnWidth(length = 100) {
  return length < 100 ? 100 : Math.min(length, 400);
}
export const addSqlCodeEvent = useEventBus<string>('addSqlCode');

export function handleSqlExecuteSuccess(data: RestApiResult, sql: string, startTime: number) {
  addLogEvent.emit({
    ...generateExecTime(data, startTime),
    sql,
    type: 1,
    createAt: Date.now(),
    rows: data.rows
  });
  sqlExecResult.data = data.data;
  sqlExecResult.head = data.column_meta.map(item => ({
    field: item[0],
    type: item[1],
    length: getDataColumnWidth(Number(item[2]))
  }));
}

export function handleSqlExecuteFail(data: RestApiResult, sql: string, startTime: number) {
  addLogEvent.emit({
    ...generateExecTime(data, startTime),
    sql,
    type: 0,
    createAt: Date.now(),
    rows: 0,
    message: data
  });
  sqlExecResult.data = [];
  sqlExecResult.head = [];
}

function generateExecTime(data: RestApiResult, startTime: number) {
  const totalTime = Date.now() - startTime;
  // timimg为纳秒，转为毫秒
  const executTime = (data.timing ?? 0) / 1e6 || 1;
  const networkTime = totalTime - executTime;
  return { totalTime, executTime, networkTime };
}

export const editorFocusEvent = useEventBus('editorFocus');

export const panelActiveTab = ref('grid');
export const partActiveTab = ref('sql');
export const favoriteActiveTab = ref('personal');

export const currentDetailComponentConfig = reactive({
  component: '',
  name: '',
  props: {} as Recordable,
  listeners: {} as Recordable
});

export const favoriteData = reactive<{
  personal: Recordable[];
  shared: Recordable[];
  total: number;
}>({
  personal: [],
  shared: [],
  total: 0
});

export const favoriteParams = reactive<{
  page: number;
  page_size: number;
  sql_desc_fuzzy: string;
  is_public?: boolean;
}>({
  page: 1,
  page_size: 10,
  sql_desc_fuzzy: '',
  is_public: false
});

export function checkPermission(privilege: string | string[], node: Node, type?: PermissionType | PermissionType[]) {
  const data = node.data;
  switch (data.typeName) {
    case 'database':
      return handleDBPermission(privilege, node);
    case 'stable':
      return handleStbPermission(privilege, node, type!);
    case 'table':
      return handleTbPermission(privilege, node, type!);
    default:
      return false;
  }
}
function handleDBPermission(privilege: string | string[], node: Node) {
  const privileges = node.data.privileges;
  if (!privileges) return false;
  if (Array.isArray(privilege)) {
    return privilege.some(item => privileges.some((p: Recordable) => p.name == item));
  } else {
    return privileges.some((item: Recordable) => item.name == privilege);
  }
}
/* 
  1. 先去 stable 中查找 type 传入的 read 或 write 权限
  2. 如果 stable 中没有，则去 database 中查找 read 或 write 权限
  3. 如果上述均未找到，则去 db 权限中找 privilege 传入的权限
*/
export type PermissionType = 'read' | 'write' | 'view';
function handleStbPermission(privilege: string | string[], node: Node, type: PermissionType | PermissionType[]) {
  const stbPrivileges = node.data.privileges ?? [];
  const dbPrivileges = node.parent.data.privileges ?? [];
  if (Array.isArray(type)) {
    if (type.some(item => stbPrivileges.some((p: Recordable) => p.name.includes(item)))) return true;
    return (
      type.some(item => dbPrivileges.some((p: Recordable) => p.name.includes(item))) ||
      handleDBPermission(privilege, node.parent)
    );
  } else {
    if (stbPrivileges.some((item: Recordable) => item.name.includes(type))) return true;
    return (
      dbPrivileges.some((item: Recordable) => item.name.includes(type)) || handleDBPermission(privilege, node.parent)
    );
  }
}
function handleTbPermission(privilege: string | string[], node: Node, type: PermissionType | PermissionType[]) {
  let stbNode = node;
  while (stbNode.data.typeName != 'stable') {
    stbNode = stbNode.parent;
  }
  return handleStbPermission(privilege, stbNode, type);
}

export const dbList = ref<Recordable[]>([]);

export function backSqlPart(init = false) {
  if (!init) {
    customCompCallback('');
  }
  currentDetailComponentConfig.component = '';
  currentDetailComponentConfig.props = {};
  currentDetailComponentConfig.name = '';
  currentDetailComponentConfig.listeners = {};
  partActiveTab.value = 'sql';
}

export const viewTableDataLimit = 200;

export const treeNodeKey = 'node-key';
