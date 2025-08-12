<!-- eslint-disable vue/no-mutating-props -->
<template>
  <el-tooltip placement="right" popper-class="tree-label-popper" :content="labelText">
    <div class="custom-node-wrapper">
      <div class="custom-node-label no-wrap">
        <Icon :name="iconMap[type]" class="node-icon"></Icon>
        <span>{{ labelText }}</span>
      </div>
      <span v-if="props.data.dataType" class="column-type">{{ props.data.dataType }}</span>
      <section v-if="type != 'dimension'" class="operate-btn-wrapper" @click.stop>
        <template v-if="!props.data.dataType">
          <!-- stable 按钮 -->
          <template v-if="type == 'stable' && props.data.tags.length">
            <el-dropdown @command="stableTagChange">
              <span class="el-dropdown-link more-btn">
                {{ t('stb.tag')
                }}<el-icon class="el-icon--right">
                  <ArrowDown />
                </el-icon>
              </span>
              <template #dropdown>
                <el-dropdown-menu class="stable-tag-dropdown">
                  <el-dropdown-item
                    v-for="item in props.data.tags"
                    :key="item.tag_name"
                    class="stable-tag-dropdown-item"
                    :command="item.tag_name"
                  >
                    <template v-if="tagFilter == item.tag_name">
                      <CaretRight class="selected-icon active" />
                      {{ item.tag_name }}
                      <Close class="clear-icon" />
                    </template>
                    <span v-else>{{ item.tag_name }}</span>
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </template>
          <el-tooltip
            v-if="isHasPermission('db:read', 'view') && type == 'table'"
            effect="light"
            placement="top"
            :content="getTooltip(data, 'view')"
          >
            <More class="operate-icon rotate-90" @click.stop="view" />
          </el-tooltip>
          <!-- db 和 stable 更多按钮 -->
          <template v-if="showMoreBtnType.includes(type)">
            <el-dropdown v-if="isHasPermission('db:read', ['read', 'write'])">
              <div>
                <el-tooltip effect="light" placement="right" :content="getTooltip(data, 'moreOperations')">
                  <More class="operate-icon more-btn ml-10px rotate-90" />
                </el-tooltip>
              </div>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item v-if="isHasPermission('db:alter', 'write')" command="add" class="tree-menu">
                    <el-tooltip effect="light" placement="right" :content="getTooltip(data, 'add')">
                      <div class="flex-start tree-menu-item" @click.stop="add()">
                        <Plus class="operate-icon"></Plus>
                        <div class="tree-menu-label">
                          {{ getSubCreateText() }}
                        </div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>

                  <el-dropdown-item
                    v-if="isHasPermission('db:read', 'read') && type == 'stable' && data.tags.length"
                    command="advancedFilter"
                    class="tree-menu"
                  >
                    <el-tooltip
                      effect="light"
                      placement="right"
                      :content="
                        props.stableTagFilterMap[key].advanced.enable &&
                        props.stableTagFilterMap[key].advanced.condition
                          ? props.stableTagFilterMap[key].advanced.condition
                          : t('explorer.advancedFilter')
                      "
                    >
                      <div class="flex-start tree-menu-item" @click.stop="advancedFilter">
                        <i
                          class="flex-center mr-0px!"
                          placement="right"
                          :class="{
                            'advanced-filter-active': props.stableTagFilterMap[key].advanced.enable
                          }"
                        >
                          <Filter class="operate-icon"></Filter>
                        </i>
                        <div class="tree-menu-label">{{ t('common.filter') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>

                  <el-dropdown-item v-if="isHasPermission('db:read', 'read')" command="view" class="tree-menu">
                    <el-tooltip effect="light" placement="right" :content="getTooltip(data, 'view')">
                      <div class="flex-start tree-menu-item" @click.stop="view">
                        <View class="operate-icon"></View>
                        <div class="tree-menu-label">{{ t('common.view') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item v-if="isHasPermission('db:alter', 'write')" command="edit" class="tree-menu">
                    <el-tooltip effect="light" placement="right" :content="getTooltip(data, 'edit')">
                      <div class="flex-start tree-menu-item" @click.stop="edit">
                        <Edit class="operate-icon"></Edit>
                        <div class="tree-menu-label">{{ t('common.edit') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item
                    v-if="
                      isCloud &&
                      isHasPermission(
                        ['user-role:grant', 'user-role:delete', 'group-role:grant', 'group-role:delete'],
                        'read'
                      )
                    "
                    command="manage"
                    class="tree-menu"
                  >
                    <el-tooltip effect="light" placement="right" :content="getTooltip(data, 'manage')">
                      <div class="flex-start tree-menu-item" @click.stop="manage">
                        <Lock class="operate-icon"></Lock>
                        <div class="tree-menu-label">{{ t('common.privileges') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item
                    v-if="type == 'database' && isCloud && isHasPermission('db:audit')"
                    command="viewLog"
                    class="tree-menu"
                    ><el-tooltip effect="light" :content="getTooltip(data, 'log')" placement="right">
                      <div class="flex-start tree-menu-item" @click.stop="viewLog">
                        <Tickets class="operate-icon"></Tickets>
                        <div class="tree-menu-label">{{ t('common.log') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item v-if="isHasPermission('db:drop', 'write')" command="del" class="tree-menu"
                    ><el-tooltip effect="light" placement="right" :content="getTooltip(data, 'del')">
                      <div class="flex-start tree-menu-item" @click="del">
                        <Delete class="operate-icon"></Delete>
                        <div class="tree-menu-label">{{ t('common.delete') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item v-if="type == 'stable'" command="clickAdd" class="tree-menu"
                    ><el-tooltip
                      effect="light"
                      placement="right"
                      :content="t('explorer.viewData', [viewTableDataLimit])"
                    >
                      <div class="flex-start tree-menu-item" @click.stop="clickAdd(true)">
                        <Search class="operate-icon"></Search>
                        <div class="tree-menu-label">{{ t('common.query') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item command="clickAdd" class="tree-menu">
                    <el-tooltip effect="light" placement="right" :content="t('explorer.appendEditor')">
                      <div class="flex-start tree-menu-item" @click.stop="clickAdd(false)">
                        <Icon class="operate-icon" name="code"></Icon>
                        <div class="tree-menu-label">{{ t('common.append') }}</div>
                      </div>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item v-if="type == 'database'" command="clickAdd" class="tree-menu">
                    <div class="flex-start tree-menu-item" @click.stop="clickAdd(false)">
                      <Search class="operate-icon"></Search>
                      <div class="tree-menu-label">
                        <el-input
                          v-model="namefilterText"
                          size="small"
                          clearable
                          style="width: 80px"
                          @clear="dbFilter"
                          @keyup.enter="dbFilter"
                        >
                        </el-input>
                      </div>
                    </div>
                  </el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </template>

          <!-- table 按钮 -->
          <template v-if="type == 'table'">
            <el-tooltip
              v-if="isHasPermission('db:alter', 'write')"
              effect="light"
              placement="top"
              :content="getTooltip(data, 'edit')"
            >
              <Edit class="operate-icon" @click.stop="edit"></Edit>
            </el-tooltip>
            <el-tooltip effect="light" :content="t('explorer.viewData', [viewTableDataLimit])">
              <Search class="operate-icon" @click.stop="clickAdd(true)"></Search>
            </el-tooltip>
            <el-tooltip effect="light" :content="t('explorer.appendEditor')">
              <Icon class="operate-icon ml-10px" name="code" @click.stop="clickAdd()"></Icon>
            </el-tooltip>
            <el-tooltip
              v-if="isHasPermission('db:drop', 'write')"
              effect="light"
              placement="top"
              :content="getTooltip(data, 'del')"
            >
              <Delete class="operate-icon ml-10px" @click.stop="del"></Delete>
            </el-tooltip>
          </template>
        </template>
      </section>
    </div>
  </el-tooltip>
</template>

<script lang="ts" setup>
import type Node from 'components/tree/src/model/node';
import { t } from 'locales';
import {
  checkPermission,
  PermissionType,
  getCurrentInfoDataProvider,
  currentDetailComponentConfig,
  partActiveTab,
  stableAdvancedFilterData,
  dbList,
  backSqlPart,
  viewTableDataLimit,
  treeNodeKey,
  addSqlCodeEvent
} from './utils';
import { getExplorerProps, getSqlProvider } from '../model/useExplorer';
// import { hasOwnProperty } from 'utils/validate';
import { cloneDeep } from 'lodash-es';
import { deleteStableReq, deleteTableReq, getStableStructReq, NORMAL_TABLE, VIRTUAL_NORMAL_TABLE } from '../../api';
import { instance } from 'config';

const props = defineProps<{
  node: Node;
  data: Recordable;
  defaultExpandedKeys: string[];
  stableTagFilterMap: Recordable;
}>();
const iconMap: Recordable = {
  database: 'database',
  stable: 'stable',
  table: 'table',
  column: 'circle-blod',
  tag: 'tag',
  dimension: 'dimension'
};
const { isCloud, isCommunity, database, customCompCallback } = getExplorerProps();
const { addSql, sqlStr } = getSqlProvider();
const currentData = getCurrentInfoDataProvider();
const showMoreBtnType = ['database', 'stable'];
const showTotalType = ['stable', 'dimension'];
const delFnMap: Recordable = {
  database: database.deleteApi,
  stable: deleteStableReq,
  table: deleteTableReq
};

const labelText = computed(() => props.node.label + getTotalText());
const namefilterText = ref('');
const tagFilter = ref('');
const requestIng = ref(false);
const type = computed(() => props.data.typeName);
const isVirtual = computed(() => props.data.isvirtual === true);
const key = computed(() => props.data[treeNodeKey]);
// let dataSourceUsedDbList: Recordable[] = [];
const emits = defineEmits([
  'nameFilter',
  'stableTagFilter',
  'update:defaultExpandedKeys',
  'updateTree',
  'advancedFilter'
]);

function getSubCreateText() {
  type == 'database' ? 'stb.stable' : isVirtual ? 'stb.virtualSubTable' : 'stb.subTable';
  switch (props.data.typeName) {
    case 'database':
      return t('stb.stable');
    case 'stable':
      if (props.data.stable_name == NORMAL_TABLE) {
        return t('stb.normalTable');
      } else if (props.data.stable_name == VIRTUAL_NORMAL_TABLE) {
        return t('stb.virtualNormalTable');
      }
      return isVirtual.value ? t('stb.virtualSubTable') : t('stb.subTable');
    case 'table':
    default:
      return '';
  }
}
// getDataSourceDbList();
function dbFilter() {
  emits('nameFilter', namefilterText.value, props.node);
}
function getTotalText() {
  if (!showTotalType.includes(type.value)) return '';
  return (
    '(' +
    (type.value == 'stable' &&
    ((props.stableTagFilterMap[key.value].advanced.enable && props.stableTagFilterMap[key.value].advanced.condition) ||
      props.stableTagFilterMap[key.value].name)
      ? !props.node.loaded || props.stableTagFilterMap[key.value].name
        ? props.data.total
        : props.node.total
      : props.data.total) +
    ')'
  );
}
function stableTagChange(value: string) {
  tagFilter.value = value;
  emits('stableTagFilter', value, props.data, props.node);
}
function isHasPermission(privilege: string | string[], type?: PermissionType | PermissionType[]) {
  if (!isCloud) return true;
  return checkPermission(privilege, props.node, type);
}

async function advancedFilter() {
  await setInfoComp();
  emits('advancedFilter', props.node, props.data);
  stableAdvancedFilterData.data = props.stableTagFilterMap[key.value].advanced;
  stableAdvancedFilterData.key = key.value;
  currentDetailComponentConfig.component = 'AdvancedFilter';
  currentDetailComponentConfig.name = t('explorer.stbAdvancedFilter', [props.data.name]);
  partActiveTab.value = 'detail';
}
async function clickAdd(all = false) {
  if (all) {
    const db = props.data.parent.split('.')[0];
    const sdata = await getStableStructReq(
      db,
      type.value == 'stable' ? props.data.name : props.data.parent.split('.')[1]
    ).catch(() => ({
      ts_field_name: '',
      columns: [] as any[],
      tags: []
    }));
    const columns = sdata.columns.map(item => `\`${item.field}\``);
    addSql(
      `${sqlStr.value ? '\n' : ''}SELECT ${columns.join(',') || '*'} FROM \`${db}\`.\`${props.data.name}\` limit ${viewTableDataLimit};`,
      true
    );
  } else {
    const code = props.data.parent
      ? `\`${props.data.parent.split('.')[0]}\`.\`${props.data.name}\``
      : `\`${props.data.name}\``;
    addSqlCodeEvent.emit(code);
  }
  partActiveTab.value = 'sql';
}
function getTooltip(data: Recordable, operate: string) {
  if (operate === 'add' && data.typeName === 'stable' && data.stable_name === NORMAL_TABLE) {
    return t('explorer.createNormalTable');
  }
  if (operate === 'add' && data.typeName === 'stable' && data.stable_name === VIRTUAL_NORMAL_TABLE) {
    return t('explorer.createVirtualNormalTable');
  }
  return (
    (
      {
        database: {
          add: t('stb.createStbInDb', [data.name]),
          edit: t('db.edit'),
          view: t('db.viewDatabase'),
          del: t('db.delete'),
          manage: t('db.managePrivilege'),
          log: t('db.operationLog'),
          moreOperations: t('common.moreOperations')
        },
        stable: {
          add: t('stb.createTableUse', [data.name]),
          edit: t('stb.editStable', [data.name]),
          view: t('stb.viewStable'),
          del: t('stb.delete'),
          manage: t('stb.managePrivilege'),
          moreOperations: t('common.moreOperations')
        },
        table: {
          edit: t('stb.editTable', [data.name]),
          view: t('stb.viewTable'),
          del: t('stb.delTb')
        }
      } as Recordable
    )[data.typeName]?.[operate] ?? ''
  );
}

// 处理全局db和stb
async function handleVar() {
  currentDetailComponentConfig.component = '';
  switch (type.value) {
    case 'database':
      //操作数据库时，获取数据库配置
      if (!isCloud || props.data?.privileges?.some((item: Recordable) => item.name == 'db:read')) {
        const name = props.data.name;
        Object.assign(props.data, await database.getStructApi(props.data.name));
        // eslint-disable-next-line vue/no-mutating-props
        props.data.name = name;
      }
      currentData.db = props.data;
      currentData.stb = {};
      currentData.tb = {};
      break;
    case 'stable':
      currentData.db = props.node.parent.data;
      currentData.stb = props.data;
      break;
    case 'table':
      // eslint-disable-next-line no-case-declarations
      let stbData = props.node.parent;
      while (stbData.data.typeName != 'stable') {
        stbData = stbData.parent;
      }
      currentData.db = stbData.parent.data;
      currentData.stb = stbData.data;
      currentData.tb = props.data;
      break;
    default:
      break;
  }
  const result = [];
  // 判断当前节点是否为展开状态,后续父节点不需要判断
  if (props.node.expanded) {
    result.push(key.value);
  }
  let currentNode = props.node.parent;
  while (currentNode && currentNode.level > 0) {
    result.push(currentNode.data[treeNodeKey]);
    currentNode = currentNode.parent;
  }
  // 处理默认展开的key
  emits('update:defaultExpandedKeys', result.reverse());
}
async function add() {
  await handleVar();
  switch (type.value) {
    case 'database':
      currentDetailComponentConfig.props = {
        version: instance.version,
        dbData: props.data,
        isEdit: false
      };
      currentDetailComponentConfig.component = 'StableCreate';
      currentDetailComponentConfig.listeners = {
        success: () => {
          emits('updateTree');
          backSqlPart();
        }
      };
      break;
    case 'stable':
      currentDetailComponentConfig.props = {
        version: instance.version,
        isEdit: false,
        isVirtual: props.data.isvirtual === true,
        stbName: props.data.name,
        dbName: currentData.db.name,
        dbData: currentData.db
      };
      switch (props.data.name) {
        case NORMAL_TABLE:
          currentDetailComponentConfig.component = 'NormalTableCreate';
          break;
        case VIRTUAL_NORMAL_TABLE:
          currentDetailComponentConfig.component = 'VirtualNormalTableCreate';
          break;
        default:
          currentDetailComponentConfig.component = 'TableCreate';
      }
      break;
    default:
      break;
  }
  currentDetailComponentConfig.name = t('common.add');
  currentDetailComponentConfig.listeners = {
    success: () => {
      emits('updateTree');
      backSqlPart();
    }
  };
  partActiveTab.value = 'detail';
}
async function setInfoComp() {
  await handleVar();
  currentData.type = ({ database: 'db', stable: 'stb', table: 'tb' } as Recordable)[type.value];
}
async function view() {
  await setInfoComp();
  currentDetailComponentConfig.component = 'Info';
  currentDetailComponentConfig.name = t(`explorer.${type.value}Info`);
  partActiveTab.value = 'detail';
}
async function manage() {
  await setInfoComp();
  const isDb = type.value == 'database';
  customCompCallback(isDb ? 'privilege' : 'stablePrivilege');
  currentDetailComponentConfig.component = type.value == 'database' ? 'DatabasePrivileges' : 'StablePrivileges';
  currentDetailComponentConfig.name = t(`explorer.${type.value}Control`, [props.data.name]);
  partActiveTab.value = 'detail';
}
async function viewLog() {
  await setInfoComp();
  customCompCallback('log');
  currentDetailComponentConfig.name = t(`explorer.logComponentName`, [props.data.name]);
  partActiveTab.value = 'detail';
}
async function edit() {
  await handleVar();
  switch (type.value) {
    case 'database':
      currentDetailComponentConfig.props = {
        formData: cloneDeep(props.data),
        isEdit: true,
        dbList: dbList.value,
        updateApi: database.updateApi,
        version: instance.version
      };
      currentDetailComponentConfig.component = 'DatabaseCreate';
      currentDetailComponentConfig.listeners = {
        success: () => {
          backSqlPart();
        }
      };
      break;
    case 'stable':
      currentDetailComponentConfig.props = {
        stbName: props.data.name,
        isEdit: true,
        dbData: currentData.db,
        version: instance.version
      };
      currentDetailComponentConfig.component = 'StableCreate';
      break;
    case 'table':
      currentDetailComponentConfig.props = {
        stbName: currentData.stb.name,
        tbName: props.data.name,
        isEdit: true,
        dbData: currentData.db
      };
      currentDetailComponentConfig.component = 'TableCreate';
      break;

    default:
      break;
  }
  currentDetailComponentConfig.name = t('common.edit');
  partActiveTab.value = 'detail';
}

async function del() {
  if (requestIng.value) return;
  if (!isCommunity) {
    const inUsing = await isDatasourceUsedDB();
    if (inUsing) return;
  }
  await handleVar();
  let msg = '';
  if (type.value == 'database') {
    if (props.data.databaseAccessType === 'PRIVATE' || props.data.databaseAccessType === 'PUBLIC') {
      msg = t('explorer.delDatabaseMsgDBMart', [props.data.name]);
    } else {
      msg = t('explorer.delDatabaseMsg', [props.data.name]);
    }
  } else {
    msg = t('msg.confirmTemp', {
      operate: t('common.delete').toLowerCase(),
      name: props.data.name
    });
  }
  const params: any[] = [];
  const delFn = delFnMap[type.value];
  switch (type.value) {
    case 'database': {
      params.push(props.data.name);
      break;
    }
    case 'stable':
      params.push({
        dbName: props.data.parent,
        stbName: props.data.name
      });
      break;
    case 'table':
      params.push({
        dbName: currentData.db.name,
        tbName: props.data.name
      });
      break;

    default:
      break;
  }
  ElMessageBox.confirm(msg, t('status.warning'), {
    confirmButtonText: t('common.confirm'),
    cancelButtonText: t('common.cancel'),
    type: 'warning'
  })
    .then(() => {
      requestIng.value = true;
      return delFn(...params);
    })
    .then(() => {
      t('msg.deleteSuccess');
      emits('updateTree');
    })
    .catch(err => {
      console.error(err);
      err.desc && ElMessage.error(err.desc);
    })
    .finally(() => {
      requestIng.value = false;
      partActiveTab.value = 'sql';
    });
}
// function getDataSourceDbList() {
//   database.getDataSourceUsedList().then(data => {
//     dataSourceUsedDbList = data;
//   });
// }
async function isDatasourceUsedDB() {
  if (type.value !== 'database') return false;
  const databaseInUsing: Recordable[] = await database.getDataSourceUsedList();
  const datasource = databaseInUsing.find(item => item.targetDB === props.data.name);
  if (!datasource) return false;
  ElMessageBox.alert(
    t('explorer.delDBUseingByDatasource', [props.data.name, datasource.name]),
    t('status.warning', {
      confirmButtonText: t('common.confirm'),
      type: 'warning'
    })
  );
  return true;
}
</script>

<style scoped lang="scss">
:deep(.tree-label-popper) {
  background: #409eff !important;
}

:deep(.el-tooltip__popper.tree-label-popper[x-placement^='right'] .popper__arrow),
:deep(.el-tooltip__popper[x-placement^='right'] .popper__arrow::after) {
  border-right-color: #409eff !important;
}

:deep(.operate-icon) {
  width: 14px;
  height: 14px;
}

.custom-node-wrapper {
  position: relative;
  display: flex;
  flex: 1;
  align-items: center;
  justify-content: space-between;
  width: 120px;

  .operate-icon + .operate-icon {
    margin-left: 10px;
  }

  .operate-btn-wrapper {
    position: absolute;
    right: 0;
    display: flex;
    align-items: center;
    height: 30px;
    padding: 0 8px;
    overflow-y: hidden;
    font-size: 12px;
    color: inherit;
    background-color: #fff;
    opacity: 0;

    &.show {
      opacity: 1;
    }
  }

  .column-type {
    flex-shrink: 0;
    padding: 0 10px;
    font-style: italic;
    color: #5961ff;
    text-transform: lowercase;
  }

  .node-icon {
    flex-shrink: 0;
    width: 18px;
    height: 18px;
    margin-right: 10px;
  }

  .custom-node-label {
    display: flex;
    align-items: center;
    font-family: Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
    line-height: 30px;
  }

  &:hover {
    .operate-btn-wrapper {
      background-color: #409eff;
      opacity: 1;
    }
  }
}

.is-current > div > .custom-node-wrapper {
  .operate-btn-wrapper {
    background-color: #409eff;
  }
}

:deep(.tree-menu) {
  padding: 0;

  .tree-menu-item {
    width: 100%;
    height: 30px;
    padding: 0 10px;
    font-size: 12px;

    .tree-menu-label {
      margin-left: 5px;
      line-height: 30px;
    }
  }
}

.stable-tag-dropdown {
  max-height: 80vh;
  overflow: auto;
}

.stable-tag-dropdown .el-dropdown-menu__item.stable-tag-dropdown-item {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 5px;

  .selected-icon {
    position: absolute;
    left: 2px;
    width: 12px;
    height: 12px;

    &.active {
      color: #409eff;
    }
  }

  .clear-icon {
    position: absolute;
    right: 2px;
    width: 12px;
    height: 12px;
    color: #909399;
  }
}
</style>
