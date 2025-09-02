<template>
  <section class="dbs-tree">
    <div class="dbs-tree-header">
      <div class="flex-center">
        <Icon name="database" class="database-icon mr-10px"></Icon>
        <span class="title">{{ t('common.databases') }}</span>
      </div>
      <div>
        <el-tooltip effect="light" placement="top" :content="t('explorer.refreshDatabaseList')">
          <el-button icon="refresh" size="small" @click="refersh"></el-button>
        </el-tooltip>
        <el-tooltip
          v-if="explorerProps.database.isCanCreateDatabase"
          effect="light"
          placement="top"
          :content="t('explorer.createDatabase')"
        >
          <el-button size="small" icon="plus" plain @click="addDatabase"></el-button>
        </el-tooltip>
      </div>
    </div>
    <div class="dbs-tree-container">
      <Tree
        :key="treeKey"
        lazy
        :empty-text="t('explorer.noDatabase')"
        highlight-current
        node-key="node-key"
        :load="loadNode"
        :props="treeProps"
        :height="height"
        :default-expanded-keys="defaultExpandedKeys"
        :filter-node-method="nodeFilter"
        @node-collapse="expandChange"
        @node-expand="expandChange"
        @all-nodes-loaded="allNodesLoaded"
      >
        <template #default="{ node, data }">
          <CustomTreeNode
            v-model:default-expanded-keys="defaultExpandedKeys"
            :node="node"
            :data="data"
            :stable-tag-filter-map="stableTagFilterMap"
            @name-filter="nameFilter"
            @stable-tag-filter="stableTagFilter"
            @update-tree="refersh"
            @advanced-filter="advancedFilter"
          />
        </template>
      </Tree>
    </div>
  </section>
</template>

<script lang="ts" setup>
import Tree from '../../tree/index';
import { getExplorerProps } from '../model/useExplorer';
import {
  useTreeHeight,
  applyStbAdvancedEvent,
  currentDetailComponentConfig,
  partActiveTab,
  dbList,
  backSqlPart,
  viewTableDataLimit
} from './utils';
import Node from '../../tree/src/model/node';
import type { LoadedCallback } from '../../tree/src/tree.type';
import { t } from 'locales';
import CustomTreeNode from './customTreeNode.vue';
import {
  getStableListReq,
  getTagHierachy,
  getTableWithTags,
  getTableListReq,
  getSubtbTagAndColumnList,
  getDbList
} from '../../api';
import { getSqlProvider } from '../model/useExplorer';
import { instance } from 'config';

const explorerProps = getExplorerProps();
const treeKey = inject('treeKey', ref(0));
const height = useTreeHeight();
const defaultExpandedKeys = shallowRef<string[]>([]);
let filterTextMap: Recordable = {};
const stableTagFilterMap = reactive<Recordable>({});
const stableAdvancedFilterNodeMap: Recordable = {};
const { executeSql } = getSqlProvider();

const treeProps = {
  label: 'name',
  children: 'children',
  isLeaf: 'leaf'
};

applyStbAdvancedEvent.on(({ data, key }) => {
  if (data.enable) {
    stableTagFilterMap[key].name = '';
  }
  stableTagFilterMap[key].advanced = data;
  const node = stableAdvancedFilterNodeMap[key];
  if (node.loaded) {
    node.loaded = false;
  }
  node.expand();
});
function refersh() {
  treeKey.value++;
}
function addDatabase() {
  currentDetailComponentConfig.name = t('common.add');
  currentDetailComponentConfig.component = 'DatabaseCreate';
  currentDetailComponentConfig.props = {
    formData: undefined,
    isEdit: false,
    isHa: instance.ha,
    updateApi: explorerProps.database.createApi,
    version: instance.version,
    dbList: dbList.value
  };
  currentDetailComponentConfig.listeners = {
    success: () => {
      refersh();
      backSqlPart();
    }
  };
  partActiveTab.value = 'detail';
}
async function loadNode(node: Node, resolve: LoadedCallback) {
  const data = node.data;
  switch (node.data?.typeName) {
    case 'database':
      // eslint-disable-next-line no-case-declarations
      const result = await getStableListReq(data.name, explorerProps.stable.getPermissionList);
      result[0].forEach(stable => {
        if (stableTagFilterMap[stable['node-key']]) return;
        stableTagFilterMap[stable['node-key']] = {
          parent: data.name,
          name: '',
          type: '',
          advanced: {
            enable: false,
            conditionJson: [],
            sql: '',
            condition: '',
            type: '0'
          }
        };
      });
      resolve(...result);
      break;
    case 'stable':
      // eslint-disable-next-line no-case-declarations
      const currentStbFilter = stableTagFilterMap[data['node-key']];
      if (currentStbFilter.name) {
        return resolve(...(await getTagHierachy(data.parent, data.name, currentStbFilter.name, currentStbFilter.type)));
      } else {
        // eslint-disable-next-line no-case-declarations
        let conditions = '';
        if (currentStbFilter.advanced.enable) {
          conditions =
            currentStbFilter.advanced.type == '0' ? currentStbFilter.advanced.condition : currentStbFilter.advanced.sql;
          return resolve(
            ...(await getTableWithTags({
              stbName: data.name,
              dbName: data.parent,
              conditions,
              pageSize: node.pageSize,
              currentPage: node.currentPage
            }))
          );
        }
        return resolve(
          ...(await getTableListReq({
            stbName: data.name,
            pageSize: node.pageSize,
            currentPage: node.currentPage,
            dbName: data.parent,
            filter: filterTextMap[data.parent]
          }))
        );
      }
    case 'dimension':
      // eslint-disable-next-line no-case-declarations
      const parts = data['node-key'].split(":");
      // eslint-disable-next-line no-case-declarations
      const tagName = parts[3];
      if (data.total > 0 && data.children) return resolve(data.children);
      // eslint-disable-next-line no-case-declarations
      let parentStb = node.parent;
      // eslint-disable-next-line no-case-declarations
      let tagValue = data.name;
      while (parentStb.data.typeName != 'stable') {
        tagValue = parentStb.data.name + '.' + tagValue;
        parentStb = parentStb.parent;
      }
      return resolve(
        ...(await getTableWithTags({
          stbName: parentStb.data.name,
          pageSize: node.pageSize,
          currentPage: node.currentPage,
          dbName: parentStb.data.parent,
          tag_value: tagValue,
          tagName: tagName,
          filter: filterTextMap[data.parent]
        }))
      );
    case 'table':
      return resolve(await getSubtbTagAndColumnList(data.parent.split('.')[0], data.name));
    default:
      // eslint-disable-next-line no-case-declarations
      const dataList = await getDbList();
      filterTextMap = dataList.reduce((acc, cur) => {
        acc[cur.name] = '';
        return acc;
      }, {});

      clearStbFilterData();
      dbList.value = dataList;
      return resolve(dataList);
  }
}
function clearStbFilterData() {
  Object.keys(stableTagFilterMap).forEach(key => {
    delete stableTagFilterMap[key];
  });
  Object.keys(stableAdvancedFilterNodeMap).forEach(key => {
    delete stableAdvancedFilterNodeMap[key];
  });
}
function nodeFilter(val: string, data: Recordable) {
  if (data.typeName == 'database') return true;
  return data.name.includes(val);
}
function nameFilter(name: string, node: Node) {
  filterTextMap[node.label] = name;
  node.expandAllChildren = true;
  node.loaded = false;
  node.expand();
}
function stableTagFilter(value: string, data: Recordable, node: Node) {
  const key = data['node-key'];
  if (!key) return;
  if (stableTagFilterMap[key]?.name != value) {
    stableTagFilterMap[key].name = value;
    filterTextMap[data.parent] = '';
    const currentStb = stableTagFilterMap[key];
    currentStb.type = data.tags.find((item: Recordable) => item.tag_name == value)?.tag_type ?? '';
    currentStb.advanced.enable = false;
  } else {
    stableTagFilterMap[key].name = '';
  }
  node.loaded = false;
  node.expand(() => {}, false);
}

function advancedFilter(node: Node, data: Recordable) {
  stableAdvancedFilterNodeMap[data['node-key']] = node;
}
function expandChange(data: Recordable) {
  if (data.typeName == 'table') {
    executeSql(
      `select * from \`${data.parent.split('.')[0]}\`.\`${data.name}\` order by _C0 desc limit ${viewTableDataLimit}`
    );
  }
}
function allNodesLoaded(node: Node) {
  if (node.level == 1) {
    node.filter(filterTextMap[node.data.name]);
  }
}
</script>

<style scoped lang="scss">
.dbs-tree {
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  width: 19%;
  min-width: 360px;
  height: 100%;
  overflow: hidden;
  border: 1px solid #dcdfe6;

  &:deep(.el-tree-node__content) {
    height: 30px;
  }

  &-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    padding: 4px 10px;
    background-color: #f5f7fa;
    border-bottom: 1px solid #ebeef5;

    .title {
      font-size: 14px;
      font-weight: 500;
      color: #303133;
    }
  }
}

.database-icon {
  flex-shrink: 0;
  width: 18px;
  height: 18px;
}

.dbs-tree-container {
  flex: 1;
  padding: 5px 0;
  overflow: auto;

  &:deep(.el-tree-node__expand-icon) {
    color: inherit;
  }

  &:deep(.el-tree--highlight-current .el-tree-node.is-current > .el-tree-node__content),
  &:deep(.el-tree-node:focus > .el-tree-node__content),
  &:deep(.el-tree-node__content:hover) {
    color: #fff;
    background-color: #409eff !important;

    .more-btn {
      color: #fff;
    }

    .operate-btn {
      background-color: #409eff;
    }
  }
}
</style>
