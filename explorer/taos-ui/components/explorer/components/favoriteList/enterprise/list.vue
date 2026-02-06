<template>
  <el-table
    style="margin-top: 20px"
    :data="listData"
    size="small"
    row-key="id"
    max-height="calc(100% - 80px)"
    @cell-click="selectSQL"
  >
    <el-table-column label="SQL" prop="sql" min-width="180">
      <template #default="scope">
        <el-tooltip placement="left-start" :open-delay="1000" effect="light">
          <template #content>
            <span>
              <pre v-highlight.noCopy class="my-popper sql-code pre-code">
                <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
              </pre>
            </span>
          </template>
          <span>{{ scope.row.sql }}</span>
        </el-tooltip>
      </template>
    </el-table-column>
    <el-table-column :label="t('explorer.desc')" prop="description" width="300"> </el-table-column>
    <el-table-column v-if="isShared" :label="t('explorer.user')" prop="username" width="120" show-overflow-tooltip>
    </el-table-column>
    <el-table-column width="30" fixed="right">
      <template #default="scope">
        <el-dropdown :data="scope.row" :trigger="'hover'">
          <el-button icon="MoreFilled" size="small" class="rotate-90!" text></el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="copy" class="tree-menu">
                <el-tooltip :content="t('common.copy')" placement="top" effect="light">
                  <div class="flex-start tree-menu-item" @click="copySql(scope.row)">
                    <CopyDocument class="operate-icon"></CopyDocument>
                    <div class="tree-menu-label">{{ t('common.copy') }}</div>
                  </div>
                </el-tooltip>
              </el-dropdown-item>
              <el-dropdown-item command="exec" class="tree-menu">
                <el-tooltip :content="t('common.run')" placement="top" effect="light">
                  <div class="flex-start tree-menu-item" @click="exec(scope.row)">
                    <VideoPlay class="operate-icon"></VideoPlay>
                    <div class="tree-menu-label">{{ t('common.run') }}</div>
                  </div>
                </el-tooltip>
              </el-dropdown-item>
              <template v-if="!isShared">
                <el-dropdown-item command="edit" class="tree-menu">
                  <el-tooltip :content="t('common.edit')" placement="top" effect="light">
                    <div class="flex-start tree-menu-item" @click="edit(scope.row)">
                      <Edit class="operate-icon"></Edit>
                      <div class="tree-menu-label">{{ t('common.edit') }}</div>
                    </div>
                  </el-tooltip>
                </el-dropdown-item>
                <el-dropdown-item v-if="!scope.row.is_public" command="share" class="tree-menu">
                  <el-tooltip v-if="!scope.row.is_public" :content="t('explorer.share')" placement="top" effect="light">
                    <div class="flex-start tree-menu-item" @click="manage(scope.row)">
                      <Share class="operate-icon"></Share>
                      <div class="tree-menu-label">{{ t('explorer.share') }}</div>
                    </div>
                  </el-tooltip>
                </el-dropdown-item>
                <el-dropdown-item v-if="scope.row.is_public" command="unshare" class="tree-menu">
                  <el-tooltip v-if="scope.row.is_public" :content="t('explorer.unshare')" placement="top" effect="light">
                    <div class="flex-start tree-menu-item" @click="manage(scope.row)">
                      <RefreshLeft class="operate-icon"></RefreshLeft>
                      <div class="tree-menu-label">{{ t('explorer.unshare') }}</div>
                    </div>
                  </el-tooltip>
                </el-dropdown-item>
              </template>
              <el-dropdown-item v-if="isShared" command="add" class="tree-menu">
                <el-tooltip v-if="isShared" :content="t('explorer.addToPersonal')" placement="top" effect="light">
                  <div class="flex-start tree-menu-item" @click="add(scope.row)">
                    <Star class="operate-icon"></Star>
                    <div class="tree-menu-label">{{ t('explorer.addToPersonal') }}</div>
                  </div>
                </el-tooltip>
              </el-dropdown-item>
              <el-dropdown-item command="delete" class="tree-menu">
                <el-tooltip :content="t('common.delete')" placement="top" effect="light">
                  <div class="flex-start tree-menu-item" @click="del(scope.row)">
                    <Delete class="operate-icon"></Delete>
                    <div class="tree-menu-label">{{ t('common.delete') }}</div>
                  </div>
                </el-tooltip>
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </template>
    </el-table-column>
  </el-table>
  <el-pagination
    v-if="total > 10"
    v-model:current-page="favoriteParams.page"
    class="pagination"
    layout="sizes, total, prev, pager, next"
    :page-sizes="[10, 20, 50, 100, 200]"
    :page-size="favoriteParams.page_size"
    :hide-on-single-page="false"
    :total="total"
    @size-change="handleSizeChange"
    @current-change="handlePageChange"
  ></el-pagination>
</template>
<script setup lang="ts">
// import { instance } from 'config';
// import { instance } from 'config';
import { getSqlProvider } from '../../../model/useExplorer';
import { favoriteParams, partActiveTab } from '../../utils';
import { ElMessageBox, ElMessage } from 'element-plus';
import { t } from 'locales';
import { VideoPlay, Edit, Share, RefreshLeft, Star, Delete, CopyDocument } from '@element-plus/icons-vue';

const emits = defineEmits(['update']);
const { addSql, executeSql } = getSqlProvider();

interface Props {
  isShared?: boolean;
  requestApi: {
    del?: (id: string) => Promise<any>;
    edit?: (id: number, data: Recordable) => Promise<any>;
    addShared?: (sql: string | Recordable) => Promise<any>;
  };
  listData: Recordable[];
  total: number;
}
const props = withDefaults(defineProps<Props>(), {
  listData: () => [],
  isShared: false
});

// 将别人共享的 SQL 添加到自己的空间下
async function add(row: Recordable) {
  const params = {
    sql: row.sql,
    description: row.description
  };
  props.requestApi.addShared!(params).then(() => {
    ElMessage.success(t('msg.operateSuccess'));
    emits('update');
  });
}
async function manage(row: Recordable) {
  const { id, is_public } = row;
  props.requestApi.edit!(id, { public: !is_public }).then(() => {
    ElMessage.success(t('msg.operateSuccess'));
    emits('update');
  });
}

function edit(row: Recordable) {
  ElMessageBox.prompt('', t('explorer.editDesc'), {
    closeOnClickModal: false,
    confirmButtonText: t('common.confirm'),
    cancelButtonText: t('common.cancel'),
    inputPattern: /^.{0,20}$/,
    inputErrorMessage: t('explorer.characterLen', ['20']),
    inputPlaceholder: t('explorer.descPlaceholder', ['20'])
  })
    .then(({ value }) => {
      const params = {
        description: value
      };
      props.requestApi.edit!(row.id, params)
        .then(() => {
          ElMessage.success(t('msg.changeSuccess'));
          emits('update');
        })
        .catch(() => {});
    })
    .catch(err => {
      console.log(err);
    });
}
async function del(row: Recordable) {
  props.requestApi.del!(row.id).then(() => {
    ElMessage.success(t('msg.deleteSuccess'));
    emits('update');
  });
}
// 点击将 sql 添加到窗口中
function selectSQL(row: Recordable, column: Recordable) {
  if (column.property === 'sql') {
    addSql('\n' + row.sql, true);
  }
}

function exec(row: Recordable) {
  console.log('=============exec sql:', row.sql);
  executeSql(row.sql);
  addSql('\n' + row.sql, true);
  partActiveTab.value = 'sql';
}

function handleSizeChange(val: number) {
  favoriteParams.page_size = val;
  emits('update');
}
function handlePageChange() {
  emits('update');
}

function copySql(row: Recordable) {
  navigator.clipboard.writeText(row.sql).then(() => {
    ElMessage.success(t('msg.copySuccess'));
  }).catch(() => {
    ElMessage.error(t('msg.copyFailed'));
  });
}

// function handleCommand(command: string, data: Recordable) {
//   switch (command) {
//     case 'exec':
//       exec(data);
//       break;
//     case 'edit':
//       edit(data);
//       break;
//     case 'share':
//     case 'unshare':
//       manage(data);
//       break;
//     case 'add':
//       add(data);
//       break;
//     case 'delete':
//       del(data);
//       break;
//   }
// }
</script>

<style scoped lang="scss">
.my-popper {
  max-width: 600px;
  max-height: 600px;
  overflow: auto;
  white-space: wrap;
}

:deep(.el-dropdown-menu__item) {
    padding: 1px 5px;
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
</style>
