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
          <TextCopy :text="scope.row.sql" :is-show-btn-text="true"></TextCopy>
        </el-tooltip>
      </template>
    </el-table-column>
    <el-table-column :label="t('explorer.desc')" prop="description" width="310"> </el-table-column>
    <el-table-column v-if="isShared" :label="t('explorer.user')" prop="username" width="120" show-overflow-tooltip>
    </el-table-column>
    <el-table-column :label="t('common.action')" width="150">
      <template #default="scope">
        <template v-if="!isShared">
          <el-tooltip :content="t('common.edit')" placement="top" effect="light">
            <el-button size="small" icon="Edit" @click="edit(scope.row)"></el-button>
          </el-tooltip>
          <el-tooltip v-if="!scope.row.is_public" :content="t('explorer.share')" placement="top" effect="light">
            <el-button size="small" icon="Share" @click="manage(scope.row)"></el-button>
          </el-tooltip>
          <el-tooltip v-if="scope.row.is_public" :content="t('explorer.unshare')" placement="top" effect="light">
            <el-button size="small" icon="RefreshLeft" @click="manage(scope.row)"></el-button>
          </el-tooltip>
        </template>
        <el-tooltip v-if="isShared" :content="t('explorer.addToPersonal')" placement="top" effect="light">
          <el-button
            :disabled="scope.row.username == instance.user"
            size="small"
            icon="Star"
            @click="add(scope.row)"
          ></el-button>
        </el-tooltip>
        <el-tooltip :content="t('common.delete')" placement="top" effect="light">
          <el-button plain size="small" icon="Delete" @click="del(scope.row)"></el-button>
        </el-tooltip>
      </template>
    </el-table-column>
  </el-table>
  <el-pagination
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
import { instance } from 'config';
import { getSqlProvider } from '../../../model/useExplorer';
import { favoriteParams } from '../../utils';
import { ElMessageBox, ElMessage } from 'element-plus';
import { t } from 'locales';

const emits = defineEmits(['update']);
const { addSql } = getSqlProvider();

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

function handleSizeChange(val: number) {
  favoriteParams.page_size = val;
  emits('update');
}
function handlePageChange() {
  emits('update');
}
</script>
<style scoped lang="scss">
.my-popper {
  max-width: 600px;
  max-height: 600px;
  overflow: auto;
  white-space: wrap;
}
</style>
