<template>
  <div class="xnode-block">
    <title-bar :show-add="true" :name="$t('taoscluster.xnodes')" @add="dialog = true"></title-bar>
    <el-table :data="pagedXnodesList" size="small">
      <el-table-column :min-width="CLUSTER_TABLE_WIDTHS.endpoint" :label="$t('taoscluster.endpoint')" prop="endpoint"></el-table-column>
      <el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionA" />
      <el-table-column :min-width="CLUSTER_TABLE_WIDTHS.extensionB" />
      <el-table-column :min-width="CLUSTER_TABLE_WIDTHS.status" :label="$t('taoscluster.status')" prop="status"></el-table-column>
      <el-table-column
        :min-width="CLUSTER_TABLE_WIDTHS.createTime"
        :label="$t('taoscluster.createtime')"
        prop="create_time"
      ></el-table-column>
      <el-table-column :min-width="CLUSTER_TABLE_WIDTHS.action" :label="$t('taoscluster.action')" align="right" header-align="right">
        <template #default="scope">
          <el-button plain size="small" icon="Delete" :disabled="!isDisable" @click="del(scope.row)"></el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-model:current-page="currentPage"
      class="pagination"
      layout="total, prev, pager, next"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
    >
    </el-pagination>

    <AddXnodeDialog v-model="dialog" :send-sql="createXnode" @success="getAllXnodes" />
  </div>
</template>

<script setup lang="ts">
import AddXnodeDialog from '@/components/xnode/AddXnodeDialog.vue';
import { buildDropXnodeSql, normalizeXnodeRows, type XnodeRow } from '@/components/xnode/xnodeDialog.helper';
import { sendSQLReq } from '@/api/explorer';
import titleBar from '../title-bar.vue';
import { CLUSTER_TABLE_WIDTHS } from './clusterTableColumns';
import useCluster from './useCluster';

const { dialog, currentPage, pageSize, total, isDisable } = useCluster();
const { t } = useI18n();
const xnodesList = ref<XnodeRow[]>([]);
const pagedXnodesList = computed(() => {
  const start = (currentPage.value - 1) * pageSize.value;
  return xnodesList.value.slice(start, start + pageSize.value);
});

async function getAllXnodes() {
  try {
    const result = await sendSQLReq('show xnodes;');
    xnodesList.value = normalizeXnodeRows(result);
    total.value = xnodesList.value.length;
    const lastPage = Math.max(1, Math.ceil(total.value / pageSize.value));
    if (currentPage.value > lastPage) {
      currentPage.value = lastPage;
    }
  } catch (error) {
    console.log(error);
  }
}

function createXnode(sql: string) {
  return sendSQLReq(sql);
}

function del(data: { endpoint?: string; id?: number | string }) {
  ElMessageBox.confirm(t('isDel', [data.endpoint || '']), t('warning'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  }).then(() => {
    let sql = '';
    try {
      sql = buildDropXnodeSql(data.id);
    } catch (_error) {
      ElMessage.error(t('taoscluster.invalidXnodeId'));
      return;
    }

    sendSQLReq(sql)
      .then((res: { code: number | string }) => {
        if (res.code == 0) {
          ElMessage.success(t('delSucc'));
          getAllXnodes();
        }
      })
      .catch((err: { desc?: string }) => {
        err.desc && ElMessage.error(err.desc);
      });
  }).catch(() => {});
}

getAllXnodes();
</script>

<style lang="scss" scoped>
.xnode-block {
  margin-bottom: 30px;
  overflow: auto;
}
</style>
