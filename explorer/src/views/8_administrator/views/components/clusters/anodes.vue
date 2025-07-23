<template>
  <div class="node-block">
    <title-bar :show-add="true" :name="'ANodes'" @add="openDialog(ruleFormRef)"></title-bar>
    <el-table :data="nodesList" size="small">
      <el-table-column :label="$t('taoscluster.endpoint')" prop="url"></el-table-column>
      <el-table-column :label="$t('taoscluster.status')" prop="status"></el-table-column>
      <el-table-column :label="$t('taoscluster.createtime')" prop="create_time" width="240"></el-table-column>

      <el-table-column :label="$t('taoscluster.action')" width="65">
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
      @current-change="handlePageChange"
    >
    </el-pagination>

    <el-dialog
      v-model="dialog"
      align="center"
      :title="$t('taoscluster.addanodes')"
      width="600px"
      :destroy-on-close="true"
      :close-on-click-modal="false"
      @close="closeDialog(ruleFormRef)"
    >
      <el-form
        ref="ruleFormRef"
        :model="ruleForm"
        :rules="rules"
        size="default"
        label-width="auto"
        class="demo-ruleForm"
      >
        <el-form-item label="Endpoint" prop="endpoint" required>
          <el-input v-model.trim="ruleForm.endpoint" @keyup.enter="addNodes(ruleFormRef)"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button class="w100" @click="dialog = false">
            {{ $t('cancel') }}
          </el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button class="w100" type="primary" @click="addNodes(ruleFormRef)">{{ $t('confirm') }}</el-button>
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import titleBar from '../title-bar.vue';
import { sendSQLReq } from '@/api/explorer';
import { FormInstance } from 'element-plus';
import useCluster from './useCluster';

const ruleFormRef = ref<FormInstance>();
const { dialog, ruleForm, rules, currentPage, pageSize, total, isDisable, openDialog, closeDialog } = useCluster();
const { t } = useI18n();
const globalCustomProperties: any = inject('globalCustomProperties');
const { $error } = globalCustomProperties;

const nodesList = ref([]);

async function getAllNodes() {
  try {
    return await sendSQLReq(`select * from information_schema.ins_anodes;`).then((res: any) => {
      nodesList.value = res.data.map((data: { [x: string]: any }) => {
        return Object.fromEntries(
          res.column_meta.map((item: any[], index: string | number) => {
            return [item[0], data[index]];
          })
        );
      });
    });
  } catch (error) {
    console.log(error);
  }
}
function handlePageChange() {}

function del(data: { endpoint: string; id: number | string }) {
  ElMessageBox.confirm(t('isDel', [data.endpoint]), t('warning'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  }).then(() => {
    sendSQLReq(`drop anode ${data.id};`).then((res: { code: number | string }) => {
      if (res.code == 0) {
        ElMessage.success(t('delSucc'));
        getAllNodes();
      }
    });
  });
}
async function addNodes(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (valid) {

      try {
        return await sendSQLReq(`create anode '${ruleForm.endpoint}';`).then((res: { code: number | string }) => {
          if (res.code == 0) {
            getAllNodes();
            dialog.value = false;
          }
        });
      } catch (err: any) {
        $error(err?.desc);
        return Promise.reject(err);
      }
    }
  });
}

getAllNodes();
</script>
<style lang="scss" scoped>
:deep(.el-form-item__content) {
  display: flex;
}

:deep(.el-select) {
  flex: 1;
  width: 100%;
}

.flex-between {
  position: absolute;
  top: 15px;
  right: 10px;
  z-index: 9999;

  .el-button {
    background: transparent;
    border: none;
  }
}

.node-block {
  margin-bottom: 30px;
  overflow: auto;
}
</style>
