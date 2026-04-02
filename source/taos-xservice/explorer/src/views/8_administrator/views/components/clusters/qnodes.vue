<template>
  <div class="qnode-block">
    <title-bar :show-add="true" :name="$t('taoscluster.qnodes')" @add="openDialog(ruleFormRef)"></title-bar>
    <el-table :data="qnodesList" size="small">
      <el-table-column :label="$t('taoscluster.endpoint')" prop="endpoint"></el-table-column>
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
      :title="$t('taoscluster.addqnodes')"
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
        <el-form-item label="DNodes" prop="DNodes" required>
          <el-select v-model="ruleForm.DNodes" placeholder="" style="width: 100%">
            <el-option v-for="item in dnodes" :key="item.id" :label="item.endpoint" :value="item.id"></el-option>
          </el-select>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button class="w100" @click="dialog = false">
            {{ $t('cancel') }}
          </el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button class="w100" type="primary" @click="addQnodes(ruleFormRef)">{{ $t('confirm') }}</el-button>
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

defineProps({
  dnodes: {
    type: Array,
    default: () => {
      return [];
    }
  }
});
const qnodesList = ref([]);

async function getAllQnodes() {
  try {
    return await sendSQLReq(`select * from information_schema.ins_qnodes;`).then((res: any) => {
      qnodesList.value = res.data.map((data: { [x: string]: any }) => {
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
    sendSQLReq(`drop qnode on dnode ${data.id};`).then((res: { code: number | string }) => {
      if (res.code == 0) {
        ElMessage.success(t('delSucc'));
        getAllQnodes();
      }
    });
  });
}
async function addQnodes(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (valid) {
      try {
        return await sendSQLReq(`create qnode  on dnode ${ruleForm.DNodes};`).then((res: { code: number | string }) => {
          if (res.code == 0) {
            getAllQnodes();
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

getAllQnodes();
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

.qnode-block {
  margin-bottom: 30px;
  overflow: auto;
}
</style>
