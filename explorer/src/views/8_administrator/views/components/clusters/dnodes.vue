<template>
  <title-bar :show-add="true" :name="$t('taoscluster.dnodes')" @add="openDialog(ruleFormRef)"></title-bar>
  <div class="dnode-block">
    <el-table :data="dnodesList" size="small">
      <el-table-column width="400" :label="$t('taoscluster.endpoint')" prop="endpoint"></el-table-column>
      <el-table-column :label="$t('taoscluster.vnodes')" prop="vnodes"></el-table-column>
      <el-table-column :label="$t('taoscluster.supportvnodes')" prop="support_vnodes"></el-table-column>
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
      :title="$t('taoscluster.adddnodes')"
      width="600px"
      :destroy-on-close="true"
      :close-on-click-modal="false"
      @close="closeDialog(ruleFormRef)"
    >
      <el-form
        ref="ruleFormRef"
        :model="ruleForm"
        :rules="rules"
        label-width="auto"
        class="demo-ruleForm"
        @submit.prevent
      >
        <el-form-item :label="$t('taoscluster.endpoint')" prop="endpoint" required>
          <el-input v-model.trim="ruleForm.endpoint" @keyup.enter="addDnodes(ruleFormRef)"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="small" class="w100" @click="dialog = false">{{ $t('cancel') }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button size="small" class="w100" type="primary" @click="addDnodes(ruleFormRef)">{{
            $t('confirm')
          }}</el-button>
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

const { dialog, ruleForm, currentPage, pageSize, total, openDialog, closeDialog } = useCluster();
const emit = defineEmits(['sendData']);
const globalCustomProperties: any = inject('globalCustomProperties');
const { $error } = globalCustomProperties;
const { t } = useI18n();
const ruleFormRef = ref<FormInstance>();

const username = localStorage.getItem('username');
const isDisable = ref(username === 'root');
const dnodesList = ref([]);
const rules = reactive({
  endpoint: [
    {
      required: true,
      message: t('taoscluster.endpointRequired')
    }
  ]
});

function handlePageChange() {}
async function getAllDnodes() {
  try {
    return await sendSQLReq(`select * from information_schema.ins_dnodes;`).then((res: any) => {
      dnodesList.value = res.data.map((data: any) => {
        return Object.fromEntries(
          res.column_meta.map((item: any[], index: string | number) => {
            return [item[0], data[index]];
          })
        );
      });
      emit('sendData', dnodesList.value);
    });
  } catch (error) {
    console.log(error);
  }
}
function del(data: { endpoint: string; id: string | number }) {
  ElMessageBox.confirm(t('isDel', [data.endpoint]), t('warning'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  })
    .then(() => {
      try {
        sendSQLReq(`drop dnode ${data.id}`)
          .then((res: { code: number | string }) => {
            if (res.code == 0) {
              ElMessage.success(t('delSucc'));
              getAllDnodes();
            }
          })
          .catch((err: { desc: any }) => {
            err.desc && $error(err.desc);
          });
      } catch (error) {
        console.log(error, '删除');
      }
    })
    .catch(() => {});
}
async function addDnodes(formEl: FormInstance | undefined) {
  try {
    if (!formEl) return;
    formEl.validate(async (valid: boolean) => {
      if (valid) {
        return await sendSQLReq(`create dnode \`${ruleForm.endpoint}\`;`).then((res: { code: number | string }) => {
          if (res.code == 0) {
            getAllDnodes();
            dialog.value = false;
          }
        });
      }
    });
  } catch (err: any) {
    $error(err?.desc);
    return Promise.reject(err);
  }
}

getAllDnodes();
</script>
<style lang="scss" scoped>
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

.dnode-block {
  overflow: auto;
  margin-bottom: 30px;
}
</style>
