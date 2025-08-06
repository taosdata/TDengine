<template>
  <div class="dnode-block">
    <div class="flex-end">
      <el-button
        plain
        type="primary"
        size="default"
        icon="Refresh"
        :disabled="refreshable || $IS_COMMUNITY"
        style="font-size: 14px"
        @click="refresh"
      >
        {{ $t('refresh') }}
      </el-button>
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!$IS_COMMUNITY">
        <template #content>
          <span v-dompurify-html="$t('communityTip')"></span>
        </template>
        <el-button
          plain
          type="primary"
          size="default"
          icon="Plus"
          style="font-size: 14px"
          :disabled="$IS_COMMUNITY"
          @click="add"
          >{{ $t('taosuser.addreplication') }}</el-button
        >
      </el-tooltip>
    </div>
    <el-table style="margin-top: 20px" :data="topicList" size="small">
      <el-table-column label="ID" width="60" prop="id">
        <template #default="scope">
          <el-tooltip :content="String(scope.row.id)" placement="top-start">
            <span class="nowrap">{{ scope.row.id }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.fromdb')" prop="fromdb" width="120">
        <template #default="scope">
          <el-tooltip :content="scope.row.fromdb" placement="top-start">
            <span class="nowrap">{{ scope.row.fromdb }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.toinstance')" prop="hostport" min-width="140">
        <template #default="scope">
          <el-tooltip :content="scope.row.hostport" placement="top-start">
            <copy-text :text="scope.row.hostport" is-show-btn-text></copy-text>
          </el-tooltip>
          <!-- {{ scope.row.hostport }} -->
        </template>
      </el-table-column>
      <!-- <el-table-column :label="$t('taosuser.todb')" prop="db" show-overflow-tooltip></el-table-column> -->

      <el-table-column :label="$t('taosuser.status')" prop="status" width="80">
        <template #default="scope">
          <el-tooltip :content="scope.row.status" placement="top-start">
            <span class="nowrap">{{ handleDSStatus(scope.row.status) }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.reason')" prop="reason">
        <template #default="scope">
          <el-tooltip :content="scope.row.reason" placement="top-start">
            <span class="nowrap">{{ scope.row.reason }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.finishat')" prop="finished_at" show-overflow-tooltip>
        <template #default="scope">
          <span>{{ parsinginZone(scope.row.finished_at) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.createat')" prop="created_at" show-overflow-tooltip>
        <template #default="scope">
          <span>{{ parsinginZone(scope.row.created_at) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.operation')" width="110">
        <template #default="scope">
          <el-switch
            :model-value="!['stopping', 'stopped'].includes(scope.row.status.toLowerCase())"
            style="--el-switch-on-color: #13ce66; --el-switch-off-color: #dcdfe6"
            :disabled="$IS_COMMUNITY"
            @change="switchOperation($event, scope.row)"
          ></el-switch>
          <el-button
            plain
            size="small"
            icon="Delete"
            :disabled="$IS_COMMUNITY"
            @click="del(scope.row, scope.$index)"
          ></el-button>
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
    ></el-pagination>
    <el-dialog
      v-model="dialog"
      align="center"
      :title="$t('taosuser.addreplication')"
      width="600px"
      :destroy-on-close="true"
      :close-on-click-modal="false"
      @close="closeDialog(ruleFormRef)"
    >
      <el-form ref="ruleFormRef" :model="ruleForm" :rules="rules" label-width="auto" class="demo-ruleForm">
        <el-form-item prop="source" required>
          <!-- <el-input v-model.trim="ruleForm.source"></el-input> -->
          <template #label>
            {{ $t('taosuser.fromsource') }}
          </template>
          <el-select v-model="ruleForm.source" :placeholder="$t('pleaseSelect')">
            <el-option v-for="db in dblist" :key="db['node-key']" :label="db.name" :value="db.name"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item prop="target" required>
          <template #label>
            {{ $t('taosuser.targetdsn') }}
            <el-tooltip effect="light" placement="top">
              <template #content>
                <span v-dompurify-html="$t('datasource.replicationTargetInfo')"></span>
              </template>
              <el-icon><InfoFilled /></el-icon>
            </el-tooltip>
          </template>
          <el-input v-model.trim="ruleForm.target" placeholder="taos://192.168.0.1:6030/db2"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="default" class="w100" @click="dialog = false">
            {{ $t('cancel') }}
          </el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="default"
            class="w100"
            type="primary"
            :loading="requesting"
            @click="addReplication(ruleFormRef)"
            >{{ $t('confirm') }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import CopyText from '@/components/CopyText.vue';
import { excuteStart, excuteStop, excuteDel } from '@/api/common';
import { getReplicationList, addReplicationData } from '@/api/replication';
import { get, has } from 'lodash-es';
import { getDBListReq } from '@/api/database';
import { decrypt, parsinginZone } from '@/utils/index';
import { replicationMockData } from '@/const';
import { FormInstance, FormRules } from 'element-plus';
const { t } = useI18n();
const globalCustomProperties: any = inject('globalCustomProperties');
const { $IS_COMMUNITY, $error } = globalCustomProperties;

const props = defineProps({
  isLessThan3330: {
    type: Boolean
  }
});

const refreshable: Ref<boolean> = ref(false);
const pageSize = ref(10);
const currentPage = ref(1);
const total = ref(10);
const dialog: Ref<boolean> = ref(false);
const requesting: Ref<boolean> = ref(false);
let dblist = reactive([]);
const ruleForm = reactive({
  source: '',
  target: ''
});
const ruleFormRef = ref<FormInstance>();
const rules = reactive<FormRules>({
  source: [
    {
      required: true,
      message: t('taosuser.fromsourceRequired')
    }
  ],
  target: [
    {
      required: true,
      message: t('taosuser.targetdsnRequired')
    }
  ]
});
let topicList = ref([]);
// parsinginZone,

const fromUrl = () => {
  const user = localStorage.getItem('username') || '';
  const password = encodeURIComponent(decrypt(localStorage.getItem('pwd') || ''));
  const native_url = localStorage.getItem('native_url');
  const base_url = native_url || localStorage.getItem('base_url') || '';
  const splitArr = base_url?.split('//');
  const url = splitArr[0] + '//' + user + ':' + password + '@' + splitArr[1];
  const type = props.isLessThan3330 ? 'tmq' : 'sync';
  return splitArr[0].startsWith('taos') ? type + ':' + '//' + splitArr[1] : type + '+' + url;
};

async function getReplication() {
  try {
    const id = localStorage.getItem('local_clusterID');
    const res = await getReplicationList(id);
    topicList.value = res.map((item: { [x: string]: any; to_expand: { subject: string } }) => {
      item['fromdb'] = get(item, 'from_expand.subject');
      item['hostport'] = get(item, 'to');
      item['db'] = item.to_expand ? item.to_expand.subject : item['fromdb'];
      return item;
    });
  } catch (error) {
    console.warn(error);
  }
  refreshable.value = false;
}
function handlePageChange() {}
function closeDialog(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.resetFields();
  formEl.clearValidate();
  dialog.value = false;
}
function add() {
  dialog.value = true;
  ruleForm.source = '';
  ruleForm.target = '';
}
function del(data: { id: string }) {
  ElMessageBox.confirm(t('replication.backupDel', [data.id]), t('warning'), {
    confirmButtonText: t('confirm'),
    cancelButtonText: t('cancel'),
    type: 'warning'
  }).then(async () => {
    await excuteDel(data.id).then(res => {
      if (res && Object.hasOwnProperty.call(res, 'id')) {
        ElMessage({
          type: 'success',
          message: t('delSucc')
        });
        getReplication();
      } else {
        ElMessage({
          type: 'error',
          message: res.message
        });
      }
    });
  });
}
function refresh() {
  refreshable.value = true;
  getReplication();
}
async function addReplication() {
  try {
    requesting.value = true;
    const id = localStorage.getItem('local_clusterID');
    console.log('output:', fromUrl());
    const params = {
      labels: ['type::replication', `cluster-id::${localStorage.getItem('local_clusterID')}`],
      to: `${ruleForm.target}`,
      from: `${fromUrl()}/${ruleForm.source}?timeout=never`
    };
    const res = await addReplicationData(id, params);
    console.log(res);
    requesting.value = false;
    if (has(res, 'code') && has(res, 'message') && res.code != 0) {
      $error(res.message);
      return;
    }
    ElMessage.success(t('createSucc'));
    requesting.value = false;
    getReplication();
    dialog.value = false;
  } catch (err) {
    requesting.value = false;
    console.error(err);
    $error(err?.message);
  }
}

async function start(data: { id: string | number }) {
  try {
    await excuteStart(data.id).then(res => {
      if (res && Object.hasOwnProperty.call(res, 'code')) {
        ElMessage({
          type: 'error',
          message: res.message
        });
      } else {
        ElMessage.success(t('operateSucc'));
        getReplication();
      }
    });
  } catch (err) {
    return Promise.reject(err);
  }
}
async function stop(data: { id: string | number }) {
  try {
    await excuteStop(data.id).then(res => {
      if (res && Object.hasOwnProperty.call(res, 'code')) {
        ElMessage({
          type: 'error',
          message: res.message
        });
      } else {
        ElMessage.success(t('operateSucc'));
        getReplication();
      }
    });
  } catch (err) {
    return Promise.reject(err);
  }
}
function switchOperation(val: boolean, data: { id: string | number }) {
  console.log('val', val, data.id);
  ElMessageBox.confirm(
    val ? t('replication.taskStart', [data.id]) : t('replication.taskStop', [data.id]),
    t('warning'),
    {
      confirmButtonText: t('confirm'),
      cancelButtonText: t('cancel'),
      type: 'warning'
    }
  ).then(() => {
    if (val) {
      start(data);
    } else {
      stop(data);
    }
  });
}

async function getDatabases() {
  try {
    dblist = await getDBListReq();
  } catch (error) {
    console.log(error);
  }
}
function handleDSStatus(value: string) {
  return t('statuses.' + value);
}

function init() {
  if ($IS_COMMUNITY) {
    topicList = replicationMockData;
  } else {
    getDatabases();
    getReplication();
  }
}
init();
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}

.el-switch {
  margin-right: 10px;
}
</style>
