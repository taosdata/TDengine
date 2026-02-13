<template>
  <div class="dnode-block">
    <div class="flex-end">
      <el-button
        plain
        type="primary"
        size="default"
        icon="Refresh"
        :disabled="backupStore.backupPlanLoading || $IS_COMMUNITY"
        style="font-size: 14px"
        @click="refresh"
        >{{ $t('refresh') }}</el-button
      >
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
          >{{ $t('taosuser.createbackup') }}</el-button
        >
      </el-tooltip>
    </div>
    <el-table style="margin-top: 20px" :data="backupStore.backupPlanList" size="small">
      <el-table-column width="50" label="ID" prop="id" show-overflow-tooltip></el-table-column>
      <el-table-column
        width="150"
        :label="$t('taosuser.database')"
        prop="database"
        show-overflow-tooltip
      ></el-table-column>
      <el-table-column width="180" :label="$t('topic.stables')" prop="stable" show-overflow-tooltip></el-table-column>
      <el-table-column
        :label="$t('taosuser.backupForm.fileDir')"
        prop="directory"
        show-overflow-tooltip
      ></el-table-column>
      <el-table-column width="100" :label="$t('taosuser.backupFile')" prop="upcoming" align="center">
        <template #default="scope">
          <span v-if="scope.row.compression_level === 'none'">expired</span>
          <a v-else @click="viewHistory(scope.row.id)">{{ $t('view') }}</a>
        </template>
      </el-table-column>
      <el-table-column width="100" :label="$t('taosuser.backupForm.s3Enable')" prop="s3_enable" align="center">
        <template #default="scope">
          <span>{{ scope.row.s3_enable ? $t('yes') : $t('no') }}</span>
        </template>
      </el-table-column>
      <el-table-column width="100" :label="$t('taosuser.lastbackup')" prop="status" show-overflow-tooltip>
        <template #default="scope">
          <div class="status-operation">
            <el-tooltip
              v-if="['interrupted', 'failed'].includes(scope.row.status.toLowerCase())"
              placement="top"
              :open-delay="0"
            >
              <template #content>
                <div>{{ scope.row.last_modified_at }}</div>
                <div>{{ scope.row.reason }}</div>
              </template>
              <span>{{ handleDSStatus(scope.row.status) }}</span>
            </el-tooltip>
            <span v-else>{{ handleDSStatus(scope.row.status) }}</span>
          </div>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.operation')" width="280">
        <template #default="scope">
          <el-switch
            v-if="scope.row.compression_level !== 'none' || scope.row.running"
            v-model="scope.row.running"
            active-color="#13ce66"
            inactive-color="#dcdfe6"
            :disabled="$IS_COMMUNITY"
            @change="switchOperation($event, scope.row, 'replication.backupTip')"
          >
          </el-switch>
          <el-button plain size="small" icon="View" @click="viewBackup(scope.row)"></el-button>
          <el-button
            v-if="scope.row.compression_level !== 'none'"
            plain
            size="small"
            icon="Edit"
            :disabled="$IS_COMMUNITY"
            @click="edit(scope.row)"
          ></el-button>
          <el-button
            v-if="scope.row.compression_level !== 'none'"
            plain
            size="small"
            icon="CopyDocument"
            @click="copy(scope.row)"
          ></el-button>
          <el-button
            plain
            size="small"
            icon="Delete"
            :disabled="$IS_COMMUNITY || scope.row.status.toLowerCase() != 'stopped'"
            @click="toDel(scope.row)"
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
    >
    </el-pagination>

    <el-dialog
      v-model="dialog"
      align="center"
      :title="dialogTitle"
      width="600px"
      :destroy-on-close="true"
      :close-on-click-modal="false"
      @close="closeDialog(ruleFormRef)"
    >
      <div v-if="viewOnly" class="cover-readonly"></div>
      <el-form
        ref="ruleFormRef"
        :model="ruleForm"
        :rules="rules"
        class="demo-ruleForm"
        :label-width="isEn ? '180px' : '120px'"
      >
        <el-form-item :label="$t('taosuser.database')" prop="database">
          <el-select v-model="ruleForm.database" :disabled="!!currentId" @change="getSTbaleList">
            <el-option v-for="db in backupStore.dbList" :key="db['node-key']" :label="db.name" :value="db.name">
            </el-option>
          </el-select>
        </el-form-item>

        <el-form-item :label="$t('taosuser.supertable')" prop="stable">
          <el-select v-model="ruleForm.stable" allow-create default-first-option :disabled="!!currentId">
            <el-option
              v-for="(item, index) in stableList"
              :key="`stable-option-${index}`"
              :label="item"
              :value="item"
            ></el-option>
          </el-select>
        </el-form-item>

        <el-form-item :label="$t('taosuser.backupForm.upcoming')" required prop="upcoming" style="text-align: left">
          <el-date-picker v-model="ruleForm.upcoming" type="datetime" :disabled-date="isPastTime"> </el-date-picker>
        </el-form-item>

        <el-form-item prop="interval_value" required :label="$t('taosuser.backupcycle')">
          <el-input v-model="ruleForm.interval_value" class="input-with-select">
            <template #append>
              <el-select v-model="ruleForm.interval_unit" style="width: 100px">
                <el-option :label="$t('taosuser.timeUnitM')" value="m"></el-option>
                <el-option :label="$t('taosuser.timeUnitH')" value="h"></el-option>
                <el-option :label="$t('taosuser.timeUnitD')" value="d"></el-option>
              </el-select>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item prop="max_retry" required :label="$t('taosuser.backupForm.maxRetry')">
          <el-input v-model="ruleForm.max_retry"></el-input>
        </el-form-item>
        <el-form-item prop="retry_interval" required :label="$t('taosuser.backupForm.retryInterval')">
          <el-input v-model="ruleForm.retry_interval">
            <template #append>{{ $t('taosuser.timeUnitS') }}</template>
          </el-input>
        </el-form-item>

        <el-form-item :label="$t('taosuser.directory')" prop="directory">
          <el-input v-model.trim="ruleForm.directory" :disabled="!!currentId"></el-input>
        </el-form-item>
        <el-form-item prop="backup_max_size_value" required :label="$t('taosuser.backupForm.backupMaxSize')">
          <el-input v-model="ruleForm.backup_max_size_value">
            <template #append>
              <el-select v-model="ruleForm.backup_max_size_unit" style="width: 100px">
                <el-option label="MB" value="MB"></el-option>
                <el-option label="GB" value="GB"></el-option>
              </el-select>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item prop="compression_level" :label="$t('taosuser.backupForm.compressionLevel')">
          <el-select v-model="ruleForm.compression_level">
            <el-option :label="$t('taosuser.compressionLevel.balanced')" value="balanced"></el-option>
            <el-option :label="$t('taosuser.compressionLevel.best')" value="best"></el-option>
            <el-option :label="$t('taosuser.compressionLevel.fastest')" value="fastest"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item v-if="viewOnly" prop="created_at" :label="$t('taosuser.createtime')">
          <el-input v-model="ruleForm.created_at"></el-input>
        </el-form-item>
        <el-form-item prop="s3_enable" :label="$t('taosuser.backupForm.s3Enable')">
          <el-switch
            v-model="ruleForm.s3_enable"
            :disabled="!s3EnableEditable"
            active-color="#13ce66"
            inactive-color="#dcdfe6"
          ></el-switch>
        </el-form-item>
        <el-form-item
          v-if="ruleForm.s3_enable"
          required
          prop="s3_endpoint"
          :label="$t('taosuser.backupForm.s3Endpoint')"
        >
          <el-input v-model="ruleForm.s3_endpoint"></el-input>
        </el-form-item>
        <el-form-item
          v-if="ruleForm.s3_enable"
          required
          prop="s3_access_key_id"
          :label="$t('taosuser.backupForm.s3AccessKeyId')"
        >
          <el-input v-model="ruleForm.s3_access_key_id"></el-input>
        </el-form-item>
        <el-form-item
          v-if="ruleForm.s3_enable"
          required
          prop="s3_secret_access_key"
          :label="$t('taosuser.backupForm.s3SecretAccessKey')"
        >
          <el-input v-model="ruleForm.s3_secret_access_key"></el-input>
        </el-form-item>
        <el-form-item v-if="ruleForm.s3_enable" required prop="s3_region" :label="$t('taosuser.backupForm.s3Region')">
          <el-input v-model="ruleForm.s3_region"></el-input>
        </el-form-item>
        <el-form-item v-if="ruleForm.s3_enable" required prop="s3_bucket" :label="$t('taosuser.backupForm.s3Bucket')">
          <el-input v-model="ruleForm.s3_bucket"></el-input>
        </el-form-item>
        <el-form-item
          v-if="ruleForm.s3_enable"
          prop="s3_object_prefix"
          :label="$t('taosuser.backupForm.s3ObjectPrefix')"
        >
          <el-input v-model="ruleForm.s3_object_prefix"></el-input>
        </el-form-item>
        <el-form-item
          v-if="ruleForm.s3_enable"
          prop="backup_retention_period_value"
          :label="$t('taosuser.backupForm.backupRetentionPeriod')"
        >
          <el-input v-model.number="ruleForm.backup_retention_period_value" class="input-with-select">
            <template #append>
              <el-select v-model="ruleForm.backup_retention_period_unit" style="width: 100px">
                <el-option :label="$t('taosuser.timeUnitH')" value="h"></el-option>
                <el-option :label="$t('taosuser.timeUnitD')" value="d"></el-option>
              </el-select>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item
          v-if="ruleForm.s3_enable"
          prop="backup_retention_size"
          :label="$t('taosuser.backupForm.backupRetentionSize')"
        >
          <el-input v-model.number="ruleForm.backup_retention_size"></el-input>
        </el-form-item>
      </el-form>

      <el-row v-if="!viewOnly" style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="default" class="w100" @click="dialog = false">{{ $t('cancel') }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button class="w100" type="primary" @click="submit(ruleFormRef)">{{ $t('confirm') }}</el-button>
        </el-col>
      </el-row>
    </el-dialog>

    <el-dialog v-model="deleteConfirmDialog" :title="$t('tips')" width="400px">
      <span
        ><el-checkbox v-model="yesDeleteFile">{{ $t('taosuser.confirmDeleteBackupFile') }}</el-checkbox></span
      >
      <template #footer>
        <span class="dialog-footer">
          <el-button size="small" class="w100" @click="deleteConfirmDialog = false">{{ $t('cancel') }}</el-button>

          <el-button v-loading="requestIng" size="small" class="w100" type="primary" @click="del()">{{
            $t('confirm')
          }}</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import { defineEmits } from 'vue';
import { addBackupData, editBackup } from '@/api/backup';
import { executeStart, executeStop, executeDel } from '@/api/common';
import { getStables } from '@/api/database';
import { decrypt } from '@/utils/index';
import { concatS3Config } from '@/utils/util';
import { FormInstance, FormRules } from 'element-plus';
import { isEn } from '@/const';
import { useBackupStore } from '@/store/modules/8_administrator/backup';
import { validateTask } from '@/api/datain';

const emit = defineEmits(['viewHistory']);
const backupStore = useBackupStore();

const s3EnableEditable = ref(true);

const { t } = useI18n();
const globalCustomProperties: any = inject('globalCustomProperties');
const { $IS_COMMUNITY, $error } = globalCustomProperties;
interface RuleForm {
  database: string;
  stable: string;
  upcoming: string;
  interval_value: string;
  interval_unit: string;
  directory: string;
  max_retry: number;
  retry_interval: number;
  backup_max_size_value: string;
  backup_max_size_unit: string;
  compression_level: string;
  created_at: string;
  s3_enable: boolean;
  s3_endpoint: string;
  s3_access_key_id: string;
  s3_secret_access_key: string;
  s3_region: string;
  s3_bucket: string;
  s3_object_prefix: string;
  backup_retention_period_value: string;
  backup_retention_period_unit: string;
  backup_retention_size: number;
}

const currentId = ref('');
const ruleFormRef = ref<FormInstance>();
const requestIng: Ref<boolean> = ref(false);
const dialogTitle = ref('Create New Backup');
const pageSize = ref(10);
const currentPage = ref(1);
const total = ref(10);
const dialog = ref(false);
const username = localStorage.getItem('username') || '';
const decryptPwd = decrypt(localStorage.getItem('pwd') || '');
const ruleForm = reactive<RuleForm>({
  database: '',
  stable: '',
  upcoming: '',
  interval_value: '1',
  interval_unit: 'd',
  directory: '',
  max_retry: 3,
  retry_interval: 5,
  backup_max_size_value: '1',
  backup_max_size_unit: 'GB',
  compression_level: 'fastest',
  created_at: '',
  s3_enable: false,
  s3_endpoint: '',
  s3_access_key_id: '',
  s3_secret_access_key: '',
  s3_region: '',
  s3_bucket: '',
  s3_object_prefix: '',
  backup_retention_period_value: '1',
  backup_retention_period_unit: 'd',
  backup_retention_size: 10
});

const viewOnly = ref(false);

const copy = (data: any) => {
  s3EnableEditable.value = true;
  currentId.value = '';
  viewOnly.value = false;
  dialog.value = true;

  dialogTitle.value = `${t('create')} ${t('taosuser.backupPlan')}`;

  ruleForm.database = '';
  ruleForm.stable = '';
  ruleForm.upcoming = data.upcoming;
  ruleForm.directory = data.directory;
  ruleForm.compression_level = data.compression_level;
  ruleForm.max_retry = data.max_retry;
  ruleForm.retry_interval = data.retry_interval;

  const interval_parts = data.interval.match(/^(\d+)([smhd])$/);
  if (interval_parts && interval_parts.length === 3) {
    ruleForm.interval_value = interval_parts[1];
    ruleForm.interval_unit = interval_parts[2];
  }
  const backup_file_max_size_parts = data.backup_max_size.match(/^(\d+)([A-Z]{2})/);
  ruleForm.backup_max_size_value = backup_file_max_size_parts[1];
  ruleForm.backup_max_size_unit = backup_file_max_size_parts[2];
  if (data.s3_enable) {
    ruleForm.s3_enable = data.s3_enable;
    ruleForm.s3_endpoint = data.s3_endpoint;
    ruleForm.s3_access_key_id = data.s3_access_key_id;
    ruleForm.s3_secret_access_key = data.s3_secret_access_key;
    ruleForm.s3_region = data.s3_region;
    ruleForm.s3_bucket = data.s3_bucket;
    ruleForm.s3_object_prefix = data.s3_object_prefix;
    ruleForm.backup_retention_period_value = data.backup_retention_period_value;
    ruleForm.backup_retention_period_unit = data.backup_retention_period_unit;
    ruleForm.backup_retention_size = data.backup_retention_size;
  } else {
    ruleForm.s3_enable = false;
    ruleForm.s3_endpoint = '';
    ruleForm.s3_access_key_id = '';
    ruleForm.s3_secret_access_key = '';
    ruleForm.s3_region = '';
    ruleForm.s3_bucket = '';
    ruleForm.s3_object_prefix = '';
    ruleForm.backup_retention_period_value = '1';
    ruleForm.backup_retention_period_unit = 'd';
    ruleForm.backup_retention_size = 10;
  }
};

const rules = reactive<FormRules<typeof ruleForm>>({
  database: [
    {
      required: true,
      message: t('taosuser.tipSelectDatabase'),
      trigger: 'change'
    }
  ],
  upcoming: [
    {
      required: true,
      message: t('taosuser.tipInputUptime'),
      trigger: 'change'
    }
  ],
  interval_value: [
    {
      required: true,
      message: t('taosuser.tipInputCycle'),
      trigger: 'change'
    }
  ],
  directory: [
    {
      required: true,
      message: t('taosuser.directoryRequired')
    }
  ]
});
// let topicList = reactive([]);

// const parseBackup = data => {
//   const targetData: any = {};
//   targetData.id = data.id;
//   targetData['database'] = data.from.split('/').at(-1);
//   const params_start = targetData['database'].indexOf('?');
//   if (params_start > 0) {
//     targetData['database'] = targetData['database'].substring(0, params_start);
//   }

//   targetData.status = data.status;
//   targetData.stable = data.from_expand.params.stable;
//   targetData.upcoming = data.trigger.upcoming;
//   targetData.running = data.status !== 'stopped';

//   targetData.interval = data.trigger.interval;
//   targetData.max_size = data.to_expand.params.max_size;

//   targetData.directory = data.to_expand.path;
//   targetData.max_retry = data.from_expand.params.max_retry;
//   const retry_interval_part = data.from_expand.params.retry_interval.match(/^(\d+)s$/);
//   if (retry_interval_part && retry_interval_part.length === 2) {
//     targetData.retry_interval = retry_interval_part[1];
//   }
//   targetData.backup_max_size = data.to_expand.params.max_size;
//   targetData.compression_level = data.to_expand.params.compression_level;
//   targetData.created_at = parsinginZone(data.created_at);
//   return targetData;
// };

// const getBackData = async () => {
//   try {
//     requestIng.value = true;
//     const id = localStorage.getItem('local_clusterID') || '';
//     const res = await getBackupList(id, 'backup');
//     topicList = res.map(item => parseBackup(item));
//     // console.log('topicList', JSON.stringify(topicList));
//     requestIng.value = false;
//   } catch (error) {
//     return Promise.reject(error);
//   }
// };

const stableList = ref<string[]>([]);
const getSTbaleList = async () => {
  stableList.value = await getStables(ruleForm.database);
};

const refresh = () => {
  backupStore.getBackupPlanList();
};

const viewBackup = data => {
  copy(data);
  ruleForm.database = data.database;
  ruleForm.stable = data.stable;
  dialogTitle.value = t('taosuser.backupPlan');
  ruleForm.created_at = data.created_at;
  viewOnly.value = true;
};

const add = () => {
  copy({
    database: '',
    stable: '',
    upcoming: '',
    interval: '1d',
    interval_value: '1',
    interval_unit: 'd',
    directory: '',
    max_retry: 3,
    retry_interval: 5,
    backup_max_size: '1GB',
    backup_max_size_value: '1',
    backup_max_size_unit: 'GB',
    compression_level: 'fastest'
  });

  s3EnableEditable.value = true;
  dialogTitle.value = t('taosuser.createbackup');
  dialog.value = true;
  currentId.value = '';
  viewOnly.value = false;
};

const edit = (data: any) => {
  copy(data);
  s3EnableEditable.value = !ruleForm.s3_enable;
  ruleForm.database = data.database;
  ruleForm.stable = data.stable;
  dialogTitle.value = `${t('change')} ${t('taosuser.backupPlan')}`;
  currentId.value = data.id;
};

const deleteConfirmDialog = ref(false);
const yesDeleteFile = ref(false);
const toDel = row => {
  currentId.value = row.id;
  deleteConfirmDialog.value = true;
  yesDeleteFile.value = false;
};

const del = () => {
  executeDel(currentId.value, yesDeleteFile.value).then(res => {
    if (res && Object.hasOwnProperty.call(res, 'id')) {
      ElMessage.success(t('delSucc'));
      deleteConfirmDialog.value = false;
      backupStore.getBackupPlanList();
    } else {
      $error(res.message);
    }
  });
};

const handleDSStatus = (value: string) => {
  return t('statuses.' + value);
};

const start = async (val, data) => {
  try {
    const res = await executeStart(data.id);
    if (res && Object.hasOwnProperty.call(res, 'code')) {
      $error(res?.message);
    } else {
      ElMessage.success(t('operateSucc'));
      backupStore.getBackupPlanList();
    }
  } catch (err) {
    return Promise.reject(err);
  }
};

const stop = async (_val: any, data: any) => {
  try {
    const res = await executeStop(data.id);
    if (res && Object.hasOwnProperty.call(res, 'code')) {
      $error(res?.message);
    } else {
      ElMessage.success(t('operateSucc'));
      backupStore.getBackupPlanList();
    }
  } catch (err) {
    return Promise.reject(err);
  }
};

const switchOperation = (val: any, data: any, tip: string) => {
  if (val) {
    ElMessageBox.confirm(t(tip, [t('replication.start'), data.id]), t('warning'), {
      confirmButtonText: t('confirm'),
      cancelButtonText: t('cancel'),
      type: 'warning'
    })
      .then(() => {
        start(val, data);
      })
      .catch(() => {
        data.running = false;
      });
  } else {
    ElMessageBox.confirm(t(tip, [t('replication.stop'), data.id]), t('warning'), {
      confirmButtonText: t('confirm'),
      cancelButtonText: t('cancel'),
      type: 'warning'
    })
      .then(() => {
        stop(val, data);
      })
      .catch(() => {
        data.running = true;
      });
  }
};

function closeDialog(formEl: FormInstance | undefined) {
  if (!formEl) return;
  formEl.resetFields();
  formEl.clearValidate();
  dialog.value = false;
}

const constructPostData = () => {
  const clusterID = localStorage.getItem('local_clusterID');

  const base_url = localStorage.getItem('base_url');
  if (!base_url) {
    throw new Error('base_url is empty');
  }
  const splitArr = base_url.split('//');
  let fromDSN = `tmq+${splitArr[0]}//${username}:${encodeURIComponent(decryptPwd)}@${splitArr[1]}/${ruleForm.database}`;
  fromDSN += `?max_retry=${ruleForm.max_retry}&retry_interval=${ruleForm.retry_interval}s`;
  if (ruleForm.stable) {
    fromDSN += `&stable=${ruleForm.stable}`;
  }
  const toDSN = `local:${ruleForm.directory}?max_size=${ruleForm.backup_max_size_value}${ruleForm.backup_max_size_unit}&compression_level=${ruleForm.compression_level}&${concatS3Config(ruleForm)}`;

  // generate current datetime timestamp (ms)
  const currentDateTime = Date.now();
  const name = `backup_${currentDateTime}`;

  return {
    name,
    labels: ['type::backup', `cluster-id::${clusterID}`],
    trigger: {
      upcoming: ruleForm.upcoming,
      interval: `${ruleForm.interval_value}${ruleForm.interval_unit}`
    },
    from: fromDSN,
    to: toDSN
  };
};

const submit = (formEl: FormInstance | undefined) => {
  if (!formEl) return;
  formEl.validate(async valid => {
    if (!valid) {
      return;
    }
    const postData = constructPostData();
    if (ruleForm.s3_enable) {
      const result = await validateTask({ from: postData.from, to: postData.to });
      if (!result || !result.valid) {
        if (result.message) {
          $error(result.message);
        } else {
          $error(t('taosuser.validateS3Failed'));
        }
        return;
      }
    }

    try {
      if (currentId.value) {
        await editBackup(currentId.value, postData);
      } else {
        await addBackupData(postData);
      }
    } catch (err) {
      $error(err);
      return;
    }

    ElMessage.success(t('operateSucc'));
    dialog.value = false;
    refresh();
  });
};

const viewHistory = (id: string) => {
  emit('viewHistory', id);
};

const isPastTime = (time: Date) => {
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  return time.getTime() < now.getTime();
};
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}

.el-switch {
  margin-right: 10px;
}

.cover-readonly {
  position: absolute;
  inset: 50px 0 0;
  z-index: 10;
}

.w100 {
  width: 80px;
}
</style>
