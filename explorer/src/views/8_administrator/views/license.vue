<template>
  <div v-loading="loading">
    <div class="flex-end" style="margin-bottom: 10px">
      <el-button
        plain
        type="primary"
        size="default"
        icon="Refresh"
        :disabled="loading || $IS_COMMUNITY"
        style="font-size: 14px"
        @click="refresh"
        >{{ $t('refresh') }}</el-button
      >
      <el-button plain type="primary" size="default" style="font-size: 14px" :disabled="$IS_COMMUNITY" @click="add">{{
        $t('taosuser.activationLicense')
      }}</el-button>
    </div>
    <title-bar :name="$t('topic.basicDatabaseFeatures')" />
    <el-descriptions style="margin-bottom: 30px" :column="3">
      <el-descriptions-item :key="'clusterId'" :label="$t('topic.clusterId')" :label-style="style">
        <span>{{ clusterId }}</span>
      </el-descriptions-item>
      <el-descriptions-item :key="'machineCode'" :label="$t('topic.machineCode')" :label-style="style">
        <span>{{ machineCode || 'N/A' }}</span>
      </el-descriptions-item>
      <el-descriptions-item
        v-for="item in licenseList"
        :key="item.key"
        :label="$INDUSTRY && item.key == 'version' ? $t('header.power') : $t(`topic.${item.key}`)"
        :label-style="style"
      >
        <span v-if="item.key !== 'version'" style="color: #333">
          {{
            ['expire_time', 'service_time'].includes(item.key) && item.value !== 'unlimited'
              ? parsinginZone(item.value, 'YYYY-MM-DD h:mm:ss')
              : item.value
          }}</span
        >
        <span v-else style="color: #333">
          <span style="padding-left: 2px">{{ serverVersion }}</span>
        </span>
      </el-descriptions-item>
    </el-descriptions>
    <template v-if="!isLessThan3_2_3_0">
      <title-bar :name="$t('topic.advancedDatabaseFeatures')" />
      <el-table style="margin-bottom: 30px" :data="advancedTableData" size="small">
        <el-table-column :label="$t('topic.advancedFeatures')" prop="display_name"></el-table-column>
        <el-table-column :label="$t('topic.used')" prop="limits">
          <template #default="scope">
            <span>{{ usedNumber(scope.row.limits) }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('topic.limit')" prop="limits">
          <template #default="scope">
            <span>{{ formatLimits(scope.row.limits) }}</span>
          </template>
        </el-table-column>
        <!-- 占位 -->
        <el-table-column />
        <el-table-column :label="$t('topic.expire_time')" prop="expire">
          <template #default="scope">
            <span>{{ scope.row.expire == 'unlimited' ? 'unlimited' : expireTime(scope.row.expire) }}</span>
          </template>
        </el-table-column>
      </el-table>
    </template>
    <title-bar :name="$t('topic.connectors')" />
    <el-table v-if="getMetaShow('dataIn')" :data="tableData" size="small">
      <el-table-column :label="$t('topic.type')" prop="type"></el-table-column>
      <el-table-column :label="$t('topic.tasks')" prop="number">
        <template #default="scope">
          <span>{{ scope.row.number == -1 ? 'unlimited' : scope.row.number }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('topic.speed')" prop="speed">
        <template #default="scope">
          <span>{{ scope.row.speed == -1 ? 'unlimited' : scope.row.speed }}</span>
        </template>
      </el-table-column>
      <el-table-column v-if="isLessThan3_2_3_0" :label="$t('topic.expire_time')" prop="expire">
        <template #default="scope">
          <span>{{ expireTime(scope.row.expire) }}</span>
        </template>
      </el-table-column>
      <el-table-column v-if="!isLessThan3_2_3_0" :label="$t('topic.expire_time')" prop="expireTime">
        <template #default="scope">
          <span>{{ scope.row.expireTime == 'unlimited' ? 'unlimited' : expireTime(scope.row.expireTime) }}</span>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog v-model="dialog" align="center" width="600px" :destroy-on-close="true" :close-on-click-modal="false">
      <template #header>
        <div>
          <div class="activate-title">{{ $t('taosuser.activationLicense') }}</div>
          <span class="activate-tip">{{ $t('taosuser.activeTip') }}</span>
        </div>
      </template>
      <el-form
        ref="ruleFormRef"
        :model="ruleForm"
        :rules="rules"
        size="default"
        :label-width="getlabelWidth"
        class="demo-ruleForm"
        label-position="left"
        @submit.prevent
      >
        <el-form-item :label="$t('taosuser.activeCode')" prop="active_code">
          <el-input v-model.trim="ruleForm.active_code" @keyup.enter="submit(ruleFormRef)"></el-input>
        </el-form-item>
        <el-form-item v-if="isLessThan3_2_3_0" :label="$t('taosuser.cActiveCode')" prop="c_active_code">
          <el-input v-model.trim="ruleForm.c_active_code" @keyup.enter="submit(ruleFormRef)"></el-input>
        </el-form-item>
      </el-form>

      <el-row style="margin-top: 20px">
        <el-col :span="5" :offset="6">
          <el-button size="default" class="w100" @click="dialog = false">{{ $t('cancel') }}</el-button>
        </el-col>
        <el-col :span="5" :push="4">
          <el-button
            size="default"
            :disabled="confirmStatus"
            class="w100"
            type="primary"
            @click="submit(ruleFormRef)"
            >{{ $t('confirm') }}</el-button
          >
        </el-col>
      </el-row>
    </el-dialog>
  </div>
</template>
<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { activeLicence } from '@/api/licence';
import { parsinginZone, getLocalLang, compareVersion } from '@/utils/index';
import { FormRules, FormInstance } from 'element-plus';
import useLicense from '@/hooks/useLicense';
import { useStore } from 'vuex';
import titleBar from './components/title-bar.vue';

const globalCustomProperties: any = inject('globalCustomProperties');
const { $IS_COMMUNITY, $INDUSTRY, $error } = globalCustomProperties;

const { getGrantsFull, getMetaShow } = useLicense();
const { t } = useI18n();
const store = useStore();
const router = useRouter();
const ruleFormRef = ref<FormInstance>();

const dialog = ref(false);
const loading = ref(false);
const ruleForm = reactive({
  active_code: '',
  c_active_code: ''
});

const rules = reactive<FormRules>({
  active_code: [
    {
      message: t('dataIn.enterTip')
    }
  ],
  c_active_code: [
    {
      message: t('dataIn.enterTip')
    }
  ]
});
const licenseList: any = ref([]);
const tableData: any = ref([]);
const advancedTableData = ref([]);
const machineCode = ref('');
const isLessThan3_2_3_0 = ref(false);
const isGreaterThan3_3_0_0 = ref(false);
const isGreaterThan3_3_0_1 = ref(false);

const style = computed(() => {
  return {
    'font-size': '14px',
    color: '#4d6992',
    'min-width': $INDUSTRY && getLocalLang() == 'en' ? '156px' : '110px',
    display: 'inline-block',
    'text-align': 'right'
  };
});
const confirmStatus = computed(() => {
  if (!ruleForm.active_code && !ruleForm.c_active_code) {
    return true;
  }
  return false;
});
const getlabelWidth = computed(() => {
  const lang = getLocalLang();
  if (lang === 'zh' && isLessThan3_2_3_0.value) {
    return '120px';
  }
  if (!isLessThan3_2_3_0.value) {
    return 'auto';
  }
  return '240px';
});
const TDengineVersion = localStorage.getItem('td_version') || '';
// const TDengineVersion =  "3.2.3.0"
const clusterId = ref(localStorage.getItem('local_clusterID') || '');
const serverVersion = ref(localStorage.getItem('serverVersion') || '');

function handlecActiveCodeShow() {
  isLessThan3_2_3_0.value = compareVersion(TDengineVersion, '<=3.2.3.0');
  isGreaterThan3_3_0_0.value = compareVersion(TDengineVersion, '>=3.3.0.0');
  isGreaterThan3_3_0_1.value = compareVersion(TDengineVersion, '>=3.3.0.1');
}

async function getData() {
  try {
    // let cols = [];
    // 获取机器码
    await sendSQLReq(`show cluster machines;`).then(res => {
      const array = res.data.map(data => {
        return Object.fromEntries(
          res.column_meta.map((item, index) => {
            return [item[0], data[index]];
          })
        );
      });
      // 获取第一个机器码
      if (array.length > 0) {
        machineCode.value = array[0].machine || '';
        clusterId.value = array[0].id || clusterId.value;
      }
    }).catch(() => {
      // 如果命令不支持，忽略错误
      machineCode.value = '';
    });
    // 不管是任何版本都show grants
    await sendSQLReq(`show grants;`).then(res => {
      const array = res.data.map(data => {
        return Object.fromEntries(
          res.column_meta.map((item, index) => {
            // cols.push({ header: item[0], value: item[0] });
            return [item[0], data[index]];
          })
        );
      });
      const allLicence =
        array.length > 0
          ? Object.keys(array[0]).map(key => {
              return {
                key: key,
                value: array[0][key]
              };
            })
          : [];
      licenseList.value = allLicence.filter(item => item.value.indexOf('{') == -1);
      if (isLessThan3_2_3_0.value) {
        tableData.value = allLicence
          .filter(item => item.value.indexOf('{') == 0)
          .map(data => {
            return JSON.parse(data.value);
          });
      }
    });
    if (!isLessThan3_2_3_0.value) {
      await sendSQLReq(`show grants full;`).then(res => {
        const array = res.data.map(data => {
          return Object.fromEntries(
            res.column_meta.map((item, index) => {
              return [item[0], data[index]];
            })
          );
        });

        const allData = array
          .filter(item => item.limits.indexOf('{') == 0)
          .map(data => {
            return {
              ...JSON.parse(data.limits),
              type: data.display_name || data.grant_name,
              grant: data.grant_name,
              expire_time: data.expireTime
            };
          });
        // 3.3.0.0 之前不显示 mysql、postgres、oracle
        tableData.value = allData.filter(v => !['mysql', 'postgres', 'oracle', '__future_datain__'].includes(v.grant));

        // 3.3.0.0 增加 mysql、postgres
        if (isGreaterThan3_3_0_0.value) {
          tableData.value = allData.filter(v => !['oracle'].includes(v.grant));
        }
        // 3.3.0.1 增加 oracle
        if (isGreaterThan3_3_0_1.value) {
          tableData.value = allData.filter(v => !['__future_datain__'].includes(v.grant));
        }
        advancedTableData.value = array.filter(item => item.limits.indexOf('{') == -1);
        console.log('this.tableData', tableData.value, advancedTableData.value);
      });
    }
    loading.value = false;
  } catch (error) {
    loading.value = false;
  }
}
function add() {
  dialog.value = true;
}

function expireTime(data: any) {
  if (isLessThan3_2_3_0.value) {
    return parsinginZone(Number(data) * 24 * 60 * 60 * 1000, 'YYYY-MM-DD');
  } else {
    return parsinginZone(data, 'YYYY-MM-DD hh:mm:ss');
  }
}

const EMPTY_NUMBER = 'n/a';
function usedNumber(data: string) {
  if (data) {
    if (data.indexOf('/') > 0) {
      const split = data.split('/');
      return split[0];
    } else {
      return EMPTY_NUMBER;
    }
  } else {
    return EMPTY_NUMBER;
  }
}
function formatLimits(data: string) {
  console.log('formatLimits', data);
  if (data) {
    if (data.indexOf('/') > 0) {
      const split = data.split('/');
      if (split.length > 2) {
        return data;
      } else {
        return split[1];
      }
    } else {
      return data;
    }
  } else {
    return 'n/a';
  }
}
function logout() {
  localStorage.clear();

  store.dispatch('app/logout');
  router.push({
    path: '/login'
  });
  window.location.reload();
}
function showLogoutConfirm() {
  ElMessageBox.confirm(t('taosuser.licenseSuccTip'), t('tips'), {
    distinguishCancelAndClose: true,
    confirmButtonText: t('signOut'),
    cancelButtonText: t('cancel')
  })
    .then(() => {
      logout();
    })
    .catch(() => {
      console.log('cancel');
    });
}
function refresh() {
  loading.value = true;
  getData();
  getGrantsFull();
}
async function submit(formEl: FormInstance | undefined) {
  if (!formEl) return;
  try {
    if (confirmStatus.value) return;
    await activeLicence(ruleForm).then(res => {
      if (res && res.code == 0) {
        ElMessage.success(t('operateSucc'));
        dialog.value = false;
        refresh();
        if ($INDUSTRY) {
          showLogoutConfirm();
        }
      } else {
        $error(res?.desc);
      }
    });
  } catch (error) {
    // this.$error(error);
    console.log('error:', error);
  }
}

getData();
handlecActiveCodeShow();
</script>
<style lang="scss" scoped>
:deep(.el-form-item__content) {
  display: flex;
}

:deep(th.el-descriptions-item__cell.el-descriptions-item__label.is-bordered-label) {
  width: 80px;
}

:deep(td.el-descriptions-item__cell.el-descriptions-item__content) {
  width: 200px;
}

:deep(.el-descriptions .el-descriptions-item__cell) {
  padding: 12px 5px;
  border-bottom: 1px solid #dfe6ec;
}

:deep(.el-form-item--default .el-form-item__label) {
  text-align: left;
  word-break: break-word;
}

.activate-title {
  font-size: 20px;
  font-weight: 500;
  line-height: 26px;
  color: #4d6992;
}

.activate-tip {
  color: #909399;
}
</style>
