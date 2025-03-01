<template>
  <div class="flaxStart">
    <el-select
      v-model="backupStore.historyPlanId"
      style="width: 350px"
      :placeholder="$t('taosuser.tipSelectPlan')"
      @change="showHistoryList"
    >
      <el-option
        v-for="plan in backupStore.backupPlanList"
        :key="`filterBackupFile-${plan.id}`"
        :label="`${plan.id} | ${plan.database} ${plan.stable ? '| ' + plan.stable : ''}`"
        :value="plan.id"
      ></el-option>
    </el-select>
  </div>
  <el-table style="margin-top: 20px" :data="backupStore.historyList" default-expand-all>
    <el-table-column :label="$t('taosuser.backupPoint')" prop="point">
      <template #default="scope">
        <span>{{ parsinginZone(scope.row.point) }}</span>
      </template>
    </el-table-column>
    <el-table-column width="180" :label="$t('taosuser.backupFileSize')" prop="file_size"></el-table-column>
    <el-table-column width="180" :label="$t('taosuser.backupFileCount')" prop="file_count"></el-table-column>
    <el-table-column width="100" :label="$t('taosuser.operation')">
      <template #default="scope">
        <el-tooltip placement="top" :content="$t('taosuser.dataRestoration')" effect="light">
          <el-button plain size="small" icon="FirstAidKit" @click="toRestoreBackup(scope.row)"></el-button>
        </el-tooltip>
      </template>
    </el-table-column>
  </el-table>

  <el-dialog v-model="restoreConfirmDialog" :title="$t('tips')" width="700px">
    <div>
      <div style="margin-bottom: 10px">
        {{ $t('taosuser.confirmRestoreRange') }}
        <el-select v-model="restoreRange.from" style="width: 230px">
          <el-option
            v-for="item in restoreRangeList"
            :key="item"
            :label="parsinginZone(item)"
            :value="item"
          ></el-option>
        </el-select>
        <span> ~ </span>
        {{ parsinginZone(restoreRange.to) }}
      </div>
      <div>
        {{ $t('taosuser.restoreToDatabase') }}
        <el-select v-model="database" style="width: 230px" :placeholder="$t('taosuser.tipSelectTarget')">
          <el-option v-for="db in backupStore.dbList" :key="db['node-key']" :label="db.name" :value="db.name">
          </el-option>
        </el-select>
      </div>
    </div>

    <template #footer>
      <div class="dialog-footer">
        <el-button class="w100" @click="restoreConfirmDialog = false">{{ $t('cancel') }}</el-button>

        <el-button v-loading="requestIng" class="w100" type="primary" @click="restoreBackup()">{{
          $t('confirm')
        }}</el-button>
      </div>
    </template>
  </el-dialog>
</template>
<script setup lang="ts">
const { t } = useI18n();
import { parsinginZone } from '@/utils/index';
import { concatS3Config } from '@/utils/util';
import { restoreBackups } from '@/api/backup';
import { defineEmits } from 'vue';
import { useBackupStore } from '@/store/modules/8_administrator/backup';
const backupStore = useBackupStore();

const showHistoryList = async () => {
  backupStore.getHistoryList(backupStore.historyPlanId);
};

let pointToRestore: any;

const restoreRangeList = ref<any[]>([]);
const restoreConfirmDialog = ref(false);
const restoreRange = reactive({ from: '', to: '' });
const toRestoreBackup = (toFile: any) => {
  restoreRangeList.value = backupStore.historyList.map((item: any) => item.point).filter((item: any) => item <= toFile.point);
  restoreRange.from = toFile.point;
  restoreRange.to = toFile.point;
  pointToRestore = toFile;
  restoreConfirmDialog.value = true;
};

const requestIng = ref(false);
const database = ref('');
const emit = defineEmits(['restoreStarted']);
const restoreBackup = async () => {
  if (!database.value) {
    ElMessage.warning(t('taosuser.selectDatabase'));
    return;
  }
  let backupDirectory = null;
  let s3Config: string = "s3_enable=false";
  for (let i = 0; i < backupStore.backupPlanList.length; i++) {
    if (backupStore.backupPlanList[i].id === backupStore.historyPlanId) {
      backupDirectory = backupStore.backupPlanList[i].directory;
      s3Config = concatS3Config(backupStore.backupPlanList[i]);
      break;
    }
  }

  try {
    requestIng.value = true;
    const res = await restoreBackups({
      from: restoreRange.from,
      to: restoreRange.to,
      database: database.value,
      point: pointToRestore,
      backupDirectory,
      s3Config
    });

    if (res && res.code) {
      ElMessage.error(res.message);
      return;
    }

    ElMessage.success(t('operateSucc'));
    restoreConfirmDialog.value = false;
    emit('restoreStarted');
    // await this.getRestoreTasks();
    // this.backupActiveTab = "restoreTask";
  } catch (err) {
    ElMessage.error(err.toString());
  } finally {
    requestIng.value = false;
  }
};
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}

.el-switch {
  margin-right: 10px;
}

.w100 {
  width: 80px;
}
</style>
