<template>
  <el-tabs v-model="backupActiveTab" :lazy="true">
    <el-tab-pane :label="$t('taosuser.backupPlan')" name="backupPlan">
      <backup-plan @view-history="onViewHistory"></backup-plan>
    </el-tab-pane>
    <el-tab-pane :label="$t('taosuser.backupFile')" name="backupFile">
      <backup-record @restore-started="onRestoreStarted"></backup-record>
    </el-tab-pane>
    <el-tab-pane :label="$t('taosuser.restoreTask')" name="restoreTask">
      <backup-restore></backup-restore>
    </el-tab-pane>
  </el-tabs>
</template>
<script setup lang="ts">
import backupPlan from './backup-plan.vue';
import backupRecord from './backup-record.vue';
import backupRestore from './backup-restore.vue';

import { useBackupStore } from '@/store/modules/8_administrator/backup';

const backupActiveTab = ref('backupPlan');

const backupStore = useBackupStore();
onMounted(() => {
  backupStore.getBackupPlanList();
  backupStore.initDatabaseList();
  backupStore.getRestoreList();
});

const onRestoreStarted = () => {
  backupStore.getRestoreList();
  backupActiveTab.value = 'restoreTask';
};

const onViewHistory = async id => {
  backupStore.setHistoryPlanId(id);
  try {
    await backupStore.getHistoryList(id);
  } catch (error) {
    ElMessage.error(error.message);
    return;
  }

  backupActiveTab.value = 'backupFile';
};
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}

.el-switch {
  margin-right: 10px;
}
</style>
