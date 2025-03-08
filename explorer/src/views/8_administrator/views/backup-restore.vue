<template>
  <div class="dnode-block">
    <div class="flex-end">
      <el-button
        plain
        type="primary"
        size="default"
        icon="refresh"
        :disabled="backupStore.backupPlanLoading || $IS_COMMUNITY"
        @click="backupStore.getRestoreList"
        >{{ $t('refresh') }}</el-button
      >
    </div>
  </div>

  <el-table style="margin-top: 20px" :data="backupStore.restoreList">
    <el-table-column width="50" label="ID" prop="id" show-overflow-tooltip></el-table-column>
    <el-table-column
      width="150"
      :label="$t('taosuser.backupForm.fileDir')"
      prop="from_path"
      show-overflow-tooltip
    ></el-table-column>
    <el-table-column width="420" :label="$t('taosuser.restoreRange')" prop="stable" show-overflow-tooltip>
      <template #default="scope">
        <span>{{ parsinginZone(scope.row.from_point_start) }} ~ {{ parsinginZone(scope.row.from_point_end) }}</span>
      </template>
    </el-table-column>
    <el-table-column :label="$t('taosuser.todb')" prop="to_database" show-overflow-tooltip></el-table-column>
    <el-table-column width="220" :label="$t('taosuser.createtime')" prop="upcoming" align="center">
      <template #default="scope">
        <span>{{ parsinginZone(scope.row.created_at) }}</span>
      </template>
    </el-table-column>
    <el-table-column width="100" :label="$t('taosuser.lastbackup')" prop="status" show-overflow-tooltip>
      <template #default="scope">
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
      </template>
    </el-table-column>
    <el-table-column :label="$t('taosuser.operation')" width="150">
      <template #default="scope">
        <el-switch
          :value="scope.row.status.toLowerCase() != 'stopped'"
          :disabled="$IS_COMMUNITY || scope.row.status.toLowerCase() == 'completed'"
          active-color="#13ce66"
          inactive-color="#dcdfe6"
        >
        </el-switch>
        <el-button
          plain
          size="small"
          icon="delete"
          :disabled="$IS_COMMUNITY || scope.row.status.toLowerCase() != 'stopped'"
        ></el-button>
      </template>
    </el-table-column>
  </el-table>
</template>
<script setup lang="ts">
const { t } = useI18n();
const { $IS_COMMUNITY } = inject('globalCustomProperties') as GlobalCustomProperties;
import { parsinginZone } from '@/utils/index';
import { useBackupStore } from '@/store/modules/8_administrator/backup';
const backupStore = useBackupStore();

const handleDSStatus = (status: string) => {
  return t('statuses.' + status);
};
</script>
<style lang="scss" scoped>
.el-switch {
  margin-right: 10px;
}

.w100 {
  width: 80px;
}
</style>
