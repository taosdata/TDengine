<template>
  <el-table :data="data" size="default" class="table-expand" row-key="at">
    <el-table-column prop="level" :label="t('dataIn.level')" width="100">
      <template #default="scope">
        <span :style="getLevelStyle(scope.row.level)">
          <el-icon v-if="scope.row.level == 'warn'">
            <WarningFilled />
          </el-icon>
          <el-icon v-if="scope.row.level == 'error'">
            <CircleCloseFilled />
          </el-icon>
          <el-icon v-if="scope.row.level == 'info'">
            <InfoFilled />
          </el-icon>
          {{ scope.row.level }}
        </span>
      </template>
    </el-table-column>
    <el-table-column prop="at" :label="t('dataIn.at')" width="220">
      <template #default="scope">
        <span>{{ getTimeParser(scope.row.at) }}</span>
      </template>
    </el-table-column>
    <el-table-column prop="activity" :label="t('dataIn.activity')">
      <template #default="scope">
        <el-tooltip :content="scope.row.activity" placement="top-start">
          <span class="nowrap">{{ scope.row.activity }}</span>
        </el-tooltip>
      </template>
    </el-table-column>
    <el-table-column prop="context" :label="t('dataIn.context')"></el-table-column>
  </el-table>
</template>
<script setup lang="ts">
import { getTimeParser } from '../model/util';
import { t } from 'locales';
interface Props {
  data: Activity[];
}
interface Activity {
  activity: string;
  at: string;
  context: string | null;
  id: number;
  level: string;
  status: string;
}
withDefaults(defineProps<Props>(), {
  data: () => []
});

// 获取活动级别的样式
const getLevelStyle = (level: string): string => {
  let style = '';
  switch (level) {
    case 'info':
      style = 'color: #67c23a';
      break;
    case 'warn':
      style = 'color: #e6a23c';
      break;
    case 'error':
      style = 'color: #fe6c6c';
      break;
  }
  return style;
};
</script>
<style scoped lang="scss">
.activity-table {
  z-index: 100;
  padding-left: 5rem;
  overflow-y: auto;
}
</style>
