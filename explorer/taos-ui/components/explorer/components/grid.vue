<template>
  <div class="gird">
    <el-table
      stripe
      border
      tooltip-effect="light"
      size="small"
      :data="dataSource"
      height="100%"
      style="border-bottom: none"
      @cell-dblclick="handleCellDblclick"
    >
      <!--数据源-->

      <template v-if="head.length">
        <el-table-column
          v-for="(item, index) in head"
          :key="item.field + index"
          :min-width="item.length + 'px'"
          :show-overflow-tooltip="true"
          :label="item.field"
          :prop="index + ''"
        >
          <template #default="{ row }">
            <el-tooltip :content="t('explorer.cellCopyTip')">
              <span>{{ row[index] }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
      </template>
    </el-table>
    <section v-if="currentHistory && dataSource.length" class="time-wrapper">
      <div class="time-block">
        <span class="title">{{ t('explorer.execute') }}:</span>
        <span class="value">{{ currentHistory.executTime }} ms</span>
      </div>
      <div class="time-block">
        <span class="title">{{ t('explorer.network') }}:</span>
        <span class="value">{{ currentHistory.networkTime }} ms</span>
      </div>
      <div class="time-block">
        <span class="title">{{ t('common.total') }}:</span>
        <span class="value">{{ currentHistory.totalTime }} ms</span>
      </div>
    </section>
  </div>
</template>
<script lang="ts" setup>
import { copy } from 'utils';
import { t } from 'locales';
import { sqlExecResult, addLogEvent } from './utils';

const head = computed(() => sqlExecResult.head);
const dataSource = computed(() => sqlExecResult.data);
const currentHistory = ref<Recordable | null>(null);

addLogEvent.on(log => {
  if (log.type == 1) {
    currentHistory.value = log;
  } else {
    currentHistory.value = null;
  }
});

function handleCellDblclick(row: Recordable, column: any) {
  copy(row[column.property]);
}
</script>
<style lang="scss" scoped>
.gird {
  position: relative;
  height: 100%;
  padding-bottom: 30px;

  // overflow: auto;
  overflow: hidden;

  &:deep(.el-table::before) {
    height: 0;
  }

  &:deep(.el-table--mini .el-table__header-wrapper .el-table__cell) {
    cursor: unset;
  }

  .time-wrapper {
    position: absolute;
    right: 0;
    bottom: -3px;
    left: 10px;

    .time-block {
      display: inline-block;
      margin-right: 20px;
      line-height: 20px;

      .title {
        margin-right: 5px;
        font-size: 16px;
        color: #4d6992;
      }

      .value {
        font-size: 14px;
        color: #999;
      }
    }
  }
}
</style>
