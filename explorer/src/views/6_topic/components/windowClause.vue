<!-- eslint-disable vue/no-mutating-props -->
<template>
  <div>
    <el-form-item :label="$t('stream.windowClause')">
      <el-radio-group v-model="windowClause.window_type" size="default">
        <el-radio-button label="SESSION" value="SESSION"></el-radio-button>
        <el-radio-button label="STATE" value="STATE"></el-radio-button>
        <el-radio-button label="INTERVAL" value="INTERVAL"></el-radio-button>
      </el-radio-group>
    </el-form-item>
    <el-form-item v-if="windowClause.window_type == 'SESSION'" :label="$t('sql.totalTime')">
      <el-input-number v-model="windowClause.tol_val" :min="0"></el-input-number>
      <el-select v-model="windowClause.tol_unit" style="width: 180px; margin-left: 20px" placeholder="">
        <el-option v-for="item in timeUnit" :key="item.value" v-bind="item"></el-option>
      </el-select>
    </el-form-item>
    <el-form-item v-if="windowClause.window_type == 'STATE'" :label="$t('stream.column')" prop="state_column" required>
      <el-select v-model="windowClause.state_column" class="w100" placeholder="">
        <el-option v-for="item in stateColumn" :key="item.field" :value="item.field"></el-option>
      </el-select>
    </el-form-item>
    <template v-if="windowClause.window_type == 'INTERVAL'">
      <el-form-item :label="$t('stream.intervalPeriod')">
        <el-input-number v-model="windowClause.interval_val" :min="1"></el-input-number>
        <el-select v-model="windowClause.interval_unit" style="width: 180px; margin-left: 20px" placeholder="">
          <el-option v-for="item in intervalTimeUnit" :key="item.value" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.intervaloffset')">
        <el-input-number v-model="windowClause.interval_offset" :min="0"></el-input-number>
        <el-select v-model="windowClause.offset_unit" style="width: 180px; margin-left: 20px" placeholder="">
          <el-option v-for="item in intervalTimeUnit" :key="item.value" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('stream.slidingPeriod')">
        <template #label>
          <span>{{ $t('stream.slidingPeriod') }}&nbsp;</span>
          <el-tooltip effect="light" :content="$t('stream.slidingTip')" placement="top">
            <el-icon><InfoFilled /></el-icon>
          </el-tooltip>
        </template>
        <el-input-number v-model="windowClause.sliding_val" :min="0"></el-input-number>
        <el-select v-model="windowClause.sliding_unit" style="width: 180px; margin-left: 20px" placeholder="">
          <el-option v-for="item in timeUnit" :key="item.value" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
    </template>
  </div>
</template>

<script setup lang="ts">
import { TDengineTimeUnit } from '@/const';
const stateColumnExclude = ['TIMESTAMP', 'FLOAT', 'DOUBLE'];
interface Props {
  windowClause: windowClauseType;
  columnList: any[];
}
interface windowClauseType {
  type: string;
  tol_val: number;
  tol_unit: string;
  interval_val: number;
  interval_offset: number;
  column: string;
  interval_unit: string;
  offset_unit: string;
  sliding_val: number;
  sliding_unit: string;
}
const props = withDefaults(defineProps<Props>(), {
  windowClause: () => ({
    type: 'SESSION',
    tol_val: 0,
    tol_unit: 'm',
    interval_val: 1,
    interval_offset: 0,
    column: '',
    interval_unit: 'm',
    offset_unit: 'm',
    sliding_val: 0,
    sliding_unit: 's'
  }),
  columnList: () => []
});

const timeUnit = TDengineTimeUnit;
const intervalTimeUnit = TDengineTimeUnit.slice(2);

const stateColumn = computed(() => {
  return props.columnList.filter(item => !stateColumnExclude.includes(item.type));
});
</script>

<style scoped lang="scss">
:deep(.el-input-number__increase),
:deep(.el-input-number__decrease) {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 30px;
}
</style>
