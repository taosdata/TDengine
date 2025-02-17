<template>
  <div>
    <el-form-item :label="t('stream.windowClause')">
      <el-radio-group v-model="currentValue.window_type" @input="typeChange">
        <el-radio-button label="SESSION" value="SESSION"></el-radio-button>
        <el-radio-button label="STATE" value="STATE"></el-radio-button>
        <el-radio-button label="INTERVAL" value="INTERVAL"></el-radio-button>
      </el-radio-group>
    </el-form-item>
    <el-form-item v-if="currentValue.window_type == 'SESSION'" :label="t('date.totalTime')">
      <el-input-number v-model="currentValue.tol_val" :min="0"></el-input-number>
      <el-select v-model="currentValue.tol_unit" class="ml-10px" placeholder="">
        <el-option v-for="item in TDengineTimeUnit" :key="item.value" v-bind="item"></el-option>
      </el-select>
    </el-form-item>
    <el-form-item v-if="currentValue.window_type == 'STATE'" :label="t('stb.column')" prop="state_column" required>
      <el-select v-model="currentValue.state_column" class="w100" placeholder="">
        <el-option v-for="item in stateColumn" :key="item.field" :value="item.field"></el-option>
      </el-select>
    </el-form-item>
    <template v-if="currentValue.window_type == 'INTERVAL'">
      <el-form-item :label="t('stream.intervalPeriod')">
        <el-input-number v-model="currentValue.interval_val" :min="1"></el-input-number>
        <el-select v-model="currentValue.interval_unit" class="ml-10px" placeholder="">
          <el-option v-for="item in intervalTimeUnit" :key="item.value" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="t('stream.slidingPeriod')">
        <template #label>
          <span>{{ t('stream.slidingPeriod') }}&nbsp;</span>
          <el-tooltip effect="light" :content="t('stream.slidingTip')" placement="top">
            <i class="el-icon-info"></i>
          </el-tooltip>
        </template>
        <el-input-number v-model="currentValue.sliding_val" :min="0"></el-input-number>
        <el-select v-model="currentValue.sliding_unit" style="margin-left: 20px" placeholder="">
          <el-option v-for="item in TDengineTimeUnit" :key="item.value" v-bind="item"></el-option>
        </el-select>
      </el-form-item>
    </template>
  </div>
</template>

<script lang="ts" setup>
import { TDengineTimeUnit } from 'constants1';
import { t } from 'locales';
import { WindowClauseValue } from './type';
const stateColumnExculde = ['TIMESTAMP', 'FLOAT', 'DOUBLE'];
const intervalTimeUnit = TDengineTimeUnit.slice(2);
const props = defineProps<{
  modelValue: WindowClauseValue;
  columnList: Recordable[];
}>();
const currentValue = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emits('update:modelValue', val);
  }
});
const emits = defineEmits(['update:modelValue']);
const stateColumn = computed(() => props.columnList.filter(item => !stateColumnExculde.includes(item.type)));
function typeChange(val: string) {
  if (val != 'INTERVAL' && currentValue.value.parttionSet != 'tbname') {
    currentValue.value.parttionSet = 'tbname';
  }
}
</script>

<style scoped lang="scss">
:deep(.el-select) {
  width: 100px;
  min-width: 100px;
}
</style>
