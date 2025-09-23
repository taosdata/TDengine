<template>
  <span v-if="props.readonly">{{ valueText }}</span>
  <el-input v-else v-model="num" type="number" class="td-time-input">
    <template #append>
      <el-select v-model="unit">
        <el-option
          v-for="item in currentTimeUint"
          :key="item.value"
          :label="item.label"
          :value="item.value"
        ></el-option>
      </el-select>
    </template>
  </el-input>
</template>

<script lang="ts" setup>
import { TDengineTimeUnit } from 'constants1';
const props = withDefaults(
  defineProps<{
    modelValue: string;
    timeUnits?: LabelValue[];
    readonly?: boolean;
  }>(),
  {
    timeUnits: () => TDengineTimeUnit,
    readonly: false
  }
);
const currentTimeUint = TDengineTimeUnit.slice(0, 6);
const num = computed({
  get: () => props.modelValue.replace(/[^\d-]/g, ''),
  set: (val: string) => {
    emits('update:modelValue', val + unit.value);
  }
});
const unit = computed({
  get: () => props.modelValue.replace(/[-\d]/g, ''),
  set: (val: string) => {
    emits('update:modelValue', num.value + val);
  }
});

const valueText = computed(() => {
  const value = props.modelValue;
  if (!value) return '';
  const num = value.replace(/[^\d-]/g, '');
  const unit = value.replace(/[-\d]/g, '');
  return `${num} ${props.timeUnits.find(item => item.value === unit)?.label}`;
});

const emits = defineEmits<{
  'update:modelValue': [string];
}>();
</script>

<style scoped lang="scss">
.td-time-input:deep(.el-input-group__append) {
  background-color: transparent;
}

.td-time-input:deep(.el-select) {
  width: 100px;
  min-width: 100px !important;
}
</style>
