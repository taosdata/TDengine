<template>
  <el-input v-model="num" type="number" class="td-time-input">
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
const props = defineProps<{
  modelValue: string;
}>();
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
