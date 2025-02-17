<template>
  <el-date-picker
    v-bind="$attrs"
    v-model="currentValue"
    :value-format="valueFormat"
    @change="(val: any) => $emit('change', val)"
    @focus="(val: any) => $emit('focus', val)"
    @blur="(val: any) => $emit('blur', val)"
  >
  </el-date-picker>
</template>

<script setup lang="ts">
import { project } from 'config';

const OFFSETUTCTIME = project.isAliyun
  ? (new Date().getTimezoneOffset() + 480) * 60 * 1000
  : new Date().getTimezoneOffset() * 60 * 1000;

const props = defineProps({
  valueFormat: {
    type: String,
    default: 'timestamp'
  },
  modelValue: {
    type: [String, Number, Date, Array, Object],
    default: ''
  },
  type: {
    type: String,
    default: ''
  }
});

const emit = defineEmits(['change', 'focus', 'blur', 'input']);

const { modelValue, valueFormat } = toRefs(props);

const dateTimeRange = computed(() => {
  return props.type?.includes('range');
});

const currentValue = computed({
  get() {
    if (valueFormat.value === 'timestamp') {
      if (dateTimeRange.value) {
        return modelValue.value && (modelValue.value as any[]).length
          ? (modelValue.value as any[]).map(item => getTime(item))
          : [];
      }
      return modelValue.value ? getTime(modelValue.value).valueOf() : '';
    }
    return modelValue.value;
  },
  set(val) {
    if (valueFormat.value === 'timestamp') {
      if (dateTimeRange.value) {
        val = val ? (val as any[]).map(item => setTime(item)) : [];
      } else {
        val = val ? setTime(val) : '';
      }
    }
    emit('input', val);
  }
});

function getTime(timestamp: any) {
  return timestamp + OFFSETUTCTIME;
}

function setTime(timestamp: any) {
  return timestamp - OFFSETUTCTIME;
}
</script>

<style scoped lang="scss"></style>
