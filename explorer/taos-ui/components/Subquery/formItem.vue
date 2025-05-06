<template>
  <el-select
    v-if="props.config.type == 'select'"
    v-bind="props.config"
    v-model="currentValue"
    :size="props.size"
    clearable
  >
    <el-option v-for="ite in getOptions()" :key="ite.value" v-bind="ite"></el-option>
  </el-select>
  <el-input
    v-else-if="props.config.type == 'input'"
    v-model="currentValue"
    :size="props.size"
    clearable
    v-bind="props.config"
  ></el-input>
  <el-input-number
    v-else-if="props.config.type == 'number'"
    v-model="currentValue"
    clearable
    v-bind="props.config"
    :size="props.size"
  ></el-input-number>
  <ArrayParams
    v-else-if="props.config.type == 'array'"
    v-model="currentValue"
    :size="props.size"
    :field-list="props.fieldList"
    :current-field="props.currentField"
    v-bind="attrs"
    :config="props.config"
  />
</template>

<script lang="ts" setup>
import type { FnFilterItem } from 'constants1';
import { rmStrBackquote } from 'utils/tdengine';
import { isArray } from 'utils/validate';
import ArrayParams from './arrayParams.vue';
const props = withDefaults(
  defineProps<{
    config: FnFilterItem;
    modelValue: any;
    fieldList?: Recordable[];
    currentField?: string;
    size?: ElSize;
  }>(),
  {
    fieldList: () => [],
    currentField: '',
    size: 'default'
  }
);

const currentValue = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emits('update:modelValue', val);
  }
});
const emits = defineEmits(['update:modelValue']);
const attrs = useAttrs();
const fieldOptions = computed(() => {
  const filed = rmStrBackquote(props.currentField);
  return props.fieldList
    .filter(item => item.field !== filed)
    .map(item => ({
      label: item.field,
      value: item.field
    }));
});

function getOptions() {
  const options = props.config.options;
  if (!options) return [];
  if (isArray(options)) return options;
  if (typeof options == 'function') return options(props, fieldOptions.value);
}
</script>

<style scoped lang="scss"></style>
