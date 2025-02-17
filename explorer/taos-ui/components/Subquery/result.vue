<template>
  <div class="result-form">
    <el-select
      v-model="currentValue.fn"
      class="flex-1 flex-center!"
      size="small"
      clearable
      :placeholder="t('common.selectFnPlaceholder')"
    >
      <el-option v-for="item in props.config" :key="item.label" :value="item.label"></el-option>
    </el-select>
    <template v-if="currentValue.params">
      <el-form :model="currentValue.params" class="shrink-0" size="small" inline>
        <el-form-item v-for="item in paramsList" :key="item.label" :label="item.label" :prop="item.field" required>
          <FormItem
            v-model="currentValue.params[item.field]"
            :field-list="props.fieldList"
            :config="item"
            v-bind="attrs"
            size="small"
          />
        </el-form-item>
      </el-form>
    </template>
  </div>
</template>

<script lang="ts" setup>
import type { TDFnType } from 'constants1';
import { TDFnDataStruct } from './type';
import FormItem from './formItem.vue';
import { t } from 'locales';

const props = defineProps<{
  modelValue: TDFnDataStruct;
  config: TDFnType[];
  fieldList: Recordable[];
}>();

const currentValue = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emits('update:modelValue', val);
  }
});
const attrs = useAttrs();
const emits = defineEmits(['update:modelValue']);
const paramsList = computed(() => {
  const currentFn = props.config.find(item => item.label === currentValue.value.fn);
  if (!currentFn || !currentFn.filters) return [];
  return currentFn.filters;
});
</script>

<style scoped lang="scss">
.result-form {
  display: flex;

  &:deep(.flex-1 > .el-select__wrapper) {
    width: 100%;
  }

  &:deep(.el-select) {
    width: 100px;
    min-width: 100px;
  }

  &:deep(.el-form--inline .el-form-item) {
    margin-right: 0;

    & + .el-form-item {
      margin-left: 10px;
    }
  }

  &:deep(.el-form-item__label) {
    font-size: 12px;
  }
}
</style>
