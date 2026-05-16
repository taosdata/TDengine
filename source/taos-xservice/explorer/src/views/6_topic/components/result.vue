<!-- eslint-disable vue/no-mutating-props -->
<template>
  <el-form style="text-align: left" size="default" label-width="140px" label-position="left">
    <el-form-item :label="$t('topic.function')">
      <el-select v-model="result.fn" class="w100" clearable placeholder="" filterable>
        <el-option-group v-for="group in fnList" :key="group.label" :label="group.label">
          <el-option
            v-for="item in group.options"
            :key="item.label"
            :label="item.label"
            :value="item.label"
            :disabled="
              (parentName == 'Stream' && (item.hasOwnProperty('supportStream') || item.selectDisable)) ||
              (parentName == 'Topic' && (item.hasOwnProperty('supportTopic') || item.selectDisable))
            "
          >
          </el-option>
        </el-option-group>
      </el-select>
    </el-form-item>
    <template v-if="currentFn && currentFn.filters">
      <el-form-item v-for="item in currentFn.filters" :key="item.field" :label="item.label">
        <el-select
          v-if="item.type == 'select'"
          v-bind="item"
          v-model="result.params[item.field]"
          class="w100"
          clearable
          filterable
        >
          <el-option
            v-for="ite in getOptions(item)"
            :key="ite.value"
            v-bind="ite"
            :value="ite.value"
            :label="ite.label"
          ></el-option>
        </el-select>
        <el-input
          v-else-if="item.type == 'input'"
          v-model="result.params[item.field]"
          clearable
          v-bind="item"
        ></el-input>
        <el-input-number
          v-else-if="item.type == 'number'"
          v-model="result.params[item.field]"
          clearable
          v-bind="item"
        ></el-input-number>
      </el-form-item>
    </template>
  </el-form>
</template>

<script setup lang="ts">
import { isArray } from '@/utils/validate';

const parentName = inject('parentName');
interface Props {
  result: Record<string, any>;
  fnList: any[];
  fieldList: any[];
  field: string;
}
const props = withDefaults(defineProps<Props>(), {
  result: () => ({}),
  fnList: () => [],
  fieldList: () => [],
  field: ''
});

const currentFn = computed(() => {
  if (props.fnList.length > 0) {
    return props.fnList
      .map(fn => fn.options)
      .flat(1)
      .find(item => item.label == props.result.fn);
  }
  return null;
});

function getOptions(item) {
  const options = item.options;
  if (!options) return [];
  if (isArray(item.options)) return item.options;
  if (typeof item.options == 'function') {
    return (
      item.options.call(this).map(opt => {
        return {
          label: opt.field,
          value: opt.field
        };
      }) || []
    );
  }
}
</script>

<style scoped lang="scss"></style>
