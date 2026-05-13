<template>
  <div class="flex-start flex-1">
    <el-select
      v-model="localData[config.field]"
      :allow-create="true"
      style="flex: 1"
      :placeholder="config.placeholder"
      :multiple="config.multiple"
      clearable
      filterable
    >
      <el-option v-for="item in options" :key="item.value" v-bind="item"></el-option>
    </el-select>
  </div>
</template>

<script setup lang="ts">
import { connectivityCheckResult } from '../model/util';

const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
    parentConfigList: Record<string, any>[];
  }>(),
  {}
);
const localData = reactive(props.data);

const options = ref<any[]>([]);

const emit = defineEmits(['update:data']);
watch(localData, newData => {
  emit('update:data', newData);
});

watch(
  connectivityCheckResult,
  data => {
    options.value =
      data?.namespaces?.map((item, index) => {
        return {
          label: item,
          value: index
        };
      }) || [];
  },
  {
    deep: true,
    immediate: true
  }
);
</script>

<style scoped lang="scss"></style>
