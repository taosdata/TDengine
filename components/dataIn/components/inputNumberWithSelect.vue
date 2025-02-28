<template>
  <div class="input-number-with-select">
    <el-input-number
      v-model="localData[config.field + '_value']"
      style="width: 80%"
      :placeholder="config.placeholder"
      :max="config.max"
      :min="config.min"
      :controls="false"
    >
    </el-input-number>
    <el-select v-model="localData[config.field + '_unit']" style="width: 20%">
      <el-option v-for="item in options" :key="item.value" v-bind="item" :title="item.label"></el-option>
    </el-select>
  </div>
</template>
<script setup lang="ts">
const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    data: Record<string, any>;
    options: Record<string, any>;
  }>(),
  {}
);
const localData = reactive(props.data);
defineEmits(['update:data']);
watch(localData, () => {
  localData[props.config.field] = localData[props.config.field + '_value'] + localData[props.config.field + '_unit'];
});
</script>
<style scoped lang="scss">
.input-number-with-select {
  display: inline-flex;
  width: 100%;

  :deep(.el-input-number .el-input__wrapper) {
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
  }

  :deep(.el-input-number .el-input__inner) {
    text-align: left;
  }

  :deep(.el-select .el-select__wrapper) {
    border-left: none;
    border-top-left-radius: 0;
    border-bottom-left-radius: 0;
  }
}
</style>
