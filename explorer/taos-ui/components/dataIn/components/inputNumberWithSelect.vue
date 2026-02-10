<template>
  <div class="input-number-with-select">
    <el-input-number
      v-model="val"
      style="width: 80%"
      :placeholder="config.placeholder"
      :max="config.max"
      :min="config.min"
      :controls="false"
      @change="onChange"
    >
    </el-input-number>
    <el-select v-model="unit" style="width: 20%" @change="onChange">
      <el-option v-for="item in options" :key="item.value" v-bind="item" :title="item.label"></el-option>
    </el-select>
  </div>
</template>
<script setup lang="ts">
const props = withDefaults(
  defineProps<{
    config: Record<string, any>;
    modelValue: string;
    options: Record<string, any>;
  }>(),
  {}
);

const val = ref(0);
const unit = ref('');
const regexInputValue = /^(\d+)([a-zA-Z%]+)$/;

// watch(
//   () => props.modelValue,
//   newVal => {

//   }
// );
onMounted(() => {
  if (props.modelValue && regexInputValue.test(props.modelValue)) {
    const matchItems = props.modelValue.match(regexInputValue);
    if (matchItems && matchItems.length === 3) {
      val.value = parseInt(matchItems[1]);
      unit.value = matchItems[2];
    }
  }
});

const emit = defineEmits(['update:modelValue']);
const onChange = () => {
  emit('update:modelValue', `${val.value}${unit.value}`);
};
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

  /* 新增：统一数字输入与下拉已选文本颜色（普通/禁用） */
  :deep(.el-input-number .el-input__inner),
  :deep(.el-input-number.is-disabled .el-input__inner),
  :deep(.el-input-number .el-input__inner[disabled]) {
    color: var(--el-text-color-regular) !important;
    -webkit-text-fill-color: var(--el-text-color-regular) !important;
    opacity: 1 !important;
  }

  :deep(.el-select__wrapper .el-select__selected-item),
  :deep(.el-select__wrapper.is-disabled .el-select__selected-item) {
    color: var(--el-text-color-regular) !important;
    -webkit-text-fill-color: var(--el-text-color-regular) !important;
    opacity: 1 !important;
  }
}
</style>
