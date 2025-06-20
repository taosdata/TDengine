<template>
  <div class="detail">
    <component
      :is="currentDetailComponent"
      v-if="currentDetailComponent"
      :key="componentKey"
      v-bind="currentDetailComponentConfig.props"
      v-on="currentDetailComponentConfig.listeners"
      @cancel="backSqlPart"
    ></component>
    <slot v-else></slot>
  </div>
</template>

<script lang="ts" setup>
import DatabaseCreate from './createDbForm.vue';
import StableCreate from './createStable/index.vue';
import NormalTableCreate from './createStable/createTable.vue';
import VirtualNormalTableCreate from './createStable/createVTable.vue';
import Info from './info.vue';
import TableCreate from './createSubTbForm.vue';
import AdvancedFilter from './advanceFilter.vue';
import { currentDetailComponentConfig, backSqlPart } from './utils';

const components = {
  DatabaseCreate,
  StableCreate,
  NormalTableCreate,
  VirtualNormalTableCreate,
  Info,
  TableCreate,
  AdvancedFilter
};
const currentDetailComponent = computed(
  () => components[currentDetailComponentConfig.component as keyof typeof components]
);
const componentKey = ref(0);
watch(
  () => currentDetailComponentConfig.props,
  () => {
    componentKey.value++;
  }
);
</script>

<style lang="scss" scoped>
.detail {
  --group-prepend: 200px;
  --group-append: 150px;
  --group-margin-top: 0px;

  position: relative;
  height: 100%;
  max-height: 100vh;
  padding: 0 15px;
  overflow: auto;

  &:deep(.el-input.is-disabled .el-input__inner),
  &:deep(.el-input-group__append),
  &:deep(.el-input-group__prepend) {
    color: #606266;
    background-color: unset;

    .el-button.is-disabled,
    .el-button.is-disabled:hover,
    .el-button.is-disabled:focus {
      background-color: transparent;
      border-color: transparent;
    }
  }

  &:deep(.el-input-group__prepend) {
    width: var(--group-prepend);
    padding-left: 15px;
  }

  &:deep(.el-input.is-disabled .el-input__inner) {
    color: #606266;
  }

  &:deep(.el-form-item__label) {
    font-weight: 500;
  }

  &:deep(.flex-center .el-select .el-input__inner) {
    border-color: #dcdfe6;
    border-right: none;
    border-top-right-radius: 0;
    border-bottom-right-radius: 0;
  }

  &:deep(.flex-center .el-input .el-input__inner) {
    border-color: #dcdfe6;
    border-top-left-radius: 0;
    border-bottom-left-radius: 0;
  }
}
</style>
