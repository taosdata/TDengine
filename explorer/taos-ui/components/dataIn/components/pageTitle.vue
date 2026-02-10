<template>
  <div class="title">
    <span>{{ props.title }}</span>
    <div v-if="props.isEnd" class="flex-end">
      <slot></slot>
      <el-button
        link
        type="primary"
        size="default"
        icon="Refresh"
        class="action-button"
        :disabled="requestIng || props.isCommunity"
        @click="refresh"
        >{{ t('common.refresh') }}</el-button
      >
      <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!props.isCommunity || props.isDisabledAdd">
        <template #content>
          <span v-dompurify-html="t('common.communityTip')"></span>
        </template>
        <el-button size="default" icon="Plus" link type="primary" class="action-button" @click="add">{{ props.addTitle }}</el-button>
      </el-tooltip>
    </div>
  </div>
</template>
<script setup lang="ts">
import { t } from 'locales';
const props = withDefaults(
  defineProps<{
    isEnd?: boolean;
    isDisabledAdd?: boolean;
    title: string;
    addTitle: string;
    requestIng: boolean;
    isCommunity?: boolean | undefined;
  }>(),
  {
    isEnd: true,
    isCommunity: false,
    isDisabledAdd: false
  }
);
const emit = defineEmits(['refresh', 'add']);
function refresh() {
  emit('refresh');
}
function add() {
  emit('add');
}
</script>
<style scoped lang="scss">
.title {
  display: flex;
  justify-content: space-between;
  justify-items: center;
  height: 44px;
  padding: 12px 16px;
  margin: 10px 0;
  font-size: 16px;
  color: #333;
  background-color: #ecf8ff;
  border-left: 5px solid #50bfff;
  border-radius: 4px;

  .flex-end {
    display: flex;
    align-items: center;

    :deep(.el-button) {
      font-size: 14px;
      padding: 0 10px;
      margin: 0;
    }

    :deep(.el-button):hover {
      color: #4259ce;
      border: 1px solid #4259ce !important;
    }

    /* 统一按钮样式 */
    :deep(.action-button.el-button.is-link) {
      padding: 0 10px;
      margin: 0;
    }
  }
}
</style>
