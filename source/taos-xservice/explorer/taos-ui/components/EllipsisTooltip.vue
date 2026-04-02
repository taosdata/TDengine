<template>
  <el-tooltip :content="content" placement="top" :disabled="!isEllipsis" effect="dark">
    <template #content>
      <div class="overflow-text" :style="{ maxWidth: maxWidth }">
        {{ content }}
      </div>
    </template>
  </el-tooltip>
</template>

<script lang="ts" setup>
const props = defineProps<{
  content: string;
  maxWidth?: string;
  rows?: number; // 控制显示行数，默认2行
}>();
const isEllipsis = ref(false);

watch(
  () => props.content,
  () => {
    checkEllipsis();
  },
  { immediate: true }
);
function checkEllipsis() {
  const textEl = document.querySelector('.overflow-text') as HTMLElement;
  if (!textEl) return;

  // 强制回流以获取准确的 scrollHeight
  textEl.style.overflow = 'visible';
  textEl.style.webkitLineClamp = 'none';

  // 计算是否溢出
  isEllipsis.value = textEl.scrollHeight > textEl.clientHeight;

  // 恢复样式
  textEl.style.overflow = 'hidden';
  textEl.style.webkitLineClamp = (props.rows || 2).toString();
}
</script>

<style scoped>
.overflow-text {
  display: -webkit-box;
  overflow: hidden;
  text-overflow: ellipsis;
  -webkit-line-clamp: 2; /* 默认2行，可通过 props 覆盖 */
  word-break: break-all;
  -webkit-box-orient: vertical;
}
</style>
