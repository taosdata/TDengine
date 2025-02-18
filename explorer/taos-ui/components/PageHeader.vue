<template>
  <div class="page-header block-border">
    <div class="flex items-center">
      <div class="page-title" @click="goBack">{{ props.title }}</div>
      <slot></slot>
    </div>
    <slot name="right"></slot>
  </div>
</template>

<script lang="ts" setup>
import { useRoute, useRouter } from 'hooks/useCurrentRouter';
const props = defineProps<{
  title: string;
}>();
const router = useRouter();
const route = useRoute();
function goBack() {
  router.push(route.matched[1]?.path || '/');
}
</script>

<style lang="scss" scoped>
.page-header {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: space-between;
  height: 55px;
  padding: 0 30px;
  margin-bottom: 5px;
}

.page-title {
  height: 30px;
  padding-left: 15px;
  font-size: 28px;
  font-weight: 700;
  line-height: 30px;
  color: #444;

  // text-transform: capitalize;
  cursor: pointer;
  border-left: 4px solid #4259ce;
  border-radius: 2px;
}
</style>
