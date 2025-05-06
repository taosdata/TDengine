<template>
  <div class="block-header">
    {{ props.title }}
    <slot></slot>
  </div>
</template>

<script setup lang="ts">
const props = withDefaults(defineProps<{ title: string }>(), {});
</script>

<style scoped lang="scss">
.block-header {
  font-size: 16px;
  font-weight: 600;
  color: #4259ce;
  text-align: left;
}
</style>
