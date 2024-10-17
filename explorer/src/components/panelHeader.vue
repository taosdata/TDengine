<template>
  <div class="panel_header">
    <div v-if="$slots.title || title" class="title">
      <slot name="title">{{ title }}</slot>
    </div>
    <slot></slot>
  </div>
</template>

<script>
  export default {
    props: ["title"],
    data() {
      return {};
    },
  };
</script>

<style scoped>
  .panel_header {
    margin-top: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .title {
    font-size: 20px;
    font-weight: bold;
    color: rgb(1, 17, 42);
    display: flex;
    align-items: center;
  }
</style>
