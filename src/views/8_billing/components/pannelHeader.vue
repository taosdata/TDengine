<template>
  <div class="pannel-header">
    <span class="title">{{ title }}</span>
    <slot></slot>
  </div>
</template>

<script>
  export default {
    props: {
      title: {
        type: String,
        default: "",
      },
    },
  };
</script>

<style lang="scss" scoped>
  .pannel-header {
    display: flex;
    align-items: center;
    position: relative;
    .title {
      font-size: 18px;
      line-height: 28px;
      color: rgb(20, 30, 53);
      letter-spacing: -0.01em;
      font-weight: 700;
    }
  }
</style>
