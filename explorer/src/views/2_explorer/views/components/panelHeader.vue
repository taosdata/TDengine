<template>
  <div class="panel-title">
    <slot></slot>
  </div>
</template>

<script>
export default {};
</script>

<style lang="scss" scoped>
.panel-title {
  width: 100%;
  color: #909399;
  display: flex;
  align-items: center;
  background-color: #f5f7fa;
  border-bottom: 1px solid #dfe4ed;
  height: 39px;
  padding: 0 10px;
  position: relative;
}
</style>
