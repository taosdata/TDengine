<template>
  <!-- <Test /> -->
  <div></div>
</template>

<script lang="ts" setup>
// import Test from './examples/sqlEditor.vue';
import { wsExport } from 'utils/wsexporter';
wsExport('test', 'test', 'test', false);
// console.log(wsExport);
</script>

<style lang="scss">
// 这里是专门用于pre代码块添加复制按钮的样式
.pre-code {
  position: relative;

  .copy-btn {
    position: absolute;
    top: 5px;
    right: 10px;
    display: none;
    padding: 2px 5px;
    font-size: 12px;
    color: #a31515;
    cursor: pointer;
    background: #fff;
    border: none;
  }

  .hljs-string,
  .hljs-subst {
    color: #a31515;
  }

  &:hover {
    .copy-btn {
      display: block !important;
    }
  }
}
</style>
