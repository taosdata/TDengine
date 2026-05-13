<template>
  <div class="page-wrapper">
    <div class="content">
      <MgDnodes @send-data="getData"></MgDnodes>
      <MgMnodes :dnodes="dnodeLists"></MgMnodes>
      <MgQnodes :dnodes="dnodeLists"></MgQnodes>
      <MgXnodes></MgXnodes>
      <MgAnodes></MgAnodes>
    </div>
  </div>
</template>
<script setup lang="ts">
import MgDnodes from './components/clusters/dnodes.vue';
import MgMnodes from './components/clusters/mnodes.vue';
import MgQnodes from './components/clusters/qnodes.vue';
import MgXnodes from './components/clusters/xnodes.vue';
import MgAnodes from './components/clusters/anodes.vue';

const dnodeLists = ref([]);
function getData(data: { length: number; filter: (arg0: (item: any) => boolean) => never[] }) {
  dnodeLists.value = data.length > 0 ? data.filter(item => item.status != 'offline') : [];
}
</script>
<style lang="scss" scoped>
section {
  position: relative;
  height: 200px;
  margin-bottom: 15px;
  border-radius: 10px;
}

.content {
  padding: 0;
  border: none;

  :deep(.el-button) {
    font-size: 14px;
  }
}
</style>
