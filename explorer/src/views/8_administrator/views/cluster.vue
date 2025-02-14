<template>
  <div class="page-wrapper">
    <div class="content">
      <section>
        <p class="title">
          <span>{{ $t('taoscluster.dnodes') }}</span>
        </p>
        <MgDnodes @send-data="getData"></MgDnodes>
      </section>
      <section>
        <p class="title">
          <span>{{ $t('taoscluster.mnodes') }}</span>
        </p>
        <MgMnodes :dnodes="dnodeLists"></MgMnodes>
      </section>
      <section>
        <p class="title">
          <span>{{ $t('taoscluster.qnodes') }}</span>
        </p>
        <MgQnodes :dnodes="dnodeLists"></MgQnodes>
      </section>
    </div>
  </div>
</template>
<script setup lang="ts">
import MgDnodes from './components/clusters/dnodes.vue';
import MgMnodes from './components/clusters/mnodes.vue';
import MgQnodes from './components/clusters/qnodes.vue';

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

.title {
  padding: 8px 16px;
  margin: 10px 0;
  font-size: 16px;
  color: #333;
  background-color: #ecf8ff;
  border-left: 5px solid #50bfff;
  border-radius: 4px;
}

.content {
  padding: 0;
  border: none;

  :deep(.el-button) {
    font-size: 14px;
  }
}
</style>
