<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.dataIn')"></MainContentHeader>
    
    <div class="content">
      <router-view></router-view>
    </div>
  </div>
</template>

<script>
import DataIn from './views/main.vue'
import DbSource from './views/dbSource.vue'
import DataCSV from './views/dataCSV.vue'
export default {
  components:{
    DataIn,
    DbSource,
    DataCSV
  },
  data() {
    return {};
  },
  watch: {},
  methods: {},
};
</script>

<style lang="scss">
::v-deep.el-form-item__content {
  margin-left: 0px !important;
}
</style>
