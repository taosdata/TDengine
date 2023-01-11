<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.dataIn')"></MainContentHeader>
    
    <div class="content">
      <el-tabs value="datain">
        <el-tab-pane name="datain" :label="$t('topic.datain')">
          <DataIn></DataIn>
        </el-tab-pane>
        <el-tab-pane name="datasource" :label="$t('topic.datasource')">
          <DataSource></DataSource>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>

<script>
<<<<<<< HEAD
export default {
=======
import DataIn from './views/main.vue'
import DataSource from './views/dataSource.vue'
export default {
  components:{
    DataIn,
    DataSource
  },
>>>>>>> 8895288015f78987fe1192eeaa34d5fe7903a71c
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
