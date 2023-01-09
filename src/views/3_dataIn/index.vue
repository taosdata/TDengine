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
import DataIn from './views/main.vue'
import DataSource from './views/dataSource.vue'
export default {
  components:{
    DataIn,
    DataSource
  },
  data() {
    return {};
  },
  watch: {},
  methods: {},
};
</script>

<style lang="scss"></style>
