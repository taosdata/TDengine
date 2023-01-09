<template>
  <div class="page-wrapper">
    <div class="content">
      <el-tabs value="datacollection">
        <el-tab-pane name="datacollection" :label="$t('topic.datacollection')">
          <DataIn></DataIn>
        </el-tab-pane>
        <el-tab-pane name="datasource" :label="$t('topic.datasource')">
          <DataSource></DataSource>
        </el-tab-pane>
        <el-tab-pane name="csv" :label="$t('topic.csv')">
          <DataCSV></DataCSV>
        </el-tab-pane>
      </el-tabs>
    </div>
    
  </div>
</template>

<script>
import DataIn from "./dataIn.vue";
import DataSource from "./dataSource.vue";
import SourceContent from './sourceContent.vue'
import DataCSV from './dataCSV.vue'
export default {
  components: {
    DataIn,
    DataSource,
    SourceContent,
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
