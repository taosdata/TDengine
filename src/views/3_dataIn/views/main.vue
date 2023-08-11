<template>
  <div class="page-wrapper">
    <div class="content">
      <el-tabs v-model="active" @tab-click='clickTab'>
        <el-tab-pane name="datacollection" :label="$t('topic.datacollection')" v-if="!isOem">
          <DataIn ></DataIn>
        </el-tab-pane>
        <el-tab-pane name="datasource" :label="$t('topic.datasource')" v-if="!isOem" :disabled='sourceDisabled'>
          <DbSource ref="dbsource"></DbSource>
        </el-tab-pane>
        <!-- <el-tab-pane name="csv" :label="$t('topic.csv')">
          <DataCSV></DataCSV>
        </el-tab-pane> -->
      </el-tabs>
    </div>
    
  </div>
</template>

<script>
import DataIn from "./dataIn.vue";
import DbSource from "./dbSource.vue";
import SourceContent from './sourceContent.vue'
import DataCSV from './dataCSV.vue'
export default {
  components: {
    DataIn,
    DbSource,
    SourceContent,
    DataCSV
  },
  data() {
    return {
      sourceDisabled:true,
      piDisable:false,
      opcDisable:false,
      isOem:process.env.VUE_APP_CUS_NAME&&process.env.VUE_APP_CUS_NAME!=='TDengine',
      active:process.env.VUE_APP_CUS_NAME&&process.env.VUE_APP_CUS_NAME!=='TDengine'?'csv':'datacollection'
    };
  },
  methods: {
    clickTab(){
      this.$refs.dbsource.currentName='dbsource'
      if(this.active=='datasource'){
        this.$refs.dbsource.getData()
        this.$refs.dbsource.reloadTable()
      }
    }
  },
};
</script>

<style lang="scss" scoped>
::v-deep.el-form-item__content {
  margin-left: 0px !important;
}
.content{
  border:none;
  padding:0px;
}
</style>
