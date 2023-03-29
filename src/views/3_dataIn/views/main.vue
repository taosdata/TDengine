<template>
  <div class="page-wrapper">
    <div class="content">
      <el-tabs :value="active">
        <el-tab-pane name="datacollection" :label="$t('topic.datacollection')" v-if="!isOem">
          <DataIn ></DataIn>
        </el-tab-pane>
        <el-tab-pane name="datasource" :label="$t('topic.datasource')" v-if="!isOem">
          <DbSource ></DbSource>
        </el-tab-pane>
        <el-tab-pane name="csv" :label="$t('topic.csv')">
          <DataCSV></DataCSV>
        </el-tab-pane>
        <!-- <el-tab-pane name='pi' label='PI'>
          <PlantInformation></PlantInformation>
        </el-tab-pane>
        <el-tab-pane name="opc" label="OPC">
          <OpcView></OpcView>
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
import PlantInformation from './plantInformation.vue'
import OpcView from './opcView.vue'
export default {
  components: {
    DataIn,
    DbSource,
    SourceContent,
    DataCSV,
    PlantInformation,
    OpcView
  },
  data() {
    return {
      isOem:process.env.VUE_APP_CUS_NAME&&process.env.VUE_APP_CUS_NAME!=='TDengine',
      active:process.env.VUE_APP_CUS_NAME&&process.env.VUE_APP_CUS_NAME!=='TDengine'?'csv':'datacollection'
    };
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
