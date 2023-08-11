<template>
  <div class="page-wrapper">
    <div class="content">
      <el-tabs v-model="active" @tab-click='clickTab'>
        <el-tab-pane name="datasource" :label="$t('topic.datasource')" v-if="!isOem">
          <DbSource ref="dbsource"></DbSource>
        </el-tab-pane>
      </el-tabs>
    </div>
    
  </div>
</template>

<script>
import DbSource from "./dbSource.vue";
export default {
  components: {
    DbSource,
  },
  data() {
    return {
      // sourceDisabled:true,
      isOem:process.env.VUE_APP_CUS_NAME&&process.env.VUE_APP_CUS_NAME!=='TDengine',
      active:'datasource'
    };
  },
  mounted() {
    this.$nextTick(() => {
      this.$refs.dbsource.currentName='dbsource'
    })
  },
  methods: {
    clickTab(){
      this.$refs.dbsource.currentName='dbsource'
      if(this.active=='datasource'){
        // this.$refs.dbsource.getData()
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
