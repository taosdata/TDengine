<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.cluster')"></MainContentHeader>
    <div class="content">
      <el-tabs value="dnodes">
        <el-tab-pane name="dnodes" :label="$t('topic.dnodes')">
          <MgDnodes></MgDnodes>
        </el-tab-pane>
        <el-tab-pane name="mnodes" :label="$t('topic.mnodes')">
          <MgMnodes></MgMnodes>
        </el-tab-pane>
        <el-tab-pane name="qnodes" :label="$t('topic.qnodes')">
          <MgQnodes></MgQnodes>
        </el-tab-pane>
        <el-tab-pane name="udf" :label="$t('topic.udf')">
          <MgUdf></MgUdf>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>
<script>
// import MgUser from './views/user.vue'
import MgDnodes from './views/dnodes.vue'
import MgMnodes from './views/mnodes.vue'
import MgQnodes from './views/qnodes.vue'
import MgUdf from './views/udf.vue'
export default {
  name: "Cluster",
  components:{
    MgDnodes,MgMnodes,MgQnodes,MgUdf
  },
  data() {
    return {
      message: "这是Management页面",
    };
  },
};
</script>