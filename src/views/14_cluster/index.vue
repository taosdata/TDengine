<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.cluster')"></MainContentHeader>
    <div class="content">
      <el-tabs value="dnodes">
        <el-tab-pane name="dnodes" :label="$t('topic.dnodes')">
          <MgDnodes @sendData='getData'></MgDnodes>
        </el-tab-pane>
        <el-tab-pane name="mnodes" :label="$t('topic.mnodes')">
          <MgMnodes :dnodes='dnodeLists'></MgMnodes>
        </el-tab-pane>
        <el-tab-pane name="qnodes" :label="$t('topic.qnodes')">
          <MgQnodes :dnodes='dnodeLists'></MgQnodes>
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
export default {
  name: "Cluster",
  components:{
    MgDnodes,MgMnodes,MgQnodes
  },
  data() {
    return {
      message: "这是Management页面",
      dnodeLists:[]
    };
  },
  methods:{
    getData(data){
      this.dnodeLists=data
    }
  }
};
</script>