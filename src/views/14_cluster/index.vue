<template>
  <div class="page-wrapper">
    <!-- <MainContentHeader :title="$t('route.cluster')"></MainContentHeader> -->
    <div class="content">
      <!-- <el-tabs value="dnodes">
        <el-tab-pane name="dnodes" :label="$t('topic.dnodes')">
          <MgDnodes @sendData="getData"></MgDnodes>
        </el-tab-pane>
        <el-tab-pane name="mnodes" :label="$t('topic.mnodes')">
          <MgMnodes :dnodes="dnodeLists"></MgMnodes>
        </el-tab-pane>
        <el-tab-pane name="qnodes" :label="$t('topic.qnodes')">
          <MgQnodes :dnodes="dnodeLists"></MgQnodes>
        </el-tab-pane>
      </el-tabs> -->
      <section>
        <p class="title">
          <span>{{ $t("taoscluster.dnodes") }}</span>
        </p>
        <MgDnodes @sendData="getData"></MgDnodes>
      </section>
      <section>
        <p class="title">
          <span>{{ $t("taoscluster.mnodes") }}</span>
        </p>
        <MgMnodes :dnodes="dnodeLists"></MgMnodes>
      </section>
      <section>
        <p class="title">
          <span>{{ $t("taoscluster.qnodes") }}</span>
        </p>
        <MgQnodes :dnodes="dnodeLists"></MgQnodes>
      </section>
    </div>
  </div>
</template>
<script>
// import MgUser from './views/user.vue'
import MgDnodes from "./views/dnodes.vue";
import MgMnodes from "./views/mnodes.vue";
import MgQnodes from "./views/qnodes.vue";
export default {
  name: "Cluster",
  components: {
    MgDnodes,
    MgMnodes,
    MgQnodes,
  },
  data() {
    return {
      message: "这是Management页面",
      dnodeLists: [],
    };
  },
  methods: {
    getData(data) {
      this.dnodeLists = data.length>0?data.filter(item=>item.status!='offline'):[];
    },
  },
};
</script>
<style lang="scss" scoped>
section{
    border-radius: 10px;
    // box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.05);
    margin-bottom:15px;
    position: relative;
    height:200px;
    // &:nth-child(2){
    //     height:220px;
    // }
    // &:first-child{
    //   flex:1;
    // }
    // &:nth-child(3){
    //   flex:auto;
    // }
}
.title{
    background-color: #ecf8ff;
    border-left-color: #50bfff;
    color: #333;
    border-left-width: 5px;
    border-left-style: solid;
    border-radius: 4px;
    font-size: 16px;
    margin: 10px 0;
    padding: 8px 16px;
}
.content{
  border:none;
  padding:0px;
  :deep{
    .el-button{
      font-size:14px;
    }
  }
}
</style>