<template>
  <div class="page-wrapper">
    <MainContentHeader :title="$t('route.healthreport')"></MainContentHeader>

    <div class="content">
      <!-- <el-tabs value="basics">
        <el-tab-pane name="basics" label="Basic Operating Status">
          <BasicOperating></BasicOperating>
        </el-tab-pane>
        <el-tab-pane name="dnode" label="DNodes">
          <DnodeHealth></DnodeHealth>
        </el-tab-pane>
        <el-tab-pane name="health" label="Health">
          <Health></Health>
        </el-tab-pane>
      </el-tabs> -->
      <section class="basics">
        <p class="title">
          <span>{{$t('health.basic')}}</span>
        </p>
        <BasicOperating></BasicOperating>
      </section>
      <section class="dnodes">
        <p class="title">
          <span>{{$t('health.dnodes')}}</span>
        </p>
        <DnodeHealth></DnodeHealth>
      </section>
      <section class="health">
        <p class="title">
          <span>{{$t('health.health')}}</span>
        </p>
        <Health></Health>
      </section>
    </div>
  </div>
</template>
<script>
// import MgUser from './views/user.vue'
import BasicOperating from "./views/basics.vue";
import DnodeHealth from "./views/dnodes.vue";
import Health from "./views/health.vue";
export default {
  name: "HealthReport",
  components: {
    BasicOperating,
    DnodeHealth,
    Health,
  },
  data() {
    return {
      message: "这是healthreport",
      dnodeLists: [],
    };
  },
  methods: {},
};
</script>
<style lang="scss" scoped>
section{
    // flex:1;
    border-radius: 10px;
    padding: 15px;
    box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.05);
    margin-bottom:15px;
    &:nth-child(2){
        height:300px;
    }
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
</style>