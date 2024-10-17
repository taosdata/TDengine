<template>
  <div class="page-wrapper">
    <div class="content">
      <el-tabs v-model="$store.state.app.activeName" @tab-click="clickTab">
        <el-tab-pane
          name="datasource"
          :label="$t('topic.datasource')"
        >
          <DbSource ref="dbsource"></DbSource>
        </el-tab-pane>
        <el-tab-pane
          name="agent"
          :label="$t('topic.agent')"
        >
          <Agents ref="agents"></Agents>
        </el-tab-pane>
        <el-tab-pane
          name="datacollection"
          :label="$t('topic.datacollection')"
          v-if="!isOem"
        >
          <DataCollection></DataCollection>
        </el-tab-pane>

        <!-- <el-tab-pane name="csv" :label="$t('topic.csv')">
          <DataCSV></DataCSV>
        </el-tab-pane> -->
      </el-tabs>
    </div>
  </div>
</template>

<script>
import DataCollection from "./dataCollection.vue";
import DbSource from "./dbSource.vue";
import DataCSV from "./dataCSV.vue";
import Agents from "../components/agents.vue";
export default {
  components: {
    DataCollection,
    DbSource,
    DataCSV,
    Agents
  },
  data() {
    return {
      piDisable: false,
      opcDisable: false,
      isOem:
        process.env.VUE_APP_CUS_NAME &&
        process.env.VUE_APP_CUS_NAME !== "TDengine",
      active:"datasource",
    };
  },
  mounted() {
    this.clickTab();
  },
  methods: {
    async clickTab() {
      this.$refs.dbsource.currentName = "dbsource";
      if (this.active == "datasource") {
        await this.$refs.dbsource.getData();
        await this.$refs.dbsource.reloadTable();
      }
    },
    setActive(val) {
      this.$store.app.state.
      this.active = val;
    }
  },
};
</script>

<style lang="scss" scoped>
::v-deep.el-form-item__content {
  margin-left: 0px !important;
}
.content {
  border: none;
  padding: 0px;
}
</style>
