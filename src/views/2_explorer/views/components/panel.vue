<template>
  <div class="panel">
    <el-tabs type="border-card" size="mini" v-model="activeTab">
      <el-tab-pane name="grid">
        <div class="flexCenter" slot="label">
          <Icon name="table" class="tab_icon"></Icon>
          <span>{{ $t("console.grid") }}</span>
          <el-tooltip
            effect="light"
            :content="$t('console.cellCopyTip')"
            :open-delay='1000'
            placement="bottom"
          >
            <i
              size="mini"
              class="el-icon-info info-icon"
            ></i>
          </el-tooltip>
        </div>
        <GridView></GridView>
      </el-tab-pane>
      <el-tab-pane name="chart">
        <div class="flexCenter" slot="label">
          <Icon name="chart" class="tab_icon"></Icon>
          <span>{{ $t("console.chart") }}</span>
        </div>
        <ChartView></ChartView>
      </el-tab-pane>
      <el-tab-pane name="favorites">
        <div class="flexCenter" slot="label">
          <Icon name="favorite_fill" class="tab_icon"></Icon>
          <span>{{ $t("console.favorites") }}</span>
        </div>
        <FavoriteView></FavoriteView>
      </el-tab-pane>
      <el-tab-pane name="log">
        <div class="flexCenter" slot="label">
          <Icon name="console_dblist" class="tab_icon"></Icon>
          <span>{{ $t("console.log") }}</span>
        </div>
        <LogView :key="activeTab"></LogView>
      </el-tab-pane>
    </el-tabs>
    <div class="panel-right">
      <p class="data-nums">{{ dataSource.length }} rows</p>
      <el-button :disabled="dataSource.length == 0" @click="exportFile" plain size="mini" style="font-size:14px;">{{ $t("console.export") }}</el-button>
    </div>
  </div>
</template>

<script>
import GridView from "./grid.vue";
import ChartView from "./chart.vue";
import FavoriteView from "./FavoriteList";
import LogView from "./log.vue";
import { mapState } from "vuex";
import FileSaver from "file-saver";
import { convertToCsvData } from '@/utils';

export default {
  components: {
    GridView,
    ChartView,
    FavoriteView,
    LogView,
  },
  computed: {
    ...mapState({
      dataSource: state => state.console.repeatResult,
      head: state => {
        let result = {};
        state.console.head.forEach(item => {
          result[item] = item;
        });
        return result;
      },
      headArr: state => {
        return state.console.head
      }
    }),
    activeTab: {
      get: function () {
        return this.$store.state.console.activeTab;
      },
      set: function (val) {
        this.$store.commit("console/SET_ACTIVE_TAB", val);
      },
    },
  },
  mounted() {
  },
  methods: {

    refresh() {
      this.$emit("refresh");
    },
    exportFile() {
      const FileName = "data.csv";
      const data = convertToCsvData(this.dataSource, this.headArr)
      const blob = new Blob([data], {
        type: "text/csv;charset=utf-8;",
      });
      FileSaver.saveAs(blob, FileName);
    },
  },
};
</script>

<style scoped>
.flexCenter {
  height: 100%;
}
.el-tabs--border-card {
  box-shadow: none;
}
.panel {
  margin-top: 15px;
  height: 100%;
  min-height: 300px;
  position: relative;
  /* overflow: hidden; */
}

/* .panel::v-deep .el-tabs */
.panel::v-deep .el-tabs__content {
  flex: 1;
  padding: 15px !important;
  overflow: initial!important;
}
.panel::v-deep .el-tabs__content > .el-tab-pane {
  left: 15px;
  right: 15px;
}
.panel::v-deep .el-tabs {
  border-left: none;
  border-bottom: none;
}

.panel::v-deep .el-tabs--border-card > .el-tabs__header {
  padding-right: 230px;
}
.tab_icon {
  width: 19px;
  height: 19px;
  cursor: pointer;
  margin-right: 5px;
}
.panel-right {
  position: absolute;
  right: 20px;
  top: 8px;
  color: #333333;
  display: flex;
}
.data-nums {
  font-size: 17px;
  margin-right: 10px;
}
.refresh-btn {
  margin-left: 20px;
}
.panel::v-deep .el-tabs--border-card > .el-tabs__header .el-tabs__item {
  font-size: 16px;
}
</style>
