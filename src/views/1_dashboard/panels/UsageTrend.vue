<template>
  <div class="usage-trend">
    <panelHeader :title="$t('dashboard.usageTrend')"></panelHeader>
    <section class="header-selecter">
      <el-switch v-model="autoRefresh" @change="autoChange"> </el-switch>
      <span class="refresh-text">{{ autoRefresh ? $t("dashboard.enableAuto") : $t("dashboard.disableAuto") }}</span>
      <el-date-picker
        style="width: 300px"
        v-model="date"
        type="datetimerange"
        size="mini"
        :clearable="false"
        format="yyyy-MM-dd HH:mm"
        :picker-options="$root.pickerOptions"
        range-separator="-"
        :start-placeholder="$t('start')"
        :end-placeholder="$t('end')"
        value-format="yyyy-MM-dd HH:mm:ss"
        @focus="timeSelectFocus"
        @blur="getData()"
      >
      </el-date-picker>
      <el-tooltip class="item" effect="light" :content="$t('utcTip')" placement="top-start">
        <el-icon class="el-icon-info info-icon"></el-icon>
      </el-tooltip>
    </section>
    <section class="chart-wrapper">
      <div class="chart-detail">
        <p class="value" :title="egressCurrent[1]">{{ egressCurrent[1] }} MB</p>
        <p class="time">{{ egressCurrent[0] }}(UTC)</p>
        <p class="title">{{ $t("dashboard.egressVolume") }}</p>
      </div>
      <div class="chart-content">
        <Echart
          @updateAxisPointer="e => updateAxisPointer(e, 0)"
          @chartMounted="e => chartMounted(e, 0)"
          :height="height"
          :chartOption="egressChartOption"
        ></Echart>
      </div>
    </section>
    <section class="chart-wrapper">
      <div class="chart-detail">
        <p class="value" :title="ingressCurrent[1]">{{ ingressCurrent[1] }} MB</p>
        <p class="time">{{ ingressCurrent[0] }}(UTC)</p>
        <p class="title">{{ $t("dashboard.ingressVolume") }}</p>
      </div>
      <div class="chart-content">
        <Echart
          @updateAxisPointer="e => updateAxisPointer(e, 1)"
          @chartMounted="e => chartMounted(e, 1)"
          :height="height"
          :chartOption="ingressChartOption"
        ></Echart>
      </div>
    </section>
    <section class="chart-wrapper">
      <div class="chart-detail">
        <p class="value" :title="storageCurrent[1]">{{ storageCurrent[1] }} GB</p>
        <p class="time">{{ storageCurrent[0] }}(UTC)</p>
        <p class="title">{{ $t("dashboard.storage") }}</p>
      </div>
      <div class="chart-content">
        <Echart
          @updateAxisPointer="e => updateAxisPointer(e, 2)"
          @chartMounted="e => chartMounted(e, 2)"
          :height="height"
          :chartOption="storageChartOption"
        ></Echart>
      </div>
    </section>
    <!-- <el-divider></el-divider> -->
    <section class="chart-wrapper" style="margin-top: 20px">
      <div class="chart-detail">
        <p class="value" :title="insertQPSCurrent[1]">{{ insertQPSCurrent[1] }} QTY</p>
        <p class="time">{{ insertQPSCurrent[0] }}(UTC)</p>
        <p class="title">{{ $t("dashboard.inserts") }}</p>
      </div>
      <div class="chart-content">
        <Echart
          @updateAxisPointer="e => updateAxisPointer(e, 3)"
          @chartMounted="e => chartMounted(e, 3)"
          :height="height"
          :chartOption="insertQPSChartOption"
        ></Echart>
      </div>
    </section>
    <!-- <section class="chart-wrapper">
      <div class="chart-detail">
        <p class="value"></p>
        <p class="time">{{ insertTimeCurrent[0] }}(UTC)</p>
        <p class="title">{{ $t("dashboard.insert") }} {{ $t("dashboard.response") }}</p>
      </div>
      <div class="chart-content">
        <Echart @updateAxisPointer="(e)=>updateAxisPointer(e,0)" @chartMounted="(e)=>chartMounted(e,0)" :height="height" :chartOption="insertTimeChartOption"></Echart>
      </div>
    </section> -->
    <section class="chart-wrapper">
      <div class="chart-detail">
        <p class="value" :title="queryQPSCurrent[1]">{{ queryQPSCurrent[1] }} QTY</p>
        <p class="time">{{ queryQPSCurrent[0] }}(UTC)</p>
        <p class="title">{{ $t("dashboard.queries") }}</p>
      </div>
      <div class="chart-content">
        <Echart
          @updateAxisPointer="e => updateAxisPointer(e, 5)"
          @chartMounted="e => chartMounted(e, 5)"
          :height="height"
          :chartOption="queryQPSChartOption"
        ></Echart>
      </div>
    </section>
    <!-- <section class="chart-wrapper">
      <div class="chart-detail">
        <p class="value"></p>
        <p class="time">{{ queryTimeCurrent[0] }}(UTC)</p>
        <p class="title">{{ $t("dashboard.query") }} {{ $t("dashboard.response") }}</p>
      </div>
      <div class="chart-content">
        <Echart @updateAxisPointer="(e)=>updateAxisPointer(e,0)" @chartMounted="(e)=>chartMounted(e,0)" :height="height" :chartOption="queryTimeChartOption"></Echart>
      </div>
    </section> -->
  </div>
</template>

<script>
  import Echart from "@/components/EChart";
  import * as api from "@/api/dashboard";
  import { parseTime } from "@/utils/index";
  import * as options from "./utils/OptionFactory";
  import panelHeader from "@/components/panelHeader";
  import { OFFSETUTCTIME } from "@/const";
  // import * as echarts from "echarts";
  const time = process.env.VUE_APP_REFRESH_TIME;
  const timeFormat = "YYYY-MM-DD HH:mm:ss";
  const defaultOffsetTime = 24 * 60 * 60 * 1000;
  const chartList = ["egress", "ingress", "storage", "insertQPS", "insertTime", "queryQPS", "queryTime"];
  export default {
    components: {
      Echart,
      panelHeader,
    },
    data() {
      return {
        autoRefresh: localStorage.getItem("autoRefresh") === "false" ? false : true,
        date: [parseTime(Date.now() - defaultOffsetTime + OFFSETUTCTIME, timeFormat), parseTime(Date.now() + OFFSETUTCTIME, timeFormat)],
        egressChartOption: {},
        ingressChartOption: {},
        storageChartOption: {},
        egressCurrent: [0, 0],
        ingressCurrent: [0, 0],
        storageCurrent: [0, 0],
        height: "250px",
        insertQPSChartOption: {},
        insertTimeChartOption: {},
        queryQPSChartOption: {},
        queryTimeChartOption: {},
        insertQPSCurrent: [0, 0],
        queryQPSCurrent: [0, 0],
        insertTimeCurrent: [0, 0],
        queryTimeCurrent: [0, 0],
        timer: null,
        chartInstanceList: [],
        storageChartInstance: null,
      };
    },
    created() {
      if (this.$store.state.app.isGuide) {
        this.autoRefresh = false;
      }
      // 处理请求的间隔时间
      this.getData();
      const Fn = () => {
        if (!document.hidden) {
          this.timer && clearTimeout(this.timer);
          this.timer = null;
        } else {
          if (this.autoRefresh) {
            this.getData();
          }
        }
      };
      document.addEventListener("visibilitychange", Fn);
      this.$once("hook:beforeDestroy", () => {
        document.removeEventListener("visibilitychange", Fn);
        this.timer && clearTimeout(this.timer);
        this.autoRefresh = false;
        this.timer = null;
      });
    },

    methods: {
      autoChange(val) {
        if (val) {
          this.getData(true);
        } else {
          this.timer && clearTimeout(this.timer);
          this.timer = null;
        }
        localStorage.setItem("autoRefresh", val);
      },
      getData(updateTime = false) {
        !updateTime && this.timer && clearTimeout(this.timer) && (this.timer = null);
        if (!this.date.length) return;
        if (updateTime) {
          this.date.splice(1, 1, parseTime(Date.now() + OFFSETUTCTIME, timeFormat));
        }
        // 转为utc时间
        let dateParams = {
          from: this.date[0],
          to: this.date[1],
        };
        Promise.all([
          this.$store.dispatch("app/getClusterInfo"),
          api
            .getIngress(dateParams)
            .then(data => {
              [this.ingressChartOption, this.ingressCurrent = [0, 0]] = options.cpuUsage(data);
            })
            .catch(() => {
              this.ingressChartOption = options.cpuUsage([])[0];
              this.ingressCurrent = [0, 0];
            }),
          api
            .getEgress(dateParams)
            .then(data => {
              [this.egressChartOption, this.egressCurrent = [0, 0]] = options.cpuUsage(data);
            })
            .catch(() => {
              this.egressChartOption = options.cpuUsage([])[0];
              this.egressCurrent = [0, 0];
            }),
          api
            .getStorage(dateParams)
            .then(res => {
              [this.storageChartOption, this.storageCurrent = [0, 0]] = options.storageUsage(res);
            })
            .catch(() => {
              this.storageChartOption = options.storageUsage([])[0];
              this.storageCurrent = [0, 0];
            }),
          // api
          //   .getInserRes(dateParams)
          //   .then(res => {
          //     [this.insertTimeChartOption, this.insertTimeCurrent = [0, 0]] = options.insertResTime(res);
          //   })
          //   .catch(() => {
          //     this.insertTimeChartOption = options.insertResTime({})[0];
          //     this.insertTimeCurrent = [0, 0];
          //   }),
          // api
          //   .getQueryRes(dateParams)
          //   .then(res => {
          //     [this.queryTimeChartOption, this.queryTimeCurrent = [0, 0]] = options.queryResTime(res);
          //   })
          //   .catch(() => {
          //     this.queryTimeChartOption = options.queryResTime({})[0];
          //     this.queryTimeCurrent = [0, 0];
          //   }),
          api
            .getQPSInsert(dateParams)
            .then(res => {
              [this.insertQPSChartOption, this.insertQPSCurrent = [0, 0]] = options.insertQPS(res);
            })
            .catch(() => {
              this.insertQPSChartOption = options.insertQPS([])[0];
              this.insertQPSCurrent = [0, 0];
            }),
          api
            .getQPSQuery(dateParams)
            .then(res => {
              [this.queryQPSChartOption, this.queryQPSCurrent = [0, 0]] = options.queryQPS(res);
            })
            .catch(() => {
              this.queryQPSChartOption = options.queryQPS([])[0];
              this.queryQPSCurrent = [0, 0];
            }),
        ]).then(() => {
          if (!this.autoRefresh) return;
          // 添加定时刷新功能
          this.timer = setTimeout(() => {
            this.getData(true);
          }, time);
        });
      },
      // 保存所有图标的实例
      chartMounted() {
        // chart.group = "group1";
        // echarts.connect("group1");
        // this.chartInstanceList.push(chart);
        // if (this.chartInstanceList.length === 6) {
        //   echarts.connect(this.chartInstanceList);
        // }
      },
      storageChartMounted(chart) {
        this.storageChartInstance = chart;
      },
      updateAxisPointer(event, index) {
        if (Object.prototype.hasOwnProperty.call(event, "axesInfo") && event.axesInfo[0]) {
          let dataIndex = event.dataIndex;
          // 赋值
          let item = chartList[index];
          if (!this[item + "ChartOption"].series) return;
          this[item + "Current"] = this[item + "ChartOption"].series?.[0]?.data[dataIndex] || [0, 0];
        }
      },
      timeSelectFocus() {
        if (this.timer) {
          clearTimeout(this.timer);
          this.timer = null;
        }
      },
    },
  };
</script>

<style lang="scss" scoped>
  .header-selecter {
    display: flex;
    justify-content: flex-end;
    align-items: center;
    position: relative;
    .refresh-text {
      margin: 10px;
    }
    &::v-deep .el-date-editor .el-range__close-icon {
      width: 0;
    }
    &::v-deep .el-date-editor .el-range-input {
      width: 45%;
    }
    .info-icon {
      position: absolute;
      right: 5px;
    }
  }
  .chart-wrapper {
    display: flex;
    @extend .block-style;
    .chart-detail {
      padding: 55px 0;
      width: 200px;
      flex-shrink: 0;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: space-around;
      // margin-right: 30px;
      .title {
        font-size: 20px;
        // line-height: 60px;
        // font-weight: bold;
      }
      .value {
        // margin-top: 10px;
        font-size: 20px;
        // line-height: 60px;
        // font-weight: bold;
        // color: #636b7b;
      }
      .time {
        font-size: 14px;
      }
    }
    .chart-content {
      flex: 1;
    }
    & + .chart-wrapper {
      margin-top: 20px;
    }
  }
</style>
