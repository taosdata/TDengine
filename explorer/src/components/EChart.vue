<template>
  <div :style="{ width: width, height: height }"></div>
</template>

<script>
import * as echarts from "echarts";
import ResizeMixin from "./ResizeHandler";
// 事件监听回调列表
const eventList = [
  "click",
  "finished",
  "mouseover",
  "mousemove",
  "updateAxisPointer"
];
// const zrEventList = ["mouseover"];
export default {
  props: {
    width: {
      type: String,
      default: "100%"
    },
    height: {
      type: String,
      default: "350px"
    },
    chartOption: {
      type: Object,
      required: true
    },
    notMerge: {
      type: Boolean,
      default: false
    }
  },
  mixins: [ResizeMixin],
  data() {
    return {
      chart: null
    };
  },
  watch: {
    chartOption: {
      deep: true,
      handler(val) {
        this.setOptions(val);
      }
    }
  },
  mounted() {
    this.$nextTick(() => {
      this.initChart();
    });
  },
  beforeDestroy() {
    this.chart?.dispose();
    this.chart = null;
  },
  methods: {
    initChart() {
      this.chart = echarts.init(this.$el);
      eventList.forEach(event => {
        this.chart.on(event, "xAxis", (...rest) => {
          this.$emit(event, ...rest);
        });
      });
      // zrEventList.forEach(event => {
      //   this.chart.getZr().on(event, (...rest) => {
      //     console.log("触发");
      //     this.$emit(event, ...rest);
      //   });
      // });
      this.$emit("chartMounted", this.chart);
      this.setOptions(this.chartOption);
    },
    setOptions(chartOption) {
      this.chart.setOption(chartOption, this.notMerge);
      this.chart.resize();
    }
  }
};
</script>

<style scoped>
.chart {
  width: 100%;
}
</style>
