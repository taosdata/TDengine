<template>
  <div class="chart">
    <el-form size="mini" :model="chartForm" inline ref="ruleForm">
      <el-form-item :label="$t('console.chartType')" prop="chartType">
        <el-select v-model="chartForm.chartType">
          <el-option
            v-for="(item,index) in chartTypes"
            :key="index"
            :label="item.value"
            :value="item.value"
          >
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('console.xAxis')" required prop="label">
        <el-select v-model="chartForm.label">
          <el-option
            v-for="(item, index) in field"
            :key="item + index"
            :label="item"
            :value="index"
          >
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('console.series')" required prop="series">
        <el-select
          size="mini"
          collapse-tags
          v-model="chartForm.series"
          multiple
        >
          <el-option
            v-for="(item, index) in field"
            :key="item + index"
            :label="item"
            :value="index"
            :disabled="item === chartForm.label"
          >
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label=" ">
        <el-button size="mini" plain :disabled="drawing" @click="drawChart">{{
          $t("console.draw")
        }}</el-button>
      </el-form-item>
    </el-form>
    <div class="chart-right" id="chart" ref="chart">
      <Echart
        width="100%"
        :notMerge="true"
        @finished="drawing = false"
        :chartOption="chartOption"
      ></Echart>
    </div>
    <!-- 列表 -->
  </div>
</template>
<script>
import Echart from "@/components/EChart.vue";
import { getAxisType } from '@/utils';
import { mapState } from "vuex";
export default {
  name: "chart",
  data() {
    return {
      chartTypes: [
        {
          value: "bar"
        },
        {
          value: "line"
        },
        {
          value: "area"
        }
      ],
      chartForm: {
        chartType: "bar",
        label: "",
        series: []
      },
      chartOption: {},
      xAxisData: [],
      drawing: false
    };
  },
  components: { Echart },
  computed: {
    ...mapState({
      data: state => state.console.result,
      field: state => state.console.head
    })
  },
  watch: {
    field() {
      this.chartForm.series = [1];
      this.chartForm.label = 0;
      this.chartOption = {};
    }
  },
  mounted() {},
  methods: {
    drawChart() {
      this.$refs.ruleForm.validate(valid => {
        if (valid) {
          const firstData = this.data[0] || {};
          this.chartOption = {
            // title: {
            //   text: 'Chart Show'
            // },
            grid: { right: 30 ,bottom: 70},
            legend: {},
            tooltip: {
              trigger: "axis"
            },
            dataZoom: [
              {
                type: 'inside'
              },
              {
                type: 'slider'
              }
            ],
            xAxis: {
              type: getAxisType(firstData[this.chartForm.label])
            },
            yAxis: {
              type: getAxisType(firstData[this.chartForm.series[0]])
            },
            series: this.handleSeriesChange()
          };
          this.drawing = true;
        }
      });
    },
    handleSeriesChange() {
      const seriesName = this.chartForm.series.reduce((pre, cur) => {
        const filed = this.field[cur];
        const num = pre.filter(item => item === filed).length;
        pre.push(num ? filed + num : filed);
        return pre;
      }, []);
      return this.chartForm.series.map((item, index) => {
        let op = {
          type: this.chartForm.chartType,
          name: seriesName[index],
          data: this.data.map(ite => [ite[this.chartForm.label], ite[item]])
        };
        if (this.chartForm.chartType === 'area') {
          op.type = 'line';
          op.areaStyle = {};
        }
        return op;
      });
    }
  }
};
</script>
<style lang="scss" scoped>
.chart {
  width: 100%;
  height: 100%;

  .chart-right {
    height: 450px;
  }
}
.chart::v-deep .el-form-item__label {
  font-size: 14px !important;
  font-weight: normal;
  color: #909399;
}
</style>
