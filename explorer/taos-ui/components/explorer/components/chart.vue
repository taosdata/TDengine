<template>
  <div class="chart">
    <el-form ref="formRef" size="small" :model="chartForm" :rules="rules" inline>
      <el-form-item :label="t('explorer.chartType')" prop="chartType">
        <el-select v-model="chartForm.chartType">
          <el-option v-for="item in chartTypes" :key="item" :label="item" :value="item"> </el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="t('explorer.xAxis')" prop="label">
        <el-select v-model="chartForm.label">
          <el-option v-for="(item, index) in fields" :key="item + index" :label="item" :value="index"> </el-option>
        </el-select>
      </el-form-item>
      <el-form-item :label="t('explorer.series')" prop="series">
        <el-select v-model="chartForm.series" collapse-tags multiple>
          <el-option
            v-for="(item, index) in fields"
            :key="item + index"
            :label="item"
            :value="index"
            :disabled="index === chartForm.label"
          >
          </el-option>
        </el-select>
      </el-form-item>
      <el-form-item label=" ">
        <el-button type="primary" :disabled="drawing" @click="drawChart">{{ t('explorer.draw') }}</el-button>
      </el-form-item>
    </el-form>
    <div class="chart-right">
      <Echart width="100%" height="100%" :option="chartOption" @finished="drawing = false"></Echart>
    </div>
    <!-- 列表 -->
     <div class="idmptip">
      <router-link to="/idmp">
        <span class="title">{{ t('explorer.idmptip') }}</span>
      </router-link>
     </div>
  </div>
</template>
<script lang="ts" setup>
import Echart from 'components/Echarts';
import { getAxisType } from 'utils';
import { t } from 'locales';
import { sqlExecResult } from './utils';
import { FormInstance } from 'element-plus';
import JSONBig from 'json-big';

const chartTypes = ['bar', 'line', 'area'];
const chartForm = reactive({
  chartType: 'bar',
  label: 0,
  series: [] as number[]
});
const formRef = shallowRef<FormInstance | null>(null);
const drawing = ref(false);
const chartOption = ref<Recordable>({});
const rules = computed(() => ({
  chartType: [{ required: true, message: t('common.requiredTemp', [t('explorer.chartType')]) }],
  label: [{ required: true, message: t('common.requiredTemp', [t('explorer.xAxis')]) }],
  series: [{ required: true, message: t('common.requiredTemp', [t('explorer.series')]) }]
}));
const fields = computed(() => sqlExecResult.head.map((item: Recordable) => item.field));

watch(
  () => sqlExecResult.head,
  () => {
    chartForm.label = 0;
    chartForm.series = [1];
    chartOption.value = {};
  }
);

function drawChart() {
  if (!formRef.value) return;
  formRef.value.validate(valid => {
    if (valid) {
      const firstData = sqlExecResult.data[0] || {};
      chartOption.value = {
        grid: { right: 30, bottom: 70 },
        legend: {},
        tooltip: {
          trigger: 'axis'
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
          type: getAxisType(firstData[chartForm.label])
        },
        yAxis: {
          type: getAxisType(firstData[chartForm.series[0]])
        },
        series: handleSeriesChange()
      };
      drawing.value = true;
    }
  });
}
function handleSeriesChange() {
  const seriesName = chartForm.series.reduce((pre, cur) => {
    const filed = fields.value[cur];
    const num = pre.filter(item => item === filed).length;
    pre.push(num ? filed + num : filed);
    return pre;
  }, [] as string[]);
  return chartForm.series.map((item, index) => {
    const op: Recordable = {
      type: chartForm.chartType,
      name: seriesName[index],
      data: sqlExecResult.data.map(ite => {
        const bignumber = JSONBig.stringify(ite[item]);
        if (bignumber) {
          return [ite[chartForm.label], bignumber]
        }
        [ite[chartForm.label], ite[item]]
      })
    };
    if (chartForm.chartType === 'area') {
      op.type = 'line';
      op.areaStyle = {};
    }
    return op;
  });
}
</script>
<style lang="scss" scoped>
.chart {
  width: 100%;
  height: 100%;

  .chart-right {
    height: 380px;
  }
}
.idmptip {
  position: absolute;
  right: 0;
  bottom: 0;
  display: inline-block;

  .title {
    margin-right: 5px;
    font-size: 16px;
    color: #4d6992;
  }

  .title:hover {
    color: #1976d2; /* 悬浮时变为蓝色 */
  }
}
</style>
