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
      <el-form-item label="">
        <el-button type="primary" :disabled="drawing" @click="drawChart">{{ t('explorer.draw') }}</el-button>
      </el-form-item>
    </el-form>
    <div class="chart-right">
      <Echart width="100%" height="100%" :option="chartOption" @finished="drawing = false"></Echart>
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
  chartType: 'line',
  label: 0,
  series: [] as number[]
});
const formRef = shallowRef<FormInstance | null>(null);
const drawing = ref(false);
const chartOption = ref<Recordable>({});
const rules = computed(() => ({
  chartType: [{ required: false, message: t('common.requiredTemp', [t('explorer.chartType')]) }],
  label: [{ required: false, message: t('common.requiredTemp', [t('explorer.xAxis')]) }],
  series: [{ required: false, message: t('common.requiredTemp', [t('explorer.series')]) }]
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

// 计算合适的 y 轴范围和间隔
function calculateYAxisRange(min: number, max: number) {
  if (!isFinite(min) || !isFinite(max)) {
    return { min: undefined, max: undefined, interval: undefined };
  }
  
  const range = max - min;
  if (range === 0) {
    return {
      min: min - 1,
      max: max + 1,
      interval: 1
    };
  }
  
  // 计算合适的间隔，使用 1, 2, 5 的倍数
  const roughInterval = range / 5; // 目标是 5-6 个刻度
  const magnitude = Math.pow(10, Math.floor(Math.log10(roughInterval)));
  const normalized = roughInterval / magnitude;
  
  let interval: number;
  if (normalized <= 1) {
    interval = magnitude;
  } else if (normalized <= 2) {
    interval = 2 * magnitude;
  } else if (normalized <= 5) {
    interval = 5 * magnitude;
  } else {
    interval = 10 * magnitude;
  }
  
  // 将最小值向下取整到间隔的倍数
  const adjustedMin = Math.floor(min / interval) * interval;
  // 将最大值向上取整到间隔的倍数
  const adjustedMax = Math.ceil(max / interval) * interval;
  
  return {
    min: adjustedMin,
    max: adjustedMax,
    interval: interval
  };
}

function drawChart(evt?: MouseEvent) {
  if (!formRef.value) return;
  formRef.value.validate(valid => {
    if (valid) {
      // 如果不是由鼠标点击触发的，自动选择前两个数字类型的列
      if (!evt) {
        const numericIndices = fields.value.map((field, index) => {
          const headItem = sqlExecResult.head.find(h => h.field === field);
          const headType = headItem?.type.toLowerCase() || '';
          return headItem && (
            headType === 'tinyint' || headType === 'tinyint unsigned' 
            || headType === 'smallint' || headType === 'smallint unsigned' 
            || headType === 'int' || headType === 'int unsigned' 
            || headType === 'bigint unsigned' || headType === 'bigint' 
            || headType === 'float' || headType === 'double'
            || headType.includes('decimal')
          ) && index !== chartForm.label ? index : -1;
        }).filter(index => index !== -1);
        if (numericIndices.length >= 2) {
          chartForm.series = numericIndices.slice(0, 2);
        } else if (numericIndices.length === 1) {
          chartForm.series = [numericIndices[0]];
        } else {
          chartForm.series = [];
        }
      }
      const firstData = sqlExecResult.data[0] || {};
      
      // 计算所有系列数据的最大最小值
      let minValue = Infinity;
      let maxValue = -Infinity;
      
      chartForm.series.forEach(seriesIndex => {
        sqlExecResult.data.forEach(row => {
          const value = row[seriesIndex];
          let parsedValue: number | undefined;
          // 处理 BigNumber2 类型
          if (value && typeof value === 'object' && 'c' in value) {
            const numStr = JSONBig.stringify(value);
            parsedValue = parseFloat(numStr);
          } else if (typeof value === 'string') {
            parsedValue = parseFloat(value);
          } else if (typeof value === 'number') {
            parsedValue = value;
          }
          if (typeof parsedValue === 'number' && !isNaN(parsedValue) && isFinite(parsedValue)) {
            minValue = Math.min(minValue, parsedValue);
            maxValue = Math.max(maxValue, parsedValue);
          }
        });
      });
      
      // 计算合适的 y 轴范围
      const yAxisRange = calculateYAxisRange(minValue, maxValue);
      
      // 检查 y 轴区间值是否都是整数
      const isYAxisInteger = 
        Number.isInteger(yAxisRange.min) && 
        Number.isInteger(yAxisRange.max) && 
        Number.isInteger(yAxisRange.interval);
      
      // 只有当 y 轴区间包含小数时才格式化为小数
      const shouldFormatAsDecimal = !isYAxisInteger;
      chartOption.value = {
        grid: { 
          top: 50,
          left: 'auto',
          right: 'auto', 
          bottom: 70,
          containLabel: true
        },
        legend: {
          top: 10
        },
        tooltip: {
          trigger: 'axis',
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
          type: getAxisType(firstData[chartForm.series[0]]),
          min: yAxisRange.min,
          max: yAxisRange.max,
          interval: yAxisRange.interval,
          axisLabel: {
            formatter: (value: number | string) => {
              if (shouldFormatAsDecimal) {
                const numValue = typeof value === 'string' ? parseFloat(value) : value;
                if (!isNaN(numValue)) {
                  return numValue.toFixed(3);
                }
              }
              return value;
            }
          }
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
        if (typeof ite[item] === 'string') {
          return [ite[chartForm.label], ite[item]];
        }
        const bignumber = JSONBig.stringify(ite[item]);
        if (bignumber) {
          return [ite[chartForm.label], bignumber];
        }
        [ite[chartForm.label], ite[item]];
      })
    };
    if (chartForm.chartType === 'area') {
      op.type = 'line';
      op.areaStyle = {};
    }
    return op;
  });
}
defineExpose({
  drawChart
});
</script>
<style lang="scss" scoped>
.chart {
  width: 100%;
  height: 100%;

  .chart-right {
    height: calc(100% - 30px);
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

.el-form-item {
  margin-right: 10px;

  .el-input, .el-cascader, .el-select, .el-autocomplete {
    min-width: 100px;
  }
}
</style>
