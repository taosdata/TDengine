<template>
  <div ref="resultRef" class="result-table" :style="{ 'max-height': state.resultTableMaxHeight + 'px' }">
    <div class="title-block">
      <span class="title">{{ t(`dataIn.transformer.${title}`) }}</span>
      <span class="title-block">
        <el-tooltip placement="top" effect="light" :open-delay="0">
          <template #content>
            {{ t('dataIn.fullscreen') }}
          </template>
          <el-icon @click="state.drawer = true"><FullScreen /></el-icon>
        </el-tooltip>
        <span class="el-icon-close" @click="transformerState.showResultTb = false"><Close /></span>
      </span>
    </div>
    <template v-for="(tableItme, index) in state.pageTableData" :key="index">
      <el-table
        ref="table"
        border
        style="width: 100%; margin-bottom: 20px"
        :max-height="state.defaultHeight - 99"
        :data="tableItme"
      >
        <el-table-column
          v-for="item in state.columns[index]"
          :key="item"
          :prop="item"
          :sortable="item == 'Name' ? true : false"
          show-overflow-tooltip
          :label="item"
        >
          <template #header>
            <el-tooltip :content="item" placement="top-start">
              <span>{{ item }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>
    </template>
    <el-drawer
      v-model="state.drawer"
      :title="t(`dataIn.transformer.${title}`)"
      direction="rtl"
      size="100%"
      @close="state.drawer = false"
    >
      <template v-for="(tableItme, index) in state.pageTableData">
        <el-table
          v-if="state.drawer"
          :key="index"
          ref="table"
          border
          style="width: 100%; margin-bottom: 20px"
          :data="tableItme"
          size="small"
        >
          <el-table-column
            v-for="item in state.columns[index]"
            :key="item"
            :prop="item"
            :sortable="item == 'Name' ? true : false"
            show-overflow-tooltip
            :label="item"
          >
            <template #header>
              <el-tooltip :content="item" placement="top-start">
                <span>{{ item }}</span>
              </el-tooltip>
            </template>
          </el-table-column>
        </el-table>
      </template>
    </el-drawer>
  </div>
</template>
<script setup lang="ts">
import { transformerState } from './util';
import { t } from 'locales';
const props = defineProps<{
  isEditable: boolean;
  currentDataSource: string;
}>();
const state = reactive({
  loading: true,
  isFixed: false,
  columns: [] as string[][],
  pageTableData: [] as any[],
  pageSize: 20,
  totalCount: 10,
  currentPage: 1,
  mqttDefaultCols: ['topic', 'qos'],
  kafkaDefaultCols: ['topic', 'partition', 'offset'],
  MongoDBDefaultCols: ['value'],
  mappingCol: ['SubTableName', 'SuperTableName'],
  defaultHeight: 510,
  drawer: false,
  resultTableMaxHeight: 510
});
const resultRef = ref();

const title = computed(() => transformerState.resultTbTitle);
const limitOffset = computed(() => 100); // 假设 limitOffset 固定为 100

watch(
  () => transformerState.transformResultTable,
  newVal => {
    if (newVal && newVal.length > 0 && transformerState.transResultName) {
      handleScroll();
      getResultData(newVal);
    } else {
      state.pageTableData = [];
      state.totalCount = 0;
    }
  },
  { deep: true }
);
onMounted(() => {
  if (transformerState.transformResultTable.length > 0 && !props.isEditable && transformerState.transResultName) {
    getResultData(transformerState.transformResultTable);
    handleScroll();
  }
  const mainDom = document.querySelector('.main-content') as HTMLElement;
  const parserDom = document.querySelector('#parser') as HTMLElement;
  nextTick(() => {
    const height = mainDom?.offsetHeight;
    state.defaultHeight = height - 100;
    state.resultTableMaxHeight = parserDom?.offsetHeight - 200;
  });
  mainDom?.addEventListener('scroll', handleScroll);
  onBeforeUnmount(() => {
    mainDom?.removeEventListener('scroll', handleScroll);
  });
});

function handleScroll() {
  nextTick(() => {
    const dom = document.querySelector('.block-title.top') as HTMLElement;
    if (dom) {
      const mainDom = document.querySelector('.main-content') as HTMLElement;
      const scrollTop = mainDom?.scrollTop;
      const top = scrollTop >= dom.offsetTop ? scrollTop : dom.offsetTop;
      transformerState.transformTableHeight = top;
      if (resultRef.value) {
        if (props.currentDataSource === 'csv') {
          const csvdom = document.querySelector('.csv-data') as HTMLElement;
          const csvtop = top >= csvdom.offsetTop + dom.offsetTop ? top : csvdom.offsetTop + dom.offsetTop + 25;
          resultRef.value.style.top = `${csvtop}px`;
        } else {
          const commomtop = scrollTop >= dom.offsetTop ? scrollTop - 160 : dom.offsetTop;
          resultRef.value.style.top = `${commomtop}px`;
        }
      }
    }
  });
}

function isArray2D(arr: any[]): boolean {
  return Array.isArray(arr) && arr.length > 0 && arr.every(item => Array.isArray(item));
}

function getResultData(data: any[]) {
  let data2D: any[][] = [];
  if (isArray2D(data)) {
    data2D = data;
  } else {
    data2D.push(data);
  }

  let hiddenCols: string[] = [];
  if (props.currentDataSource === 'mqtt') {
    hiddenCols = state.mqttDefaultCols;
  }
  if (props.currentDataSource === 'kafka') {
    hiddenCols = state.kafkaDefaultCols;
  }
  if (props.currentDataSource === 'mongodb') {
    hiddenCols = state.MongoDBDefaultCols;
  }
  const columns = data2D.map(item => {
    return Object.keys(item[0]).filter(field => !hiddenCols.includes(field));
  });

  state.columns = columns as any;
  state.totalCount = columns.length;
  if (transformerState.resultTbTitle === 'mappingResTb') {
    state.mappingCol.forEach(item => {
      state.columns.forEach(cols => {
        const index = cols.indexOf(item);
        if (index > 0) {
          cols.splice(index, 1);
          cols.unshift(item);
        }
      });
    });
  }
  state.pageTableData = data2D.map(arr => arr.slice(0, limitOffset.value));
}
</script>
<style>
/* @media screen and (max-width: 1366px) {
  .result-table {
    background: red;
    display: none !important;
  }
} */
</style>
<style lang="scss" scoped>
.result-table {
  //   max-width: 600px;
  //   min-width: 480px;
  position: absolute;
  width: 100%;
  padding: 20px;
  overflow-y: auto;
  border: 1px solid #e3e4e6;
  border-radius: 12px;

  .block-page {
    overflow: auto;
  }

  //   top: 54%;
  .title-block {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    margin-bottom: 15px;

    .title {
      font-size: 14px;
      font-weight: 600;
      color: #4259ce;
    }

    .el-icon-close {
      cursor: pointer;
    }

    .el-icon-full-screen {
      display: inline-block;
      width: 30px;
      cursor: pointer;
    }
  }

  :deep(.el-table) {
    thead tr th {
      background-color: #f5f7fa;
    }

    .el-table--group::after {
      border-color: transparent !important;
    }

    &.el-table__cell {
      padding: 6px 0 !important;
    }

    .active-row {
      background: #ecf2fe !important;
    }

    &::before {
      background-color: transparent;
    }
  }
}
</style>
