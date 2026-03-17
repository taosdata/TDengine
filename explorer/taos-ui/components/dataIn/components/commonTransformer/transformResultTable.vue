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
    <template v-for="table in previewTables" :key="table.key">
      <div v-if="getColumnPageCount(table) > 1" class="column-toolbar">
        <span class="column-range">
          {{ getColumnRangeText(table) }}
        </span>
        <el-pagination
          :current-page="table.columnPage"
          class="column-pagination"
          layout="prev, pager, next"
          :page-size="table.columnPageSize"
          :pager-count="5"
          :hide-on-single-page="false"
          :total="table.allColumns.length"
          small
          @current-change="page => handleColumnPageChange(table.key, page)"
        />
      </div>
      <el-table
        ref="table"
        border
        style="width: 100%; margin-bottom: 20px"
        :max-height="state.defaultHeight - 99"
        :data="table.rows"
        :size="table.lite ? 'small' : 'default'"
      >
        <el-table-column
          v-for="item in getVisibleColumns(table)"
          :key="`${table.key}-${item}`"
          :prop="item"
          :sortable="table.sortable && item == 'Name'"
          :show-overflow-tooltip="table.enableOverflowTooltip"
          :label="item"
        >
          <template #header>
            <span v-if="!table.enableHeaderTooltip" :title="item">{{ item }}</span>
            <el-tooltip v-else :content="item" placement="top-start">
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
      <template v-if="state.drawer">
        <template v-for="table in previewTables" :key="`${table.key}-drawer`">
          <div v-if="getColumnPageCount(table) > 1" class="column-toolbar">
            <span class="column-range">
              {{ getColumnRangeText(table) }}
            </span>
            <el-pagination
              :current-page="table.columnPage"
              class="column-pagination"
              layout="prev, pager, next"
              :page-size="table.columnPageSize"
              :pager-count="5"
              :hide-on-single-page="false"
              :total="table.allColumns.length"
              small
              @current-change="page => handleColumnPageChange(table.key, page)"
            />
          </div>
          <el-table ref="table" border style="width: 100%; margin-bottom: 20px" :data="table.rows" size="small">
            <el-table-column
              v-for="item in getVisibleColumns(table)"
              :key="`${table.key}-drawer-${item}`"
              :prop="item"
              :sortable="table.sortable && item == 'Name'"
              :show-overflow-tooltip="table.enableOverflowTooltip"
              :label="item"
            >
              <template #header>
                <span v-if="!table.enableHeaderTooltip" :title="item">{{ item }}</span>
                <el-tooltip v-else :content="item" placement="top-start">
                  <span>{{ item }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
        </template>
      </template>
    </el-drawer>
  </div>
</template>

<script setup lang="ts">
import { transformerState } from './util';
import { t } from 'locales';

interface PreviewTable {
  key: string;
  rows: Recordable[];
  allColumns: string[];
  columnPage: number;
  columnPageSize: number;
  lite: boolean;
  sortable: boolean;
  enableOverflowTooltip: boolean;
  enableHeaderTooltip: boolean;
}

const PREVIEW_ROW_LIMIT = 100;
const DEFAULT_COLUMN_PAGE_SIZE = 10;
const MIN_COLUMN_PAGE_SIZE = 8;
const MAX_COLUMN_PAGE_SIZE = 20;
const ESTIMATED_COLUMN_WIDTH = 160;
const DEFAULT_DRAWER_COLUMN_PAGE_SIZE = 16;
const MIN_DRAWER_COLUMN_PAGE_SIZE = 12;
const MAX_DRAWER_COLUMN_PAGE_SIZE = 32;
const DRAWER_ESTIMATED_COLUMN_WIDTH = 120;
const LITE_CELL_THRESHOLD = 4000;
const LITE_COLUMN_THRESHOLD = 40;

const props = defineProps<{
  isEditable: boolean;
  currentDataSource: string;
}>();
const state = reactive({
  mqttDefaultCols: ['topic', 'qos'],
  kafkaDefaultCols: ['topic', 'partition', 'offset'],
  MongoDBDefaultCols: ['value'],
  mappingCol: ['SubTableName', 'SuperTableName'],
  defaultHeight: 510,
  drawer: false,
  resultTableMaxHeight: 510
});
const resultRef = ref();
const previewTables = shallowRef<PreviewTable[]>([]);
let mainDom: HTMLElement | null = null;
let resizeObserver: ResizeObserver | null = null;

const title = computed(() => transformerState.resultTbTitle);

watch(
  () => [transformerState.transformResultTable, transformerState.transResultName, transformerState.resultTbTitle],
  ([newVal]) => {
    if (newVal && newVal.length > 0 && transformerState.transResultName) {
      handleScroll();
      getResultData(newVal);
    } else {
      previewTables.value = [];
    }
  }
);

watch(
  () => state.drawer,
  () => {
    nextTick(() => {
      syncColumnPageSize();
    });
  }
);

onMounted(() => {
  if (transformerState.transformResultTable.length > 0 && !props.isEditable && transformerState.transResultName) {
    getResultData(transformerState.transformResultTable);
    handleScroll();
  }
  mainDom = document.querySelector('.main-content') as HTMLElement;
  const parserDom = document.querySelector('#parser') as HTMLElement;
  nextTick(() => {
    const height = mainDom?.offsetHeight || state.defaultHeight + 100;
    state.defaultHeight = height - 100;
    state.resultTableMaxHeight = parserDom?.offsetHeight ? parserDom.offsetHeight - 200 : state.resultTableMaxHeight;
    observeResize();
  });
  mainDom?.addEventListener('scroll', handleScroll);
});

onBeforeUnmount(() => {
  mainDom?.removeEventListener('scroll', handleScroll);
  resizeObserver?.disconnect();
});

function handleScroll() {
  nextTick(() => {
    const dom = document.querySelector('.block-title.top') as HTMLElement;
    const docPart = document.querySelector('.doc-part') as HTMLElement;
    let docPartBottom = 0;
    if (docPart && resultRef.value) {
      docPartBottom = docPart.offsetTop + docPart.offsetHeight;
    }
    if (dom) {
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
          const finalTop = commomtop >= docPartBottom ? commomtop : docPartBottom + 10;
          resultRef.value.style.top = `${finalTop}px`;
        }
      }
    }
  });
}

function isArray2D(arr: any[]): boolean {
  return Array.isArray(arr) && arr.length > 0 && arr.every(item => Array.isArray(item));
}

function getColumnPageSize() {
  const width = state.drawer ? window.innerWidth : (resultRef.value?.clientWidth ?? 0);
  if (!width) {
    return state.drawer ? DEFAULT_DRAWER_COLUMN_PAGE_SIZE : DEFAULT_COLUMN_PAGE_SIZE;
  }

  const minColumns = state.drawer ? MIN_DRAWER_COLUMN_PAGE_SIZE : MIN_COLUMN_PAGE_SIZE;
  const maxColumns = state.drawer ? MAX_DRAWER_COLUMN_PAGE_SIZE : MAX_COLUMN_PAGE_SIZE;
  const estimatedColumnWidth = state.drawer ? DRAWER_ESTIMATED_COLUMN_WIDTH : ESTIMATED_COLUMN_WIDTH;

  return Math.min(maxColumns, Math.max(minColumns, Math.floor((width - 48) / estimatedColumnWidth)));
}

function observeResize() {
  if (!resultRef.value || typeof ResizeObserver === 'undefined') {
    return;
  }

  resizeObserver?.disconnect();
  resizeObserver = new ResizeObserver(() => {
    syncColumnPageSize();
  });
  resizeObserver.observe(resultRef.value);
}

function syncColumnPageSize() {
  const columnPageSize = getColumnPageSize();
  previewTables.value = previewTables.value.map(table => {
    const maxPage = Math.max(1, Math.ceil(table.allColumns.length / columnPageSize));
    return {
      ...table,
      columnPageSize,
      columnPage: Math.min(table.columnPage, maxPage)
    };
  });
}

function handleColumnPageChange(key: string, page: number) {
  previewTables.value = previewTables.value.map(table =>
    table.key === key
      ? {
          ...table,
          columnPage: page
        }
      : table
  );
  handleScroll();
}

function getVisibleColumns(table: PreviewTable) {
  const start = (table.columnPage - 1) * table.columnPageSize;
  return table.allColumns.slice(start, start + table.columnPageSize);
}

function getColumnPageCount(table: PreviewTable) {
  return Math.max(1, Math.ceil(table.allColumns.length / table.columnPageSize));
}

function getColumnRangeText(table: PreviewTable) {
  const start = (table.columnPage - 1) * table.columnPageSize + 1;
  const end = Math.min(table.columnPage * table.columnPageSize, table.allColumns.length);
  return `${start}-${end} / ${table.allColumns.length}`;
}

function shouldUseLiteTable(rows: Recordable[], columns: string[]) {
  const cellCount = rows.length * Math.min(columns.length, getColumnPageSize());
  return cellCount >= LITE_CELL_THRESHOLD || columns.length >= LITE_COLUMN_THRESHOLD;
}

function buildPreviewTable(rows: Recordable[], columns: string[], index: number): PreviewTable {
  const previewRows = rows.slice(0, PREVIEW_ROW_LIMIT);
  const lite = shouldUseLiteTable(previewRows, columns);

  return {
    key: `${transformerState.transResultName || 'result'}-${index}`,
    rows: previewRows,
    allColumns: columns,
    columnPage: 1,
    columnPageSize: getColumnPageSize(),
    lite,
    sortable: !lite,
    enableOverflowTooltip: !lite,
    enableHeaderTooltip: !lite
  };
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
    return Object.keys(item[0] || {}).filter(field => !hiddenCols.includes(field));
  });

  if (transformerState.resultTbTitle === 'mappingResTb') {
    state.mappingCol.forEach(item => {
      columns.forEach(cols => {
        const index = cols.indexOf(item);
        if (index > 0) {
          cols.splice(index, 1);
          cols.unshift(item);
        }
      });
    });
  }

  previewTables.value = data2D.map((rows, index) => buildPreviewTable(rows, columns[index] || [], index));
}
</script>

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

  .column-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 10px;
  }

  .column-range {
    flex-shrink: 0;
    color: #606266;
    font-size: 12px;
  }

  :deep(.column-pagination) {
    margin-left: auto;
  }
}
</style>
