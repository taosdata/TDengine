<template>
  <div ref="result" class="dataset-result-table" :style="{ 'max-height': defaultHeight, top: defaultTop }">
    <div class="title-block">
      <span class="title">{{ t('dataIn.transformer.resulttb') }}</span>
      <span class="flex">
        <el-tooltip placement="top" effect="light" :open-delay="0">
          <template #content>
            {{ t('dataIn.fullscreen') }}
          </template>
          <el-icon @click="drawer = true"><FullScreen /></el-icon>
        </el-tooltip>
        <el-icon size="16px" @click="isShowDatasetTable = false"><Close /></el-icon>
      </span>
    </div>
    <el-table
      ref="table"
      v-loading="loading"
      border
      style="width: 100%"
      :max-height="defaultHeight - 99"
      :data="tableData"
      size="default"
    >
      <el-table-column v-for="col in columns" :key="col" :prop="col" :label="col" show-overflow-tooltip>
        <template #header>
          <el-input
            v-model="searchTextMap[col]"
            style="width: 80%"
            size="default"
            :placeholder="t('dataIn.pointFilter')"
            @change="searchInputChange"
          >
            <template #prepend>{{ col }}</template>
          </el-input>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-model:current-page="currentPage"
      class="pagination"
      layout="total, prev, pager, next"
      :page-size="pageSize"
      :hide-on-single-page="false"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
    <el-drawer id="my-drawer" v-model="drawer" :title="t('dataIn.transformer.resulttb')" direction="rtl" size="100%">
      <el-table ref="table" border style="width: 100%" :max-height="fullTableHeight" :data="tableData" size="small">
        <el-table-column v-for="col in columns" :key="col" :prop="col" :label="col" show-overflow-tooltip>
          <template #header>
            <el-input
              v-model="searchTextMap[col]"
              style="width: 80%"
              size="default"
              :placeholder="t('dataIn.pointFilter')"
              @change="searchInputChange"
            >
              <template #prepend>{{ col }}</template>
            </el-input>
          </template>
        </el-table-column>
      </el-table>

      <el-pagination
        v-model:current-page="currentPage"
        class="pagination"
        layout="total, prev, pager, next"
        :page-size="pageSize"
        :hide-on-single-page="false"
        :total="total"
        @current-change="handlePageChange"
      ></el-pagination>
    </el-drawer>
  </div>
</template>
<script setup lang="ts">
import { datasetsField, isShowDatasetTable, datasetTableData } from '../model/util';
import { t } from 'locales';
import { ElMessage } from 'element-plus';

const loading = ref(true);
const pageSize = ref(200);
const total = ref(10);
const currentPage = ref(1);
const defaultHeight = ref(495);
const defaultTop = ref('50%');
const columns = ref<string[]>([]);
const searchTextMap = ref<Record<string, string>>({});
const drawer = ref(false);
const fullTableHeight = ref(600);
const tableData = ref<any[]>([]); //表格实际展示的数据
const lists = ref<any[]>([]); // 全部的点位数据
const filterTableData = ref<any[]>([]); //增加过滤条件的全部数据

watch(
  () => datasetTableData,
  val => {
    if (val) {
      getDatasetsData(val.value);
    }
  },
  {
    deep: true,
    immediate: true
  }
);
watch(drawer, val => {
  if (val) {
    nextTick(() => {
      fullTableHeight.value = getFullTableHeight();
    });
  }
});

async function searchInputChange() {
  loading.value = true;
  currentPage.value = 1;
  const activeFilters = Object.entries(searchTextMap.value).filter(([, v]) => v && v.trim() !== '');
  if (activeFilters.length === 0) {
    filterTableData.value = lists.value;
  } else {
    const filters = Object.fromEntries(activeFilters);
    filterTableData.value = await lists.value.filter(row =>
      Object.keys(filters).every(key =>
        String(row[key] ?? '')
          .toLowerCase()
          .includes(String(filters[key]).toLowerCase())
      )
    );
  }
  getTableData(filterTableData.value);
  loading.value = false;
}
function handlePageChange(page: number) {
  currentPage.value = page;
  getTableData(filterTableData.value);
}
function getTableData(data: Recordable[]) {
  total.value = data.length;
  tableData.value = data.slice(pageSize.value * (currentPage.value - 1), pageSize.value * currentPage.value);
}
async function getDatasetsData(res: Recordable) {
  if (res?.code == 0) {
    const { page, list, columns: cols } = res?.data as any;
    currentPage.value = page;
    lists.value = list;
    filterTableData.value = list;
    columns.value = Array.isArray(cols) && cols.length > 0 ? cols : Object.keys(list?.[0] ?? {});
    // init search map keys
    searchTextMap.value = Object.fromEntries(columns.value.map(c => [c, '']));
    getTableData(list);
  } else {
    total.value = 0;
    lists.value = [];
    filterTableData.value = [];
    columns.value = [];
    searchTextMap.value = {};
    getTableData(lists.value);
    ElMessage.error(res?.message);
  }
  getEleTop();
  loading.value = false;
}

function getEleTop() {
  nextTick(() => {
    const dom1 = document.getElementById(`${datasetsField}`) as HTMLElement;
    const dom2 = document.querySelector('.right-ui') as HTMLElement;
    if (!dom1 || !dom2) return;
    const rect1 = dom1.getBoundingClientRect();
    const rect2 = dom2.getBoundingClientRect();
    defaultTop.value = rect1.top - rect2.top + 'px';
  });
}
function getFullTableHeight() {
  const dom = document.getElementById('my-drawer') as HTMLElement;
  const rect = dom.getBoundingClientRect();
  return rect.height - 150;
}
</script>

<style lang="scss">
.dataset-result-table {
  position: absolute;
  width: 100%;
  padding: 20px;
  border: 1px solid #e3e4e6;
  border-radius: 12px;

  .block-page {
    overflow: auto;
  }

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
  }

  :deep(.el-pagination__jump) {
    display: none;
  }

  :deep(.pagination) {
    margin-top: 15px;
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
