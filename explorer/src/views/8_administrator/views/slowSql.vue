<template>
  <div>
    <el-tabs model-value="log_desc">
      <el-tab-pane :label="$t('slowSql.tab1')" name="log_desc">
        <div class="dnode-block">
          <section class="flex-between">
            <el-form
              ref="ruleFormRef"
              :model="filterParams"
              :inline="true"
              size="default"
              :disabled="requestIng"
              label-position="left"
              :rules="rules"
            >
              <el-form-item :label="$t('slowSql.startTs')">
                <TimezoneDatePicker
                  v-model="date"
                  size="small"
                  type="datetimerange"
                  :shortcuts="shortcuts"
                  range-separator="-"
                  :start-placeholder="$t('start')"
                  :end-placeholder="$t('end')"
                  value-format="YYYY-MM-DDTHH:mm:ssZ"
                  align="left"
                  style="width: 320px"
                >
                </TimezoneDatePicker>
              </el-form-item>
              <el-form-item :label="$t('slowSql.queryTime')" prop="query_time">
                <el-input-number
                  v-model="filterParams.query_time_1"
                  style="width: 70px"
                  placeholder="[min"
                  :min="0"
                  :controls="false"
                  :precision="1"
                />
                -
                <el-input-number
                  v-model="filterParams.query_time_2"
                  style="width: 70px"
                  placeholder="max]"
                  :min="0"
                  :controls="false"
                  :precision="1"
                />
              </el-form-item>
              <el-form-item :label="$t('slowSql.deDuplication')" prop="de_duplication">
                <el-switch v-model="filterParams.de_duplication" />
              </el-form-item>
              <!-- </div> -->
              <el-form-item>
                <el-button icon="Search" @click="handlePageChange()">{{ $t('search') }}</el-button>
              </el-form-item>
              <el-form-item>
                <el-button icon="Refresh" @click="handlePageReset('tab1')">{{ $t('reset') }}</el-button>
              </el-form-item>
              <!-- </section> -->
            </el-form>
            <div style="margin-bottom: 18px">
              <el-button :disabled="requestIng" icon="Download" size="default" type="primary" plain @click="exportFile"
                >{{ $t('slowSql.exportingSlowLogs') }}
              </el-button>
            </div>
          </section>
          <el-table style="margin-top: 20px" :data="slowSqlLogList" size="small" @sort-change="customSort">
            <el-table-column :label="$t('slowSql.startTs')" prop="start_ts" width="220">
              <template #default="scope">
                <span>{{ parsinginZone(scope.row.start_ts) }}</span>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.sql')" prop="sql" min-width="180">
              <template #default="scope">
                <el-tooltip placement="left-start" :content="scope.row.sql" popper-class="my-popper" :open-delay="1000">
                  <span>
                    <pre v-highlight class="nowrap sql-code pre-code">
                      <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
                    </pre>
                  </span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.db')" prop="db">
              <template #default="scope">
                <el-tooltip :content="scope.row.db" placement="top-start">
                  <span class="nowrap">{{ scope.row.db }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.ip')" prop="ip">
              <template #default="scope">
                <el-tooltip :content="scope.row.ip" placement="top-start">
                  <span class="nowrap">{{ scope.row.ip }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.user')" prop="user">
              <template #default="scope">
                <el-tooltip :content="scope.row.user" placement="top-start">
                  <span class="nowrap">{{ scope.row.user }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column
              :label="$t('slowSql.queryTime')"
              prop="query_time"
              sortable="custom"
              width="160px"
              align="right"
            >
              <template #default="scope">
                <el-tooltip :content="String(numToFixed(scope.row.query_time))" placement="top-start">
                  <span class="nowrap">{{ numToFixed(scope.row.query_time) }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.rowsNum')" prop="rows_num" align="right">
              <template #default="scope">
                <el-tooltip :content="String(scope.row.rows_num)" placement="top-start">
                  <span class="nowrap">{{ scope.row.rows_num }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
          <el-pagination
            v-model:current-page="currentPage"
            class="pagination"
            layout="sizes, total, prev, pager, next"
            :page-sizes="[20, 50, 100, 200]"
            :page-size="pageSize"
            :hide-on-single-page="false"
            :total="total"
            @size-change="handleSizeChange"
            @current-change="handlePageChange"
          ></el-pagination>
        </div>
      </el-tab-pane>
      <el-tab-pane :label="$t('slowSql.tab2')" name="statistics">
        <div class="dnode-block">
          <section class="flex-between">
            <el-form inline size="default" :disabled="requestIng">
              <section class="flex-between">
                <div>
                  <el-form-item :label="$t('slowSql.startTs')">
                    <TimezoneDatePicker
                      v-model="date_two"
                      size="small"
                      type="datetimerange"
                      :shortcuts="shortcuts"
                      range-separator="-"
                      :start-placeholder="$t('start')"
                      :end-placeholder="$t('end')"
                      value-format="YYYY-MM-DDTHH:mm:ssZ"
                      align="left"
                      style="width: 320px"
                    >
                    </TimezoneDatePicker>
                  </el-form-item>
                </div>
                <el-form-item>
                  <el-button icon="Search" @click="handlePageChangeTwo()">{{ $t('search') }}</el-button>
                </el-form-item>
                <el-form-item>
                  <el-button @click="handlePageReset('tab2')">{{ $t('reset') }}</el-button>
                </el-form-item>
              </section>
            </el-form>
            <div style="margin-bottom: 18px"></div>
          </section>
          <el-table style="margin-top: 20px" :data="statisticsList" size="small" @sort-change="customSort">
            <el-table-column :label="$t('slowSql.sql')" prop="sql" min-width="180">
              <template #default="scope">
                <el-tooltip placement="left-start" :content="scope.row.sql" popper-class="my-popper" :open-delay="1000">
                  <span>
                    <pre v-highlight class="nowrap sql-code pre-code">
                      <code class="language-sql" style="overflow:hidden">{{ scope.row.sql }} </code>
                    </pre>
                  </span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.db')" prop="db">
              <template #default="scope">
                <el-tooltip :content="scope.row.db" placement="top-start">
                  <span class="nowrap">{{ scope.row.db }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.executionTimes')" prop="query_count" width="130px" align="right">
              <template #default="scope">
                <el-tooltip :content="String(scope.row.query_count)" placement="top-start">
                  <span class="nowrap">{{ scope.row.query_count }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column
              :label="$t('slowSql.averageTime')"
              prop="avg_query_time"
              width="200px"
              sortable="custom"
              align="right"
            >
              <template #default="scope">
                <el-tooltip :content="String(numToFixed(scope.row.avg_query_time))" placement="top-start">
                  <span class="nowrap">{{ numToFixed(scope.row.avg_query_time) }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column
              :label="$t('slowSql.maximumTime')"
              prop="max_query_time"
              width="200px"
              sortable="custom"
              align="right"
            >
              <template #default="scope">
                <el-tooltip :content="String(numToFixed(scope.row.max_query_time))" placement="top-start">
                  <span class="nowrap">{{ numToFixed(scope.row.max_query_time) }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.averageRow')" prop="avg_rows_num" width="130px" align="right">
              <template #default="scope">
                <el-tooltip :content="String(scope.row.avg_rows_num)" placement="top-start">
                  <span class="nowrap">{{ scope.row.avg_rows_num }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column :label="$t('slowSql.maximumRow')" prop="max_rows_num" width="130px" align="right">
              <template #default="scope">
                <el-tooltip :content="String(scope.row.max_rows_num)" placement="top-start">
                  <span class="nowrap">{{ scope.row.max_rows_num }}</span>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
          <el-pagination
            v-model:current-page="currentPageTwo"
            class="pagination"
            layout="sizes, total, prev, pager, next"
            :page-sizes="[20, 50, 100, 200]"
            :page-size="pageSizeTwo"
            :hide-on-single-page="false"
            :total="totalTwo"
            @size-change="handleSizeChangeTwo"
            @current-change="handlePageChangeTwo"
          ></el-pagination>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { parsinginZone } from '@/utils/index';
import { json2csv } from 'json-2-csv';
import { saveAs } from 'file-saver';
import { getSlowSqlLogs, getSlowSqlStatistics } from '@/api/slowSql';
import { FormInstance, FormRules } from 'element-plus';

const { t } = useI18n();

const props = defineProps({
  activeName: {
    type: String,
    default: ''
  }
});
const ruleFormRef = ref<FormInstance>();
let pageSize = ref(20);
const currentPage = ref(1);
let total = ref(10);
const requestIng: Ref<boolean> = ref(false);
let pageSizeTwo = ref(20);
const currentPageTwo = ref(1);
let totalTwo = ref(10);

let slowSqlLogList = ref([]);
let statisticsList = ref([]);
const filterParams = reactive({
  query_time_1: 10,
  query_time_2: undefined,
  de_duplication: false
});

const date = ref([new Date(new Date().getTime() - 3600 * 1000 * 24 * 1).toISOString(), new Date().toISOString()]);
const date_two = ref([]);
const query_time_sort = ref('');
const orderSql = ref('');

function checkQueryTime(_: any, value: any, callback: (arg0?: Error | undefined) => void) {
  const { query_time_1, query_time_2 } = filterParams;
  if (query_time_1 && query_time_2 && query_time_2 < query_time_1) {
    return callback(new Error(t('slowSql.queryTimeTip')));
  } else {
    callback();
  }
}
const rules = reactive<FormRules>({
  query_time: [
    {
      validator: checkQueryTime,
      trigger: 'blur'
    }
  ]
});

const shortcuts = [
  {
    text: t('yesterday'),
    value: () => {
      const end = new Date();
      const start = new Date();
      start.setDate(start.getDate() - 1);
      return [start, end];
    }
  },
  {
    text: t('agoWeek'),
    value: () => {
      const end = new Date();
      const start = new Date();
      start.setDate(start.getDate() - 7);
      return [start, end];
    }
  },
  {
    text: t('agoMonth'),
    value: () => {
      const end = new Date();
      const start = new Date();
      start.setMonth(start.getMonth() - 1);
      return [start, end];
    }
  }
];

const conditions = computed(() => {
  let conditions = '';
  if (date.value?.length > 0) {
    conditions = ` start_ts > '${date.value[0]}' AND start_ts <= '${date.value[1]}' AND`;
  }
  const { query_time_1, query_time_2 } = filterParams;
  if (query_time_1) {
    conditions += ` query_time >= ${query_time_1 * 1000} AND`;
  }
  if (query_time_2) {
    conditions += ` query_time <= ${query_time_2 * 1000} AND`;
  }
  conditions = conditions.replace(/ AND$/g, '');
  return conditions;
});
const conditions_two = computed(() => {
  let conditions = '';
  if (date_two.value?.length > 0) {
    conditions = ` start_ts > '${date_two.value[0]}' AND start_ts <= '${date_two.value[1]}' AND`;
  }
  conditions = conditions.replace(/ AND$/g, '');
  return conditions;
});

watch(
  () => props.activeName,
  val => {
    if (val == 'slowSql') {
      init();
    }
  }
);
async function init() {
  await getSlowSqlLogData();
  await getStatisticsData();
}

function handlePageChange() {
  getSlowSqlLogData();
}
function handleSizeChange(val: any) {
  pageSize = val;
  getSlowSqlLogData();
}
function handlePageChangeTwo() {
  getStatisticsData();
}
function handleSizeChangeTwo(val: any) {
  pageSizeTwo = val;
  getStatisticsData();
}
function handlePageReset(tab: string) {
  if (tab == 'tab2') {
    getStatisticsData();
  } else {
    filterParams.query_time_1 = 10;
    filterParams.query_time_2 = undefined;
    filterParams.de_duplication = false;
    getSlowSqlLogData();
  }
}

async function getSlowSqlLogData() {
  try {
    if (requestIng.value) return;
    requestIng.value = true;
    slowSqlLogList.value = [];

    [slowSqlLogList, total] = await getSlowSqlLogs({
      currentPage: currentPage.value,
      pageSize: pageSize.value,
      conditions: conditions.value,
      deDuplication: filterParams.de_duplication,
      sortBy: query_time_sort.value
    });
    requestIng.value = false;
  } catch (error) {
    console.log('err', error);
  }
}
async function getStatisticsData() {
  try {
    if (requestIng.value) return;
    requestIng.value = true;
    statisticsList.value = [];

    [statisticsList, totalTwo] = await getSlowSqlStatistics({
      currentPage: currentPageTwo.value,
      conditions: conditions_two.value,
      pageSize: pageSizeTwo.value,
      orderSql: orderSql.value || ''
    });
    requestIng.value = false;
  } catch (error) {
    console.log('err', error);
  }
}
async function getAllSlowSqlData() {
  const dataSql = `SELECT
        ${filterParams.de_duplication ? 'LAST_ROW(start_ts) as start_ts,' : 'start_ts,'}
        db, ip, \`user\`, sql, query_time, rows_num FROM log.taos_slow_sql_detail 
        ${conditions.value ? 'WHERE' + conditions.value : ''}
        ${filterParams.de_duplication ? 'PARTITION by sql,db' : ''}
        ORDER BY start_ts DESC
      `;
  const countSql = `select count(*) from (${dataSql})`;
  await sendSQLReq(countSql);

  const res = await sendSQLReq(dataSql);
  if (res.data && res.data.length > 0) {
    return res.data.map((data: { [x: string]: any }) => {
      return Object.fromEntries(
        res.column_meta.map((item: any[], index: string | number) => {
          return [item[0], data[index]];
        })
      );
    });
  } else {
    return Object.fromEntries(
      res.column_meta.map((item: any[]) => {
        return [item[0], ''];
      })
    );
  }
}
async function exportFile() {
  const list = await getAllSlowSqlData();
  if (Array.isArray(list)) {
    list.map(item => {
      item.query_time = numToFixed(item.query_time);
    });
  }
  const FileName = 'slowSql.csv';
  const data = json2csv(list);
  const blob = new Blob(['\uFEFF' + data], {
    type: 'text/csv;charset=utf-8;'
  });
  saveAs(blob, FileName);
}
function numToFixed(num: any) {
  if (!num) return num;
  return (Number(num) / 1000).toFixed(1);
}
function customSort({ prop, order }) {
  const sortBy = order ? (order == 'descending' ? 'DESC' : 'ASC') : order;
  if (prop == 'query_time') {
    query_time_sort.value = sortBy;
    getSlowSqlLogData();
  }
  if (prop == 'max_query_time' || prop == 'avg_query_time') {
    orderSql.value = `${sortBy ? `ORDER BY ${prop} ${sortBy}` : ''}`;
    getStatisticsData();
  }
}
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}

.el-switch {
  margin-right: 10px;
}

.flex {
  display: flex;
}

:deep(.el-input-number__increase, .el-input-number__decrease) {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 30px;
}

.before {
  width: 200px;
}

.end {
  width: 80px;
}

.ds-select {
  width: 90%;
}

.my-popper {
  max-width: 600px;
  max-height: 600px;
  overflow: hidden auto;
}
</style>
