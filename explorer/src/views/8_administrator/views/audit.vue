<template>
  <div class="dnode-block">
    <section class="flex-between">
      <el-form inline size="default" :disabled="requestIng">
        <section class="flex-between">
          <div>
            <el-form-item>
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
              >
              </TimezoneDatePicker>
            </el-form-item>
            <el-form-item>
              <el-input
                v-model="filterParams.user_name"
                :placeholder="$t('taosuser.user')"
                @keyup.enter="handlePageChange()"
              ></el-input>
            </el-form-item>
            <el-form-item>
              <el-input
                v-model="filterParams.operation"
                :placeholder="$t('taosuser.operation')"
                @keyup.enter="handlePageChange()"
              ></el-input>
            </el-form-item>
          </div>
          <el-form-item>
            <el-button icon="Search" :disabled="$IS_COMMUNITY" @click="handlePageChange()">{{
              $t('search')
            }}</el-button>
          </el-form-item>
          <el-form-item>
            <el-button icon="Refresh" @click="handlePageReset()">{{ $t('reset') }}</el-button>
          </el-form-item>
        </section>
      </el-form>
      <div style="margin-bottom: 18px">
        <el-tooltip placement="top" effect="light" :open-delay="0" :disabled="!$IS_COMMUNITY">
          <template #content>
            <span v-dompurify-html="$t('communityTip')"></span>
          </template>
          <el-button
            :disabled="requestIng || $IS_COMMUNITY"
            icon="Download"
            size="default"
            type="primary"
            plain
            @click="exportFile"
            >{{ $t('console.export') }}
          </el-button>
        </el-tooltip>
      </div>
    </section>
    <el-table style="margin-top: 20px" :data="auditList" size="small">
      <el-table-column :label="$t('taosuser.time')" prop="ts" width="220">
        <template #default="scope">
          <span>{{ parsinginZone(scope.row.ts) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.clientAddress')" prop="client_address" width="180">
        <template #default="scope">
          <el-tooltip :content="scope.row.client_address" placement="top-start">
            <span class="nowrap">{{ scope.row.client_address }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.user')" prop="user_name">
        <template #default="scope">
          <el-tooltip :content="scope.row.user_name" placement="top-start">
            <span class="nowrap">{{ scope.row.user_name }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.operation')" prop="operation">
        <template #default="scope">
          <el-tooltip :content="scope.row.operation" placement="top-start">
            <span class="nowrap">{{ scope.row.operation }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.db')" prop="db">
        <template #default="scope">
          <el-tooltip :content="scope.row.db" placement="top-start">
            <span class="nowrap">{{ scope.row.db }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.resource')" prop="resource">
        <template #default="scope">
          <el-tooltip :content="scope.row.resource" placement="top-start">
            <span class="nowrap">{{ scope.row.resource }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="$t('taosuser.details')" prop="details" min-width="260">
        <template #default="scope">
          <el-tooltip :content="scope.row.details" placement="top-start">
            <span class="nowrap">{{ scope.row.details }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-model:current-page="currentPage"
      class="pagination"
      layout="total, prev, pager, next"
      :page-size="pageSize"
      :hide-on-single-page="true"
      :total="total"
      @current-change="handlePageChange"
    ></el-pagination>
  </div>
</template>
<script setup lang="ts">
import { sendSQLReq } from '@/api/explorer';
import { getAudits } from '@/api/audit';
import { getDBListReq } from '@/api/database';
import { parsinginZone } from '@/utils/index';
import { json2csv } from 'json-2-csv';
import FileSaver from 'file-saver';
import { auditMockData } from '@/const';
import { t } from '@/lang';

const globalCustomProperties: any = inject('globalCustomProperties');
const { $IS_COMMUNITY } = globalCustomProperties;

const props = defineProps({
  activeName: {
    type: String,
    default: ''
  }
});
const pageSize = ref(20);
const currentPage = ref(1);
const total = ref(10);
const requestIng = ref(false);
const dblist = ref([]);
interface Audit {
  ts: string;
  client_address: string;
  user_name: string;
  operation: string;
  db: string;
  resource: string;
  details: string;
}

const auditList = ref<Audit[]>([]);
const filterParams = reactive({
  user_name: '',
  operation: ''
});
const date = ref([]);

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
    const start = date.value[0];
    const end = date.value[1];
    conditions = ` ts > '${start}' AND ts <= '${end}' AND`;
  }
  const currentFilterParams: Record<string, any> = { ...filterParams };
  for (const key in currentFilterParams) {
    if (!currentFilterParams[key]) {
      delete currentFilterParams[key];
    } else {
      conditions += ` ${key} = '${currentFilterParams[key]}' AND`;
    }
  }
  conditions = conditions.replace(/ AND$/g, '');
  return conditions;
});

watch(
  () => props.activeName,
  val => {
    if (val == 'audit' && !$IS_COMMUNITY) {
      getDatabases();
      getAuditData();
    }
  }
);

function handlePageChange() {
  if (!$IS_COMMUNITY) {
    getAuditData();
  }
}
function handlePageReset() {
  if (!$IS_COMMUNITY) {
    filterParams.operation = '';
    filterParams.user_name = '';
    date.value = [];
    getAuditData();
  }
}
async function getAuditData() {
  try {
    if (requestIng.value) return;
    requestIng.value = true;

    [auditList.value, total.value] = await getAudits({
      currentPage: currentPage.value,
      pageSize: pageSize.value,
      conditions: conditions.value
    });

    requestIng.value = false;
  } catch (error) {
    console.log('err');
  }
}
async function getDatabases() {
  try {
    dblist.value = await getDBListReq();
  } catch (err) {
    return Promise.reject(err);
  }
}
async function getAllAuditData() {
  const res = await sendSQLReq(`select * from audit.operations ${conditions.value ? 'where' + conditions.value : ''}`);
  if (res.data && res.data.length > 0) {
    return res.data.map((data: any) => {
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
  const exportAuditList = await getAllAuditData();
  const FileName = 'audit.csv';
  const data = json2csv(exportAuditList);
  const blob = new Blob(['\uFEFF' + data], {
    type: 'text/csv;charset=utf-8;'
  });
  FileSaver.saveAs(blob, FileName);
}

function init() {
  if ($IS_COMMUNITY) {
    auditList.value = auditMockData;
  } else {
    getDatabases();
    getAuditData();
  }
}
init();
</script>
<style lang="scss" scoped>
.el-select {
  width: 100%;
}

.el-switch {
  margin-right: 10px;
}
</style>
