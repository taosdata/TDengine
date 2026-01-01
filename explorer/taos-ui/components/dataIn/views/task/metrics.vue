<template>
  <el-dialog v-bind="config" v-model="visible">
    <div v-loading="loading" style="padding-bottom: 20px">
      <el-tabs v-model="activeName">
        <el-tab-pane
          v-for="item in metricsArray"
          :key="item.name"
          :value="item.name"
          :name="item.name"
          :label="t(`dataIn.${item.name}Metrics`)"
        >
          <el-table border :data="item.metrics">
            <el-table-column prop="name" show-overflow-tooltip :label="t('dataIn.metricsName')" min-width="140">
              <template #default="{ row }">
                <span>{{ row.name }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="metricsDesc" show-overflow-tooltip :label="t('dataIn.metricsDesc')" min-width="300">
              <template #default="{ row }">
                <span>{{ metricsDesc[row.name] }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="name" show-overflow-tooltip :label="t('dataIn.metricsValue')" min-width="140">
              <template #default="{ row }">
                {{ handleValue(row) }}
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane v-if="type == 'tmq'" :label="t('dataIn.replicationProgress')" name="3">
          <p class="title">{{ t('dataIn.tbReplicationProgress') }}</p>
          <el-form
            ref="formRef"
            :inline="true"
            size="small"
            :model="formInline"
            class="demo-form-inline"
            :rules="rules"
          >
            <el-form-item :label="t('dataIn.tbName')" prop="table">
              <el-input v-model="formInline.table" style="width: 200px" :placeholder="t('dataIn.tbNameP')"></el-input>
            </el-form-item>
            <el-form-item :label="t('dataIn.timeRange')">
              <el-date-picker
                v-model="formInline.timeRange"
                value-format="x"
                type="datetimerange"
                :start-placeholder="t('dataIn.start')"
                :end-placeholder="t('dataIn.end')"
              >
              </el-date-picker>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="requesting_q" @click="submit">{{ t('dataIn.query') }}</el-button>
            </el-form-item>
          </el-form>
          <el-table :data="tbReplicationData" border>
            <el-table-column
              prop="table_name"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.table')"
              min-width="120"
            ></el-table-column>
            <el-table-column
              prop="from_last_ts"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.source')"
              min-width="200"
            >
              <template #default="{ row }">
                <span>{{ row.from_last_ts ? getTimeParser(convertTsToMilliseconds(row.from_last_ts)) : 'null' }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="to_last_ts" show-overflow-tooltip :label="t('dataIn.tbHeader.sink')" min-width="200">
              <template #default="{ row }">
                <span>{{ row.to_last_ts ? getTimeParser(convertTsToMilliseconds(row.to_last_ts)) : 'null' }}</span>
              </template>
            </el-table-column>
            <el-table-column
              prop="difference"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.difference')"
              min-width="130"
            >
              <template #default="{ row }">
                <span>{{ formatDuration(row.from_last_ts, row.to_last_ts) || 0 }}</span>
              </template>
            </el-table-column>
            <el-table-column
              prop="from_count"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.sourceNum')"
              min-width="180"
            ></el-table-column>
            <el-table-column
              prop="to_count"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.sinkNum')"
              min-width="170"
            ></el-table-column>
          </el-table>
          <br />
          <br />
          <p class="title">{{ t('dataIn.vgroupReplicationProgress') }}</p>
          <div style="margin-bottom: 8px" class="flex-between">
            <span>{{ t('dataIn.updateTime') }} {{ getTimeParser(update_time) }}</span>
            <el-button :loading="requesting" size="small" type="primary" @click="handleRefresh">{{
              t('dataIn.refresh')
            }}</el-button>
          </div>
          <el-table :data="vgroupData" border>
            <el-table-column
              prop="topic"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.topic')"
              min-width="140"
              :filters="filterMap.topic"
              :filter-method="filterHandler"
            ></el-table-column>
            <el-table-column
              prop="vgroup"
              sortable
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.vgroup')"
              min-width="140"
              :filters="filterMap.vgroup"
              :filter-method="filterHandler"
            >
            </el-table-column>
            <el-table-column
              prop="offset"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.offset')"
              min-width="140"
            ></el-table-column>
            <el-table-column
              prop="latest"
              show-overflow-tooltip
              :label="t('dataIn.tbHeader.latest')"
              min-width="140"
            ></el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { getPreciseDurationFromNow, convertTsToMilliseconds } from 'utils/date';
import { getTimeParser } from '../../model/util';
import { ElMessage, FormInstance } from 'element-plus';
import { getDataInProps } from '../../model/useDataIn';
import { t } from 'locales';
import { base64Utils } from 'utils';
import { instance } from 'config';

const METRIC_IN_ORDER = [
  'start_time',
  'execute_time',
  'created_stables',
  'created_tables',
  'received_batches',
  'processed_batches',
  'received_messages',
  'processed_messages',
  'processed_rows',
  'written_rows',
  'written_raw_blocks',
  'written_points',
  'rows_per_second',
  'points_per_second'
];

const formRef = shallowRef<FormInstance | null>(null);
const dataInProps = getDataInProps();

const props = withDefaults(
  defineProps<{
    data: object;
    taskId: number | string;
    type: string;
    modelValue?: boolean;
  }>(),
  {
    modelValue: false
  }
);
const emit = defineEmits(['update:modelValue', 'close', 'update']);

const visible = computed({
  get: () => props.modelValue,
  set: val => emit('update:modelValue', val)
});
const config = computed(() => {
  return {
    title: t('dataIn.metrics'),
    width: '1100px'
  };
});
const metricsArray = ref<Recordable[]>([]);
const activeName = ref('current');
const loading = ref<boolean>(true);
const requesting = ref<boolean>(false);
const requesting_q = ref<boolean>(false);
const update_time = ref<string>('');
const socket = ref<Recordable>();
const formInline = reactive({
  table: '',
  timeRange: ''
});
const tbReplicationData = ref([]);
const vgroupData = ref<Recordable[]>([]);
const metricsDesc = ref<Recordable>({});

const rules = computed(() => {
  return {
    table: [{ required: true, message: t('required', [t('dataIn.tbName')]), trigger: 'blur' }]
  };
});
const filterMap = computed(() => {
  const topicFilteredArray = [];
  const vgroupFilteredArray = [];
  const seen: Recordable = {};
  const seen1: Recordable = {};

  for (const item of vgroupData.value) {
    if (!seen[item.topic]) {
      topicFilteredArray.push({ text: item.topic, value: item.topic });
      seen[item.topic] = true;
    }
  }

  for (const item of vgroupData.value) {
    if (!seen1[item.vgroup]) {
      vgroupFilteredArray.push({ text: item.vgroup, value: item.vgroup });
      seen1[item.vgroup] = true;
    }
  }

  return {
    topic: topicFilteredArray,
    vgroup: vgroupFilteredArray
  };
});

watch(
  () => props.modelValue,
  val => {
    if (val) {
      getMetricsDesc();
      handleMetricsData(props.data);
      connect();
      if (props.type == 'tmq') {
        tbReplicationData.value = [];
        vgroupData.value = [];
        handleRefresh();
      }
    } else {
      disconnect();
    }
  },
  {
    immediate: true
  }
);

async function getMetricsDesc() {
  metricsDesc.value = await dataInProps.metrics.api.getMetricsDesc();
}

function handleValue(data: Recordable) {
  if (/start_time/i.test(data.name) && !isNaN(Number(data.value))) {
    return getTimeParser(data.value, 'YYYY-MM-DD HH:mm:ss');
  } else if (
    ['points_per_second', 'rows_per_second', 'total_points_per_second', 'total_rows_per_second'].includes(data.name)
  ) {
    return Number(data.value).toFixed(2);
  } else if (/execute_time/i.test(data.name)) {
    return getPreciseDurationFromNow(data.value);
  } else {
    return data.value;
  }
}

function formatDuration(from_last_ts: number, to_last_ts: number) {
  const from_time = convertTsToMilliseconds(from_last_ts);
  const to_time = convertTsToMilliseconds(to_last_ts);
  const diff_time = from_time - to_time;

  const formattedDuration = getPreciseDurationFromNow(diff_time, from_last_ts, to_last_ts);

  return formattedDuration;
}

function connect() {
  disconnect();
  loading.value = false;
  activeName.value = 'current';
  const user = instance.user;
  const pass = instance.password;
  const token = base64Utils.encode(user + ':' + pass);
  socket.value = new WebSocket(dataInProps.metrics.webSocketUrl + props.taskId + '/' + token);

  if (socket.value) {
    socket.value.onerror = (err: any) => {
      console.log('Error', err);
      metricsArray.value = [];
    };
    socket.value.onmessage = (ev: any) => {
      const data = JSON.parse(ev.data);

      handleMetricsData(data);
    };
  }
}
function handleMetricsData(metricsData: Recordable) {
  const array = Object.keys(metricsData).map(item => ({
    name: item,
    value: metricsData[item]
  }));
  metricsArray.value = array.map(v => {
    const metrics = [];
    for (let i = 0; i < METRIC_IN_ORDER.length; i++) {
      const item = v.value[METRIC_IN_ORDER[i]];
      if (item !== undefined) {
        metrics.push({
          name: METRIC_IN_ORDER[i],
          value: item
        });
      }
    }
    for (const key in v.value) {
      if (!METRIC_IN_ORDER.includes(key)) {
        metrics.push({
          name: key,
          value: v.value[key]
        });
      }
    }
    return { name: v.name, metrics };
  });
}

function disconnect() {
  if (socket.value) {
    console.log('Disconnecting...');
    metricsArray.value = [];
    socket.value.close();
    socket.value = undefined;
    loading.value = false;
  }
}

async function handleRefresh() {
  try {
    requesting.value = true;
    const res: any = await dataInProps.metrics.api.getVgroupProgress(props.taskId);
    if (res && res.code && res.code != 0) {
      ElMessage.error(res?.message);
      update_time.value = '';
      vgroupData.value = [];
      return;
    }
    update_time.value = res.update_time;
    vgroupData.value = res.data;
  } catch (error) {
    requesting.value = false;
  }
  requesting.value = false;
}

async function submit() {
  formRef.value?.validate(async valid => {
    if (!valid) {
      requesting_q.value = false;
      return;
    }
    try {
      requesting_q.value = true;
      const { table, timeRange } = formInline;
      console.log('output:', formInline);
      let params = 'table' + '=' + table;
      params +=
        timeRange && timeRange.length > 0
          ? `&start=${encodeURIComponent(getTimeParser(timeRange[0]))}&end=${encodeURIComponent(getTimeParser(timeRange[1]))}`
          : '';
      const res: any = await dataInProps.metrics.api.getTableProgress(props.taskId, params);
      if (res && res.code && res.code != 0) {
        ElMessage.error(res?.message);
        tbReplicationData.value = [];
        requesting_q.value = false;
        return;
      }
      requesting_q.value = false;
      tbReplicationData.value = [].concat(res);
    } catch (error) {
      requesting_q.value = false;
    }
  });
}
function filterHandler(value: string, row: Recordable, column: Recordable) {
  const property = column['property'];
  return row[property] === value;
}
</script>

<style scoped lang="scss">
.title {
  margin-bottom: 14px;
  font-size: 14px;
  font-weight: 600;
}

.demo-form-inline {
  :deep(.el-form-item__label) {
    margin-right: 0;
  }
}
</style>
