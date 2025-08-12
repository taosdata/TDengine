<template>
  <div v-loading="requestIng" style="height: 100%">
    <PageTitle
      :title="t('dataIn.datasource')"
      :add-title="t('dataIn.addsource')"
      :request-ing="requestIng"
      :is-community="dataInProps.isCommunity"
      @add="addDbSource"
      @refresh="refresh"
    >
      <el-tooltip
        placement="top-start"
        effect="light"
        :open-delay="0"
        :disabled="!isDisabled"
        :content="t('dataIn.batchOperateTip', [`${t('dataIn.start')}`])"
      >
        <el-button
          link
          type="primary"
          size="default"
          icon="VideoPlay"
          :disabled="isDisabled || dataInProps.isCommunity"
          @click="handleBatchTask('start')"
          >{{ startCase(t('dataIn.start') + t('dataIn.task')) }}</el-button
        >
      </el-tooltip>
      <el-tooltip
        :content="t('dataIn.batchOperateTip', [`${t('dataIn.stop')}`])"
        :disabled="!isDisabled"
        placement="top-start"
        effect="light"
        :open-delay="0"
      >
        <el-button
          link
          type="primary"
          size="default"
          icon="VideoPause"
          :disabled="isDisabled || dataInProps.isCommunity"
          @click="handleBatchTask('stop')"
          >{{ startCase(t('dataIn.stop') + t('dataIn.task')) }}</el-button
        >
      </el-tooltip>
      <el-tooltip
        :content="t('dataIn.batchOperateTip', [`${t('dataIn.delete')}`])"
        :disabled="!isDisabled"
        placement="top-start"
        effect="light"
        :open-delay="0"
      >
        <el-button
          link
          type="primary"
          size="default"
          icon="Delete"
          :disabled="isDisabled || dataInProps.isCommunity"
          @click="handleBatchTask('delete')"
          >{{ startCase(t('dataIn.delete') + t('dataIn.task')) }}</el-button
        >
      </el-tooltip>

      <el-button
        link
        type="primary"
        size="default"
        icon="Sell"
        :disabled="isDisabled || dataInProps.isCommunity"
        @click="handleExportTask"
        >{{ startCase(t('dataIn.export') + t('dataIn.task')) }}</el-button
      >

      <task-import @import-o-k="refresh" />
    </PageTitle>
    <div>
      <el-table
        ref="dataSourceTableRef"
        class="tasks-table with-operations"
        style="margin-top: 20px"
        :data="taskList"
        size="default"
        :max-height="maxHeight"
        row-key="id"
        @selection-change="handleSelectionChange"
        @cell-click="clickAgent"
        @cell-mouse-enter="onTaskTableMouseEnter"
        @cell-mouse-leave="onTaskTableMouseLeave"
      >
        <el-table-column type="selection" :reserve-selection="true" width="50"> </el-table-column>
        <el-table-column type="expand">
          <template #default="rowData">
            <Activities :data="rowData.row.activities" />
          </template>
        </el-table-column>
        <el-table-column v-if="false" :label="t('dataIn.taskid')" prop="taskid" width="80">
          <template #default="scope">
            <span>
              <i class="el-circle" :class="getStatusClass(scope.row.healthStatus)"></i>
            </span>
            <span style="padding-left: 5px">{{ scope.row.taskid }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="t('dataIn.name2')" sortable prop="localname" min-width="100">
          <template #default="scope">
            <span>
              <i class="el-circle mr-5px" :class="getStatusClass(scope.row.healthStatus)"></i>
            </span>
            <el-tooltip :content="scope.row.localname" placement="top-start">
              <span class="nowrap">{{ scope.row.localname }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column
          :label="t('dataIn.type')"
          prop="localtype"
          width="180"
          sortable
          :filters="filterMap.type"
          :filter-method="filterHandler"
        >
          <template #default="scope">
            <el-tooltip :content="scope.row.localtype" placement="top-start">
              <span class="nowrap">{{ scope.row.localtype }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column :label="t('dataIn.target')" prop="target" width="120">
          <template #default="scope">
            <el-tooltip :content="scope.row.target" placement="top-start">
              <span class="nowrap">{{ scope.row.target }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column :label="t('dataIn.createat')" prop="created_at" width="220">
          <template #default="scope">
            <span>{{ getTimeParser(scope.row.created_at) }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="t('dataIn.via')" prop="via" width="80">
          <template #default="{ row }">
            <el-tooltip :content="agentMap[row.via]" placement="top-start">
              <span class="nowrap" style="cursor: pointer">{{ agentMap[row.via] }}</span>
            </el-tooltip>
          </template>
        </el-table-column>

        <el-table-column :label="t('dataIn.metrics')" prop="finished_at" width="120">
          <template #default="scope">
            <el-button
              size="small"
              style="font-size: 12px; color: #4d6992"
              :disabled="scope.row.status.toLowerCase() == 'cancelled' || dataInProps.isCommunity"
              @click="viewMetrics(scope.row, scope.row.status.toLowerCase())"
              >{{ t('common.view') }}</el-button
            >
          </template>
        </el-table-column>

        <el-table-column
          :label="t('dataIn.status')"
          prop="status"
          sortable
          :filters="filterMap.status"
          :filter-method="filterHandler"
          width="150"
        >
          <template #default="scope">
            <div class="status-operation" style="display: flex; white-space: nowrap">
              <el-tooltip
                v-if="showErrStatus.includes(scope.row.status.toLowerCase())"
                placement="bottom"
                effect="light"
                popper-class="datain"
              >
                <template #content>
                  <div v-dompurify-html="scope.row.reason" style="max-height: 200px; overflow: auto"></div>
                </template>
                <span style="display: inline-block; width: 80px">{{ getStatusText(scope.row.status) }}</span>
              </el-tooltip>
              <span v-else style="display: inline-block; width: 80px">{{ getStatusText(scope.row.status) }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column
          :label="t('dataIn.healthStatusTitle')"
          prop="healthStatus"
          width="120"
          sortable
          :filters="filterMap.healthStatus"
          :filter-method="filterHandler"
        >
          <template #default="scope">
            <div
              v-if="showHealthStatus.includes(scope.row.status)"
              class="status-operation"
              style="display: flex; white-space: nowrap"
            >
              <el-tooltip placement="bottom" effect="light" popper-class="datain">
                <template #content>
                  <div
                    v-dompurify-html="
                      scope.row.healthStatus ? t('dataIn.healthStatus.' + scope.row.healthStatus + 'Desc') : ''
                    "
                    style="max-height: 200px; overflow: auto"
                  ></div>
                </template>
                <span style="display: inline-block; width: 80px">{{
                  scope.row.healthStatus ? t('dataIn.healthStatus.' + scope.row.healthStatus) : ''
                }}</span>
              </el-tooltip>
            </div>
          </template>
        </el-table-column>
        <el-table-column class="with-operations" width="50">
          <template #default="scope">
            <el-dropdown class="operations" :class="{ show: scope.row.hover }">
              <el-button icon="MoreFilled" size="small" class="rotate-90!" text></el-button>
              <template #dropdown>
                <el-dropdown-menu @mouseenter="onMenuMouseEnter" @mouseleave="onMenuMouseLeave">
                  <template v-if="permitStartStatus.includes(scope.row.status.toLowerCase())">
                    <el-dropdown-item @click="start(scope.row)">
                      <el-icon><VideoPlay /></el-icon>
                      {{ t('dataIn.excutestart').replace('{name}', scope.row.name) }}
                    </el-dropdown-item>
                  </template>
                  <template v-if="permitStopStatus.includes(scope.row.status.toLowerCase())">
                    <el-dropdown-item @click="stop(scope.row)">
                      <el-icon><VideoPause /></el-icon>
                      {{ t('dataIn.excutestop').replace('{name}', scope.row.name) }}
                    </el-dropdown-item>
                  </template>
                  <el-dropdown-item @click="refreshCurrentTask(scope.row)">
                    <el-icon><Refresh /></el-icon>
                    {{ t('common.refresh') }}
                  </el-dropdown-item>
                  <el-dropdown-item @click="exportCurrentTask(scope.row)">
                    <el-icon><Sell /></el-icon>
                    {{ t('common.export') }}
                  </el-dropdown-item>
                  <el-dropdown-item
                    :disabled="
                      scope.row.disableEdit ||
                      (dataInProps.isCommunity ? dataInProps.isCommunity : scope.row.from === undefined) ||
                      !getEditStatus(scope.row.labels)
                    "
                    @click="edit(scope.row, scope.row.status.toLowerCase())"
                  >
                    <el-icon><Edit /></el-icon>
                    {{ t('dataIn.editconfig') }}
                  </el-dropdown-item>
                  <el-dropdown-item @click="copyTask(scope.row, scope.row.status.toLowerCase())">
                    <el-icon><DocumentCopy /></el-icon>
                    {{ t('common.copy') }}
                  </el-dropdown-item>
                  <el-dropdown-item @click="del(scope.row)">
                    <el-icon><Delete /></el-icon>
                    {{ t('common.delete') }}
                  </el-dropdown-item>
                  <template v-if="scope.row.from.type === 'kafka'">
                    <el-dropdown-item @click="confirmSkipToLatest(scope.row)">
                      <el-icon><DArrowRight /></el-icon>
                      {{ t('dataIn.tipForSkip') }}
                    </el-dropdown-item>
                  </template>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
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
    <Metrics v-model="isMetricsVisible" v-bind="metricsConfig" />
    <el-alert
      v-if="dataInProps.isCommunity"
      class="my-alert"
      style="margin-top: 8px"
      type="warning"
      :description="t('common.communityDemoDataTip')"
      :closable="true"
      center
    />
  </div>

  <el-dialog v-model="dlgConfirmSeek2End" :title="$t('tips')" width="700px">
    <div>
      <div style="margin-bottom: 10px; font-size: 16px">
        {{ t('dataIn.skip2Latest', [taskToSeek.name]) }}
      </div>
      <div>
        <el-checkbox v-model="isRecoverHistoryData" style="margin-left: 10px">
          {{ t('dataIn.redoPiledupData') }}
        </el-checkbox>
      </div>
    </div>

    <template #footer>
      <div class="dialog-footer">
        <el-button class="w100" @click="dlgConfirmSeek2End = false">{{ $t('cancel') }}</el-button>

        <el-button v-loading="requestIng" class="w100" type="primary" @click="skipToLatest">{{
          $t('confirm')
        }}</el-button>
      </div>
    </template>
  </el-dialog>
</template>
<script setup lang="ts">
import { startCase } from 'lodash-es';
import {
  getTimeParser,
  agentId,
  agentList,
  currentTaskStatus,
  getSourceConfig,
  dataInMockData
} from '../../model/util';
import { downloadByData } from '../../../../utils/files';
import Metrics from './metrics.vue';
import Activities from '../../components/activities.vue';
import PageTitle from '../../components/pageTitle.vue';
import TaskImport from '../../components/task-import.vue';
import { ElMessage, ElMessageBox } from 'element-plus';
import { getDataInProps } from '../../model/useDataIn';
import { useActivitySubscription, ActivitieProps } from '../../model/useWebSoket';
import { useRouter } from 'hooks/useCurrentRouter';
import { t } from 'locales';
const router = useRouter();

const dataInProps = getDataInProps();
const isMetricsVisible = ref<boolean>(false);

const connectData: Recordable = reactive({
  activity: null,
  close: null
});

const hasConnect = ref<boolean>(false);

const metricsConfig = reactive({
  type: '',
  taskId: '',
  data: {}
});
const dataSourceTableRef = ref();
const pageSize = ref(10);
const currentPage = ref(1);
const total = ref(10);
const taskList = ref<any[]>([]);
const requestIng = ref<boolean>(false);
const maxHeight = ref(500);
// 不允许 start/stop 的状态 sopping, suspending
const permitStartStatus = ['created', 'failed', 'stopped', 'suspended', 'completed'];
const permitStopStatus = ['queued', 'running', 'interrupted', 'waiting', 'resumed'];
const showErrStatus = ['waiting', 'suspending', 'suspended', 'failed', 'interrupted'];
const permitDeleteStatus = ['completed', 'stopped', ' failed', 'interrupted', 'ticked'];
const showHealthStatus = ['running', 'stopping', 'waiting', 'resumed'];
const multipleSelection = ref<any[]>([]);
import { isEn } from 'config';

const filterMap: Recordable = reactive({
  type: [],
  status: [],
  healthStatus: [],
  healthStatusFilterSet: {}
});

const dataSourceMap: Recordable = reactive({});

const agentMap = computed(() => {
  return agentList.value.reduce((pre, cur) => {
    pre[cur.id] = cur.name;
    return pre;
  }, {});
});
const isDisabled = computed(() => {
  return multipleSelection.value.length < 1;
});

watch(
  () => connectData.activity,
  (newVal: ActivitieProps) => {
    nextTick(() => {
      handleTaskActivities(newVal);
      getHealthStatusFilters();
    });
  },
  {
    immediate: true,
    deep: true
  }
);

async function getList() {
  const activityOfTask: any = {};
  taskList.value.forEach(item => {
    activityOfTask[item.id] = item.activities;
  });

  taskList.value = [];
  const result: any = await dataInProps.task.api.getTask('datain');
  if (result.desc || result.message) {
    throw result.desc || result.message;
  }

  if (result) {
    const dataSourceFilterSet: Recordable = {};
    const statusFilterSet: Recordable = {};
    taskList.value = result.map((item: any) => {
      item.from = item.from_json;
      if (!dataSourceFilterSet[item.from.type]) {
        filterMap.type.push({
          value: item.from.type,
          text: dataSourceMap[item.from.type] // 等数据源确定后再修改
        });
        dataSourceFilterSet[item.from.type] = true;
      }

      item['statusText'] = getStatusText(item.status);
      if (!statusFilterSet[item.status]) {
        filterMap.status.push({
          value: item.status,
          text: item.statusText
        });
        statusFilterSet[item.status] = true;
      }
      (item['taskid'] = item.id), (item['localname'] = item.name);
      item['localtype'] = dataSourceMap[item.from.type] ? dataSourceMap[item.from.type] : '';
      item['target'] = item.to_expand?.subject || '';
      item['created_at'] = item.created_at ? item.created_at.replace(/(?<=\.)\S+$/, '').replace('.', '') + 'Z' : '';
      item['activities'] = reactive(activityOfTask[item.id] || []);
      // item['disableEdit'] = item.from.type === 'csv' && item.from.data.csvData.currentTab === 'upload_csv_file';
      return item;
    });
  }
  // 刷新页面获取完数据后建立连接为了获取历史数据
  closeConnect();

  nextTick(() => {
    if (!hasConnect.value) {
      hasConnect.value = true;
      const { activity, close } = useActivitySubscription(dataInProps.task.webSoketUrl);
      connectData.activity = activity;
      connectData.close = close;
    } else {
      closeConnect();
    }
  });
}

function stop(data: Recordable) {
  try {
    ElMessageBox.confirm(t('dataIn.stoptip', [data.name]), t('common.warning'), {
      confirmButtonText: t('common.confirm'),
      cancelButtonText: t('common.cancel'),
      type: 'warning'
    }).then(async () => {
      const result: any = await dataInProps.task.api.stop(data.id);
      if (result?.message) {
        ElMessage({
          dangerouslyUseHTMLString: true,
          message: `<strong>${result.message.replaceAll('\n', '<br/>')}</strong>`,
          type: 'warning'
        });
        return;
      }
      await refresh();
    });
  } catch (err) {
    return Promise.reject(err);
  }
}

function start(data: Recordable) {
  try {
    ElMessageBox.confirm(t('dataIn.starttip', [data.name]), t('common.warning'), {
      confirmButtonText: t('common.confirm'),
      cancelButtonText: t('common.cancel'),
      type: 'warning'
    }).then(async () => {
      const result: any = await dataInProps.task.api.start(data.id);
      if (result && result.message) {
        ElMessage({
          dangerouslyUseHTMLString: true,
          message: `<strong>${result.message.replaceAll('\n', '<br/>')}</strong>`,
          type: 'warning'
        });
        return;
      }
      await refresh();
    });
  } catch (err) {
    return Promise.reject(err);
  }
}

function del(data: Recordable) {
  ElMessageBox.confirm(t('dataIn.deletetip', [data.name]), t('dataIn.warning'), {
    confirmButtonText: t('dataIn.ok'),
    cancelButtonText: t('dataIn.cancel'),
    type: 'warning'
  }).then(async () => {
    const result: any = await dataInProps.task.api.delete(data.id);
    if (result?.message) {
      ElMessage.warning(result.message);
      return;
    }
    ElMessage({
      type: 'success',
      message: t('dataIn.deleteok')
    });
    await refresh();
  });
}

async function refreshCurrentTask(data: Recordable) {
  try {
    const result: any = await dataInProps.task.api.refreshTask(data.taskid);
    if (result && (result.message || result.desc)) {
      ElMessage.error(result.message || result.desc);
      return;
    }
    const index = taskList.value.findIndex(item => item.taskid == data.taskid);
    const theActivities = taskList.value[index]['activities'] || [];
    taskList.value.splice(
      index,
      1,
      [].concat(result).map((item: any) => {
        (item['taskid'] = item.id), (item['localname'] = item.name);
        item['created_at'] = item.created_at ? item.created_at.replace(/(?<=\.)\S+$/, '').replace('.', '') + 'Z' : '';
        // item['disableEdit'] = item.from.type === 'csv' && item.from.data.csvData.currentTab === 'upload_csv_file';
        item['localtype'] = dataSourceMap[item.from.type] ? dataSourceMap[item.from.type] : '';
        item['target'] = item.to_expand?.subject || '';
        item['statusText'] = getStatusText(item.status);
        item['activities'] = reactive(theActivities);
        return item;
      })[0]
    );
    refreshCurrentSelection(data.taskid);
    ElMessage.success(t('dataIn.refreshsuccess'));
  } catch (error) {
    console.log(error);
  }
}

async function exportCurrentTask(data: Recordable) {
  try {
    requestIng.value = true;
    const res = await dataInProps.task.api.batchExportTask([data.id]);

    if (res && res.code) {
      return ElMessage.error(res.message);
    }
    downloadByData(res as BlobPart, `datain-tasks-${data.id}.json`);
    setTimeout(() => {
      requestIng.value = false;
    }, 1000);
  } catch (err) {
    return Promise.reject(err);
  }
}

function handlerConfirm(
  content: string,
  excuteFn: RequestApiFn<Recordable[]> | null,
  ids: any[],
  showConfirmButton: boolean
) {
  try {
    ElMessageBox.confirm(content, t('dataIn.warning'), {
      confirmButtonText: t('dataIn.ok'),
      cancelButtonText: t('dataIn.cancel'),
      type: 'warning',
      confirmButtonClass: showConfirmButton ? '' : 'not-show'
    }).then(async () => {
      await excuteFn!({ ids });
      dataSourceTableRef.value.clearSelection();
      await refresh();
    });
  } catch (err) {
    return Promise.reject(err);
  }
}

async function handleExportTask() {
  try {
    requestIng.value = true;
    const ids = multipleSelection.value.map(item => item.id);
    const res = await dataInProps.task.api.batchExportTask(ids);

    if (res && res.code) {
      return ElMessage.error(res.message);
    }
    downloadByData(res as BlobPart, `datain-tasks-${ids.join()}.json`);
    setTimeout(() => {
      requestIng.value = false;
    }, 1000);

    dataSourceTableRef.value.clearSelection();
  } catch (err) {
    return Promise.reject(err);
  }
}

async function handleBatchTask(type: string) {
  let ids: any[] = [];
  let content: string = '';
  let excuteFn: RequestApiFn<Recordable[]> | null = null;
  let showConfirmButton: boolean = true;
  // requestIng.value = true;
  switch (type) {
    case 'start':
      ids = filterBatchIds(permitStartStatus);
      excuteFn = dataInProps.task.api.batchStartTask;
      content = t('dataIn.taskStart', [ids]);
      break;

    case 'stop':
      ids = filterBatchIds(permitStopStatus);
      excuteFn = dataInProps.task.api.batchStopTask;
      content = t('dataIn.taskStop', { ids });
      break;

    case 'delete':
      ids = filterBatchIds(permitDeleteStatus);
      excuteFn = dataInProps.task.api.batchDelTask;
      content = t('dataIn.taskDel', [ids]);
      break;
  }
  if (ids.length < 1) {
    showConfirmButton = false;
    content = t('dataIn.noTaskOperateTip', [`${t(`dataIn.${type}`)}`]);
  }
  handlerConfirm(content, excuteFn, ids, showConfirmButton);
}

function handlePageChange() {}
//非root用户不能修改root下创建的数据源
function getEditStatus(data: string[]) {
  const currentUser = localStorage.getItem('username');
  if (data) {
    const result = data
      .filter(item => item.includes('user'))
      .toString()
      .split('::');
    return currentUser == 'root' || result[1] == currentUser;
  } else {
    return false;
  }
}

async function viewMetrics(data: Recordable, status: string) {
  try {
    const result: any = await dataInProps.metrics.api.getMetrics(data.id);
    if (result.message) {
      ElMessage.error(result.message);
      return;
    }
    if (Object.keys(result).length === 0) {
      switch (status) {
        case 'running':
          ElMessage.error(t('dataIn.metricTips.running'));
          return;
        case 'completed':
          ElMessage.error(t('dataIn.metricTips.completed'));
          return;
        case 'stopped':
          ElMessage.error(t('dataIn.metricTips.stopped'));
          return;
      }
    }
    isMetricsVisible.value = true;
    metricsConfig.taskId = data.id;
    metricsConfig.type = data.from.type;
    metricsConfig.data = result;
  } catch (error) {
    console.log(error);
  }
}

function addDbSource() {
  router.push({
    path: '/dataIn/add'
  });
}

async function edit(data: Recordable, status: string) {
  currentTaskStatus.value = status;
  router.push({
    path: `/dataIn/${data.id}/${data.from.type}/edit`
  });
}
//copy一个新的task
async function copyTask(data: Recordable, status: string) {
  currentTaskStatus.value = status;
  router.push({
    path: `/dataIn/${data.id}/${data.from.type}/copy`
  });
}

async function refresh() {
  try {
    requestIng.value = true;
    await getList();
  } catch (err: any) {
    ElMessage.error(err);
    requestIng.value = false;
    return;
  }
  requestIng.value = false;

  // getAgentList(dataInProps.agent.api);

  dataSourceTableRef.value.clearSelection();
}

// 先勾选再刷新单独任务的时候更新勾选的数据
function refreshCurrentSelection(taskid: string | number) {
  if (multipleSelection.value.length <= 0) return;
  const filterRow = taskList.value.filter(item => item.taskid == taskid);
  multipleSelection.value = multipleSelection.value.map((item: any) => {
    if (item.taskid == taskid) {
      item = { ...filterRow[0] };
    }
    return item;
  });
}

function filterHandler(value: string, row: Recordable, column: Recordable) {
  const property = column['property'];
  return row[property] === value;
}

function handleTaskActivities(activity: ActivitieProps) {
  taskList.value.forEach(task => {
    if (String(activity?.id) === String(task.taskid)) {
      // 初始化 task.activities
      if (!Array.isArray(task.activities)) {
        task.activities = reactive([]);
      }

      if (task.activities.length > 0 && task.activities[0].at === activity.at) {
        return;
      }

      task.activities.unshift(activity);

      // 保持 activities 数组的长度不超过 10 条
      if (task.activities.length > 10) {
        task.activities.splice(10, task.activities.length - 10);
      }

      task['healthStatus'] = getHealthStatus(task.activities, task?.healthStatus as string);
    }
  });
}

function getHealthStatus(activities: ActivitieProps[], lastHealthStatus: string) {
  for (const activity of activities) {
    if (activity.status === 'health') {
      return activity.activity !== lastHealthStatus ? activity.activity : lastHealthStatus;
    }
  }
}
function getStatusText(value: string): string {
  return value ? t('dataIn.statuses.' + value) : '';
}

function clickAgent(row: Recordable, column: Recordable) {
  if (column.property === 'via' && row.via) {
    router.push({
      path: '/dataIn/agent'
    });
    agentId.value = row.via;
  }
}
function handleSelectionChange(val: []) {
  multipleSelection.value = val;
}
const hoverTimeout: Record<string, ReturnType<typeof setTimeout>> = {};
let hoverTimeoutCache: any[] = [];

function onTaskTableMouseEnter(d1: any) {
  if (hoverTimeout[d1.id]) {
    clearTimeout(hoverTimeout[d1.id]);
    delete hoverTimeout[d1.id];
  }
  hoverTimeout[d1.id] = setTimeout(() => {
    d1.hover = true;
  }, 100); // 100ms delay
}

function onTaskTableMouseLeave(d1: any) {
  if (hoverTimeout[d1.id]) {
    clearTimeout(hoverTimeout[d1.id]);
    delete hoverTimeout[d1.id];
  }
  hoverTimeout[d1.id] = setTimeout(() => {
    d1.hover = false;
  }, 100); // 100ms delay
}

function onMenuMouseEnter() {
  // 清除 hoverTimeout，防止鼠标移入菜单时触发 hover 状态
  Object.keys(hoverTimeout).forEach(key => {
    clearTimeout(hoverTimeout[key]);
    hoverTimeoutCache.push(key);
    delete hoverTimeout[key];
  });
}

function onMenuMouseLeave() {
  const cache = hoverTimeoutCache;
  hoverTimeoutCache = [];
  cache.forEach(v => {
    taskList.value.forEach(item => {
      if (item.id == v) {
        item.hover = false;
      }
    });
  });
}
function filterBatchIds(permitStatus: string[]): string[] {
  const result: string[] = [];
  multipleSelection.value.filter((item: any) => {
    if (permitStatus.includes(item.status)) {
      result.push(item.id);
    }
  });
  return result;
}

function handleResize() {
  const windowHeight = window.innerHeight;
  maxHeight.value = windowHeight - 300;
}
function getHealthStatusFilters() {
  taskList.value.forEach(item => {
    if (!filterMap.healthStatusFilterSet[item.healthStatus]) {
      filterMap.healthStatus.push({
        value: item.healthStatus,
        text: item.healthStatus ? t('dataIn.healthStatus.' + item.healthStatus) : ''
      });
      filterMap.healthStatusFilterSet[item.healthStatus] = true;
    }
  });
}
function getStatusClass(status: string) {
  let name = '';
  switch (status) {
    case 'ready':
    case 'idle':
      name = 'circle-bg-green';
      break;
    case 'busy':
      name = 'circle-bg-orange';
      break;
    case 'bounce':
    case 'source_error':
    case 'sink_error':
      name = 'circle-bg-pink';
      break;
    case 'fatal':
      name = 'err-circle';
      break;
    default:
      name = 'circle-bg-green';
  }
  return name;
}

function closeConnect() {
  hasConnect.value = false;
  if (connectData && connectData.close) {
    connectData.close(dataInProps.task.webSoketUrl);
  }
}

onMounted(() => {
  const defaultConfig = getSourceConfig(isEn.value);
  defaultConfig.definitionsList.forEach(item => {
    dataSourceMap[item.id] = item.name;
  });

  nextTick(() => {
    handleResize();
  });
  window.addEventListener('resize', handleResize);
  if (dataInProps.isCommunity) {
    taskList.value = dataInMockData;
  } else {
    refresh();
  }
});
onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize);
  closeConnect();
});

const dlgConfirmSeek2End = ref(false);
const isRecoverHistoryData = ref(false);
const taskToSeek = ref<Recordable>({});

const confirmSkipToLatest = (item: Recordable) => {
  taskToSeek.value = {
    id: item.id,
    name: item.localname
  };
  dlgConfirmSeek2End.value = true;
};

const skipToLatest = async () => {
  try {
    await dataInProps.task.api.skip2Latest(taskToSeek.value.id, isRecoverHistoryData.value);
  } catch (error) {
    console.log(error);
    ElMessage.error(error);
  } finally {
    dlgConfirmSeek2End.value = false;
    isRecoverHistoryData.value = false;
    await refresh();
  }
};
</script>
<style lang="scss">
.el-tooltip__popper {
  max-width: 450px !important;
}

.not-show {
  display: none;
}
</style>
<style lang="scss" scoped>
:deep(.el-form-item__label) {
  margin-right: 100px;
  white-space: nowrap !important;
}

.el-form-item {
  display: flex;
}

.w100 {
  width: 100px;
}

:deep(.el-form-item--mini .el-form-item__content) {
  margin-left: 0 !important;
}

:deep(.el-input--mini .el-input__inner),
:deep(.el-input.el-input--mini.el-input--suffix) {
  width: 172px !important;
}

:deep(.input.el-input__inner) {
  width: 172px !important;
}

:deep(.el-button.is-link) {
  padding: 8px 10px;
}

.tabel-expand {
  min-width: 70%;
  padding: 0 5px;
  margin-left: 40px;

  :deep(.el-table th.el-table__cell.is-leaf) {
    border: none !important;
  }

  :deep(.el-table td.el-table__cell) {
    border: none !important;
  }
}

// 配合将 max-height 设置为百分比
// .data-source {
//   &:deep(.el-table) {
//     display: flex;
//     flex-direction: column;
//   }
//   &:deep(.el-table__header-wrapper) {
//     min-height: 30px;
//   }
// }

:deep(.el-table td.el-table__cell) div {
  word-break: break-word;
  word-wrap: break-word;
}

.el-circle {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
}

.err-circle {
  background-color: #fe6c6c;
  animation: circle 1s infinite;
}

.circle-bg-pink {
  background-color: pink;
}

.circle-bg-green {
  background-color: #67c23a;
}

.circle-bg-orange {
  background-color: #e6a23c;
}

.my-alert :deep(.el-alert .el-alert__description) {
  font-size: 14px;
}

@keyframes circle {
  0% {
    opacity: 1;
  }

  100% {
    opacity: 0;
  }
}

td {
  .operations.show {
    display: block;
  }

  .operations {
    position: absolute;
    top: 20%;
    right: 30px;
    z-index: 1;
    display: none;
    width: max-content;
    height: 100%;
    vertical-align: middle;
    cursor: default;
  }
}
</style>
