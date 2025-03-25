<template>
  <div class="data-agent">
    <PageTitle
      :title="t('dataIn.agent')"
      :add-title="t('dataIn.taosxAgent.createnewagent')"
      :request-ing="requestIng"
      :is-community="dataInProps.isCommunity"
      :is-disabled-add="true"
      @add="add"
      @refresh="refresh"
    />
    <el-table
      ref="singleTableRef"
      style="margin-top: 20px"
      :data="agentList"
      size="small"
      row-key="id"
      :max-height="maxHeight"
      highlight-current-row
    >
      <el-table-column type="expand">
        <template #default="rowData">
          <Activities :data="rowData.row.activities" />
        </template>
      </el-table-column>
      <el-table-column label="ID" prop="id"></el-table-column>
      <el-table-column :label="t('dataIn.taosxAgent.name')" prop="name"></el-table-column>

      <el-table-column :label="t('dataIn.taosxAgent.created_at')" prop="created_at">
        <template #default="scope">
          <span>{{ getTimeParser(scope.row.created_at) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('dataIn.taosxAgent.status')" prop="status">
        <template #default="scope">
          <span>{{ getStatusText(scope.row.status) }}</span>
        </template>
      </el-table-column>

      <el-table-column :label="t('common.action')" width="100">
        <template #default="scope">
          <el-button
            plain
            size="small"
            icon="Edit"
            :disabled="dataInProps.isCommunity"
            @click="edit(scope.row)"
          ></el-button>
          <el-button
            plain
            size="small"
            icon="Delete"
            :disabled="dataInProps.isCommunity"
            @click="del(scope.row)"
          ></el-button>
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
    <el-dialog
      v-model="showAgent"
      :title="dialogTitle"
      :destroy-on-close="true"
      :close-on-click-modal="false"
      @close="closeDialog"
    >
      <AddAgent
        :key="componentKey"
        :agent="currentRow"
        :agent-list="agentList"
        @update="getAgents"
        @close="closeDialog"
      ></AddAgent>
    </el-dialog>
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
</template>

<script setup lang="ts">
import { ElMessage, type TableInstance } from 'element-plus';
import { getTimeParser, agentId, agentList, getAgentList, agentMockData } from '../../model/util';
import AddAgent from './addAgent.vue';
import Activities from '../../components/activities.vue';
import PageTitle from '../../components/pageTitle.vue';
import { getDataInProps } from '../../model/useDataIn';
import { useActivitySubscription, ActivitieProps } from '../../model/useWebSoket';
import { t } from 'locales';

const dataInProps = getDataInProps();
const singleTableRef = ref<TableInstance>();

interface Agent {
  id: number;
  name: string;
  created_at?: string;
  token?: string;
}

const showAgent = ref(false);
const requestIng = ref(false);
const isEditDialog = ref(false);
const dialogTitle = ref('');
const pageSize = ref(10);
const currentPage = ref(1);
const total = ref(10);
const currentRow = ref<Agent>();
// const expandRowKeys = ref<number[]>([]);
const maxHeight = ref(500);
const componentKey = ref(0);
const connectData: Recordable = reactive({
  activity: null,
  close: null
});
const hasConnect = ref<boolean>(false);

watch(
  agentId,
  via => {
    nextTick(() => {
      const row = agentList.value.find(item => item.id == via);
      if (row) setCurrent(row as Agent);
    });
  },
  {
    immediate: true
  }
);

watch(
  () => connectData.activity,
  newActivity => {
    handleTaskActivities(newActivity);
  },
  {
    immediate: true,
    deep: true
  }
);

function closeConnect() {
  connectData.close(dataInProps.agent.webSoketUrl);
  hasConnect.value = false;
}

onMounted(() => {
  if (dataInProps.isCommunity) {
    agentList.value = agentMockData;
  } else {
    getAgents();
  }
  nextTick(() => handleResize());
  window.addEventListener('resize', handleResize);
});

onBeforeUnmount(() => {
  agentId.value = '';
  window.removeEventListener('resize', handleResize);
  closeConnect();
});

const closeDialog = () => {
  showAgent.value = false;
};

const handlePageChange = () => {};

const del = (data: Agent) => {
  ElMessageBox.confirm(t('dataIn.taosxAgent.deletetip').replace(/{id}/, data.id.toString()), t('common.warning'), {
    confirmButtonText: t('common.confirm'),
    cancelButtonText: t('common.cancel'),
    type: 'warning'
  }).then(async () => {
    try {
      const res: any = await dataInProps.agent.api.deleteAgent(data.id);
      res?.message && ElMessage.error(res.message);
      refresh();
    } catch (err: any) {
      err?.response?.data?.message && ElMessage.error(err.response.data.message);
    }
  });
};

const add = () => {
  showAgent.value = true;
  currentRow.value = {} as Agent;
  dialogTitle.value = 'Create New Agent';
  isEditDialog.value = false;
  componentKey.value++;
};

const refresh = () => {
  getAgents();
};

const edit = (data: Agent) => {
  dialogTitle.value = 'Edit Agent';
  showAgent.value = true;
  currentRow.value = data;
  componentKey.value++;
};

const getAgents = async () => {
  try {
    requestIng.value = true;
    await getAgentList(dataInProps.agent.api);
    nextTick(() => {
      if (!hasConnect.value) {
        hasConnect.value = true;
        const { activity, close } = useActivitySubscription(dataInProps.agent.webSoketUrl);
        connectData.activity = activity;
        connectData.close = close;
      } else {
        closeConnect();
      }
    });
    requestIng.value = false;
  } catch (err: any) {
    requestIng.value = false;
    err?.response?.data?.message && ElMessage({ type: 'error', message: err.response.data.message });
  }
};

const handleResize = () => {
  maxHeight.value = window.innerHeight - 300;
};
const getStatusText = (value: string) => t('dataIn.statuses.' + value);

const setCurrent = (row: Agent) => {
  if (row) {
    singleTableRef.value?.setCurrentRow(row);
  }
};
function handleTaskActivities(activity: ActivitieProps) {
  agentList.value.forEach(task => {
    if (String(activity?.id) === String(task.id)) {
      if (!Array.isArray(task.activities)) {
        task.activities = reactive([]);
      }
      task.activities.unshift(activity);

      if (task.activities.length > 10) {
        task.activities.splice(10, task.activities.length - 10);
      }
    }
  });
}
</script>

<style lang="scss" scoped>
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

  :deep(.el-table td.el-table__cell) div {
    word-break: break-word;
    word-wrap: break-word;
  }
}
</style>
