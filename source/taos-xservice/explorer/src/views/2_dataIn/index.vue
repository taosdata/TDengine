<template>
  <DataIn v-bind="props" :xnodes-exist="xnodesExist" :missing-xnode-callback="promptGoToCreateXnode" />
</template>
<script setup lang="ts">
import { hasAnyXnode, normalizeXnodeRows } from '@/components/xnode/xnodeDialog.helper';
import { createEnsureXnodeController } from '@/components/xnode/xnodeGate.helper';
import { sendSQLReq } from '@/api/explorer';
import DataIn from 'taos-ui/components/dataIn/index.vue';
import pathDetector from '@/utils/pathDetector';
import { ElMessageBox } from 'element-plus';

import {
  getTask,
  refreshTask,
  skip2Latest,
  getMetrics,
  getMetricsDesc,
  batchStartTask,
  batchExportTask,
  importTask,
  batchStopTask,
  batchDelTask,
  getTableProgress,
  getVgroupProgress,
  validateTask,
  getCsvEmptyTemplate,
  getTicket,
  checkReadyFile,
  downloadOpcPointFile,
  getOpcCsvHeader,
  addOpcPoint,
  getDatasets,
  getUaAndDaData,
  generatePIDefaultConfigFile,
  getPointOptions,
  validOpcFile,
  getParser,
  getSampleDataMsgbody,
  listParserPlugins,
  getStableParser,
  getCSVColumns,
  AddSource,
  EditSource,
  loadTaskDetail
} from '@/api/datain';
import { getAgentsData, addNewAgent, deleteAgent, editAgent } from '@/api/agent';
import { executeStart, executeStop, executeDel } from '@/api/common';
import { getDBListReq, createDB } from '@/api/database';
import { getLocalTimezone } from '@/utils';
const taoxAddress = localStorage.getItem('local_endpoint') ?? '';

const { $IS_COMMUNITY, $IS_OEM, $INDUSTRY, $IS_TSDBLITE } = inject('globalCustomProperties') as GlobalCustomProperties;
const router = useRouter();
const { t } = useI18n();

const xApiBasePath = pathDetector.getXApiBasePath();

const xnodesExist = ref<boolean | null>(null);

async function refreshXnodesExist() {
  try {
    const result = await sendSQLReq('show xnodes;', false, false);
    xnodesExist.value = hasAnyXnode(normalizeXnodeRows(result));
  } catch {
    xnodesExist.value = false;
  }
}

onMounted(refreshXnodesExist);
onActivated(refreshXnodesExist);

async function promptGoToCreateXnode() {
  try {
    await ElMessageBox.confirm(t('dataIn.xnodeRequiredMessage'), t('dataIn.xnodeRequiredTitle'), {
      confirmButtonText: t('dataIn.goCreateXnode'),
      cancelButtonText: t('cancel'),
      type: 'warning'
    });
    await router.push('/management/cluster');
  } catch {
    // Keep the user on the current page when they cancel the guidance prompt.
  }
}

const xnodeController = createEnsureXnodeController({
  hasXnode: async () => {
    const result = await sendSQLReq('show xnodes;', false, false);
    return hasAnyXnode(normalizeXnodeRows(result));
  },
  onMissingXnode: promptGoToCreateXnode
});

function getUrl(path: string) {
  const base_api = xApiBasePath;
  let proto = '';
  let host = '';
  let wsUri = '';
  const { location } = window;
  if (base_api.startsWith('http')) {
    proto = base_api.startsWith('https') ? 'wss' : 'ws';
    host = base_api.replace(/https?:\/\//, '');
  } else {
    host = `${location.host}${xApiBasePath}`;
    proto = location.protocol == 'https:' ? 'wss' : 'ws';
  }
  wsUri = `${proto}://${host}${path}`;
  return wsUri;
}

type Props = InstanceType<typeof DataIn>['$props'];
const props: Props = {
  isCommunity: $IS_COMMUNITY,
  isOem: $IS_OEM,
  isIndustry: !!$INDUSTRY,
  isTsdbLite: !!$IS_TSDBLITE,
  taoxAddress,
  ensureXnodeThen: xnodeController.ensureXnodeThen,
  timeZone: getLocalTimezone(),
  downloadFileUrl: pathDetector.getXApiBasePath() + `/download?file_path=`,
  uploadFileUrl: pathDetector.getXApiBasePath() + `/upload`,
  hover: false,
  task: {
    webSocketUrl: getUrl('/activities/tasks'),
    api: {
      getTask,
      refreshTask,
      skip2Latest,
      batchStartTask,
      batchStopTask,
      batchDelTask,
      batchExportTask,
      importTask,
      start: executeStart,
      stop: executeStop,
      delete: executeDel
    }
  },
  metrics: {
    webSocketUrl: getUrl('/metrics/task/'),
    api: {
      getMetrics,
      getMetricsDesc,
      getTableProgress,
      getVgroupProgress
    }
  },
  agent: {
    webSocketUrl: getUrl('/activities/agents'),
    api: {
      getAgentsData,
      addNewAgent,
      deleteAgent,
      editAgent
    }
  },
  dataSource: {
    api: {
      getDatabase: getDBListReq,
      createApi: createDB,
      connectivityCheckApi: validateTask,
      fechCsvEmptyTemplateApi: getCsvEmptyTemplate,
      fechTicketApi: getTicket,
      checkReadyFile,
      fechOpcPointFileApi: downloadOpcPointFile,
      fechOpcCsvHeaderApi: getOpcCsvHeader,
      addOpcPointApi: addOpcPoint,
      getDatasets,
      fechSets: getUaAndDaData,
      generatePIDefaultConfigFile,
      getPointOptionsApi: getPointOptions,
      validOpcFile,
      addSourceApi: AddSource,
      editSourceApi: EditSource,
      getTaskDetailApi: loadTaskDetail
    }
  },
  transform: {
    api: {
      getParser,
      getSampleDataMsgbody,
      listParserPlugins,
      getStableParser,
      getCSVColumns
    }
  }
};
</script>
<style scoped lang="scss"></style>
