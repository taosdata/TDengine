<template>
  <DataIn v-bind="props" />
</template>
<script setup lang="ts">
import DataIn from 'taos-ui/components/dataIn/index.vue';

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
  downlaodOpcPointFile,
  getOpcCsvHeader,
  addOpcPoint,
  getDatasets,
  getUaAndDaData,
  generatePIDefaultConfigFile,
  validOpcFile,
  getParser,
  getSampleDataMsgbody,
  listParserPlugins,
  getStabelParser,
  getCSVColumns,
  AddSource,
  EditSource,
  loadTaskDetail
} from '@/api/datain';
import { getAgentsData, addNewAgent, deleteAgent, editAgent } from '@/api/agent';
import { excuteStart, excuteStop, excuteDel } from '@/api/common';
import { getDBListReq, createDB } from '@/api/database';
import { getLocalTimezone } from '@/utils';
const taoxAddress = localStorage.getItem('local_endpoint') ?? '';

const { $IS_COMMUNITY, $IS_OEM, $INDUSTRY } = inject('globalCustomProperties') as GlobalCustomProperties;

const clusterId = localStorage.getItem('local_clusterID') ?? '';

function getUrl(path: string) {
  const base_api = import.meta.env.VITE_APP_BASE_URL;
  let proto = '';
  let host = '';
  let wsUri = '';
  if (base_api) {
    proto = base_api.startsWith('https') ? 'wss' : 'ws';
    host = base_api.replace(/https?:\/\//, '');
  } else {
    const { location } = window;
    proto = location.protocol.startsWith('https') ? 'wss' : 'ws';
    host = location.host;
  }
  wsUri = `${proto}://${host}/api/x${path}`;
  return wsUri;
}
type Props = InstanceType<typeof DataIn>['$props'];
const props: Props = {
  isCommunity: $IS_COMMUNITY,
  isOem: $IS_OEM,
  isIndusty: !!$INDUSTRY,
  taoxAddress,
  timeZone: getLocalTimezone(),
  downloadFileUrl: import.meta.env.VITE_APP_X_API + `/download?file_path=`,
  uploadFileUrl: import.meta.env.VITE_APP_X_API + `/upload`,
  hover: false,
  task: {
    webSoketUrl: getUrl(`/activities/tasks/${clusterId}`),
    api: {
      getTask,
      refreshTask,
      skip2Latest,
      batchStartTask,
      batchStopTask,
      batchDelTask,
      batchExportTask,
      importTask,
      start: excuteStart,
      stop: excuteStop,
      delete: excuteDel
    }
  },
  metrics: {
    webSoketUrl: getUrl('/metrics/task/'),
    api: {
      getMetrics,
      getMetricsDesc,
      getTableProgress,
      getVgroupProgress
    }
  },
  agent: {
    webSoketUrl: getUrl(`/activities/agents/${clusterId}`),
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
      fechOpcPointFileApi: downlaodOpcPointFile,
      fechOpcCsvHeaderApi: getOpcCsvHeader,
      addOpcPointApi: addOpcPoint,
      getDatasets,
      fechSets: getUaAndDaData,
      generatePIDefaultConfigFile,
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
      getStabelParser,
      getCSVColumns
    }
  }
};
</script>
<style scoped lang="scss"></style>
