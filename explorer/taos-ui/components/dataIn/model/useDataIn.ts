// import { ComputedRef } from 'vue';
export interface DataInProps {
  isCommunity?: boolean;
  isOem?: boolean;
  isCloud?: boolean;
  isIndusty: boolean;
  hover: boolean;
  task: TaskProps;
  timeZone: string;
  taoxAddress: string;
  metrics: MetricsProps;
  agent: AgentProps;
  downloadFileUrl: string;
  uploadFileUrl: string;
  dataSource: dataSourceProps;
  transform: transformProps;
  // pageTitle: string | ComputedRef<string>;
}

interface TaskProps {
  webSoketUrl: string;
  api: {
    getTask: RequestApiFn<Recordable[]>;
    refreshTask: RequestApiFn<Recordable[]>;
    skip2Latest: RequestApiFn<Recordable[]>;
    stop: RequestApiFn<Recordable[]>;
    start: RequestApiFn<Recordable[]>;
    delete: RequestApiFn<Recordable[]>;
    batchStartTask: RequestApiFn<Recordable[]>;
    batchStopTask: RequestApiFn<Recordable[]>;
    batchDelTask: RequestApiFn<Recordable[]>;
    batchExportTask: RequestApiFn<Recordable>;
    importTask: RequestApiFn<Recordable>;
  };
}

interface MetricsProps {
  webSoketUrl: string;
  api: {
    getMetrics: RequestApiFn<Recordable[]>;
    getMetricsDesc: RequestApiFn<Recordable[]>;
    getVgroupProgress: RequestApiFn<Recordable[]>;
    getTableProgress: RequestApiFn<Recordable[]>;
  };
}

interface AgentProps {
  webSoketUrl: string;
  api: {
    getAgentsData: RequestApiFn<Recordable[]>;
    addNewAgent: RequestApiFn<Recordable>;
    deleteAgent: RequestApiFn<Recordable[]>;
    editAgent: RequestApiFn<Recordable>;
  };
}

interface dataSourceProps {
  api: {
    getDatabase: RequestApiFn<Recordable[]>;
    createApi: RequestApiFn<Recordable[]>;
    connectivityCheckApi: RequestApiFn<Recordable>;
    fechCsvEmptyTemplateApi: RequestApiFn<BlobPart>;
    fechTicketApi: RequestApiFn<Recordable>;
    checkReadyFile: RequestApiFn<Recordable>;
    fechOpcPointFileApi: RequestApiFn<Recordable>;
    fechOpcCsvHeaderApi: RequestApiFn<Recordable>;
    addOpcPointApi: RequestApiFn<Recordable>;
    getDatasets: RequestApiFn<Recordable>;
    fechSets: RequestApiFn<Recordable>;
    generatePIDefaultConfigFile: RequestApiFn<Recordable>;
    validOpcFile: RequestApiFn<Recordable>;
    addSourceApi: RequestApiFn<Recordable>;
    editSourceApi: RequestApiFn<Recordable>;
    getTaskDetailApi: RequestApiFn<Recordable>;
  };
}

interface transformProps {
  api: {
    getParser: RequestApiFn<Recordable>;
    getSampleDataMsgbody: RequestApiFn<Recordable>;
    listParserPlugins: RequestApiFn<Recordable[]>;
    getStabelParser: RequestApiFn<Recordable>;
    getCSVColumns: RequestApiFn<Recordable>;
  };
}

export const dataInPropsKey = Symbol('dataInProps');

export function getDataInProps(): DataInProps {
  return inject(dataInPropsKey) as DataInProps;
}
