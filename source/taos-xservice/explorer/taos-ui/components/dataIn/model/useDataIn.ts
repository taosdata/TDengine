import { project, organization, user } from 'config';

// import { ComputedRef } from 'vue';
export interface DataInProps {
  isCommunity?: boolean;
  isOem?: boolean;
  isCloud?: boolean;
  isIndustry: boolean;
  isIdmp?: boolean;
  isTsdbLite?: boolean;
  hover: boolean;
  task: TaskProps;
  ensureXnodeThen?: (action: () => void | Promise<void>) => Promise<void>;
  /** Pre-loaded xnode availability; null = not yet fetched. */
  xnodesExist?: boolean | null;
  /** Called directly when xnodes are known to be absent (no async re-check needed). */
  missingXnodeCallback?: () => void | Promise<void>;
  timeZone: string;
  taoxAddress: string;
  metrics: MetricsProps;
  agent: AgentProps;
  downloadFileUrl: string;
  uploadFileUrl: string;
  dataSource: dataSourceProps;
  transform: transformProps;
  tasoxVersion?: string;
  // pageTitle: string | ComputedRef<string>;
}

interface TaskProps {
  webSocketUrl: string;
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
  webSocketUrl: string;
  api: {
    getMetrics: RequestApiFn<Recordable[]>;
    getMetricsDesc: RequestApiFn<Recordable[]>;
    getVgroupProgress: RequestApiFn<Recordable[]>;
    getTableProgress: RequestApiFn<Recordable[]>;
  };
}

interface AgentProps {
  webSocketUrl: string;
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
    getPointOptionsApi?: RequestApiFn<Recordable>;
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
    getStableParser: RequestApiFn<Recordable>;
    getCSVColumns: RequestApiFn<Recordable>;
  };
}

export const dataInPropsKey = Symbol('dataInProps');

export function getDataInProps(): DataInProps {
  return inject(dataInPropsKey) as DataInProps;
}

export const uploadHeaders = computed(() => {
  return project.isCloud
    ? {
        Authorization: user.token,
        'Account-Id': organization.orgId
      }
    : {};
});
