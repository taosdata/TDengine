import { cloneDeep } from 'lodash-es';
import { FormInstance } from 'element-plus';
import { parseTime, formatDateInTimeZone } from 'utils/date';
import { getDataInProps } from './useDataIn';
import { t } from 'locales';

// 时间展示的格式化
export function getTimeParser(time?: DateType | null, pattern = 'YYYY-MM-DD HH:mm:ss') {
  const { isCloud } = getDataInProps();
  return isCloud ? parseTime(time, pattern) : formatDateInTimeZone(time);
}

// 当前的数据源页面类型
export const currentPageType = ref<'add' | 'edit' | 'copy' | 'view'>('add');
// 编辑操作时的任务id
export const taskId = ref<number>();

export const currentTaskStatus = ref<string>('');

export function getSourceConfig(isEn: boolean) {
  // 数据源按文件名排序
  const modules: Record<string, any> = {};
  let modulesFiles: Record<string, any> = {};
  const definitionsList: Record<string, any>[] = [];
  if (isEn) {
    modulesFiles = import.meta.glob<true, string, any>('../config/en/*.ts', { eager: true });
  } else {
    modulesFiles = import.meta.glob<true, string, any>('../config/zh/*.ts', { eager: true });
  }

  for (const path in modulesFiles) {
    if (!modulesFiles[path].default.id) {
      continue;
    }
    if (modulesFiles[path].default.id == 'sparkplugb') {
      // FIXME(@huolinhe): sparkplugb support starts from 3.3.7.0
      continue;
    }
    definitionsList.push({
      id: modulesFiles[path].default.id,
      name: modulesFiles[path].default.name
    });

    const namespace = path.replace(/.*-(\w+)\.\w+$/, '$1');
    if (!modules[namespace]) {
      modules[namespace] = {};
    }

    modules[namespace] = cloneDeep(modulesFiles[path].default);
  }
  return { defaultSourceConfig: modules, definitionsList };
}

export const currentDefinition: Recordable = ref([]);
export const sourceForm = reactive({
  name: '',
  type: '',
  targetDB: '',
  agent: '',
  data: {} as Recordable
});

export const agentList = ref<Recordable[]>([]);
export const agentId = ref('');
export const NoNeedAgentType = ['tmq', 'taos', 'csv'];

export async function getAgentList(api: any) {
  api.getAgentsData().then((data: any) => {
    agentList.value = data.map((item: any) => {
      // item['activities'] = [];
      item['created_at'] = item.created_at ? item.created_at.replace(/(?<=\.)\S+$/, '').replace('.', '') + 'Z' : '';
      return item;
    });
  });
}

interface checkResultProp {
  data_source?: string;
  valid?: boolean;
  support?: boolean;
  version?: string;
  message?: string;
  namespaces?: string[];
}

// 连通性检查的结果
export const connectivityCheckResult = ref<checkResultProp>({
  valid: true,
  support: true,
  data_source: '',
  namespaces: []
});

export const validOpcFileResult = ref();

export const TimeFormats = [
  'beginDateTime',
  'endDateTime',
  'start',
  'end',
  'beginTime',
  'endTime',
  'BackfillStartTime',
  'BackfillEndTime'
];

export function getFieldClassMarkName(field: string) {
  return field.replace(/[^\w-]/g, '-');
}

// 验证特定（连通性检查）字段之前的必填表单,并执行 callback
export function validateFormFields(formRef: FormInstance, onValid: AnyFunction, onInvalid?: AnyFunction) {
  const validFieldListAll = () => {
    const result: string[] = [];
    getValidFieldList(currentDefinition.value.config, result);

    return result;
  };
  const validFieldList = validFieldListAll().filter(item =>
    document.querySelector(`.source-ui .left-ui .${getFieldClassMarkName(item)}`)
  );

  formRef.validateField(validFieldList, (valid: boolean) => {
    if (valid) {
      const type = sourceForm.type;
      const agent = sourceForm.agent;
      if (type == 'kafka') {
        formRef.clearValidate();
      }
      const param = {
        ...sourceForm
        // data: {
        //   connection_options: sourceForm.data.connection_options,
        //   authentication: sourceForm.data.authentication,
        //   groups_before: sourceForm.data.groups_before
        // }
      };

      onValid(param, agent);
    } else {
      onInvalid && onInvalid();
      nextTick(() => {
        document.querySelector('.source-ui .left-ui .is-error')?.scrollIntoView();
      });
    }
  });
}

// 获取需要在点击通性检查之前的必填字段
export const getValidFieldList = (data: any, result: any[], parent = 'data') => {
  for (const val of data) {
    if (val.field == 'checkConnectivity') break;
    if (val.children) {
      getValidFieldList(val.children, result, parent + '.' + val.field);
    } else {
      if (val.host) {
        result.push(parent + '.' + val.host.field);
      }
      if (val.port) {
        result.push(parent + '.' + val.port.field);
      }
      if (val.required) {
        result.push(parent + '.' + val.field);
      }
    }
  }
};

// mongodb 查询排序字段输入框校验函数
export function checkJson(_: any, value: string, callback: AnyFunction) {
  function isValidJSON(text: string) {
    try {
      return Object.keys(JSON.parse(text)).length !== 0;
    } catch (error) {
      return false;
    }
  }

  if (value && !isValidJSON(value)) {
    return callback(new Error(t('dataIn.jsonTip')));
  }
  return callback();
}

export function generateFormInitData(paramsConfig: Recordable[]) {
  return paramsConfig.reduce((data, item) => {
    const value = item.type == 'number' ? Number(item.defaultValue) : (item.defaultValue ?? '');

    if (item.children && item.children.length) {
      data[item.field] = generateFormInitData(item.children);
      if (item.valueField) {
        data[item.field][item.valueField] = value;
      }
      if (item.type === 'grouping') {
        item.children.forEach((child: any) => {
          data[item.field][child.host.field] = child.host.defaultValue ?? '';
          data[item.field][child.port.field] = child.port.defaultValue ?? '';
        });
      }
    } else {
      data[item.field] = value;
      if (item.type === 'compose') {
        data[item.field + '_unit'] = item.unit_value || '';
        data[item.field] = '';
      }
      // if (item.type === 'composeAppend') {
      //   data[item.field + '_unit'] = item.unit_value || '';
      //   data[item.field + '_value'] = Number(value);
      //   data[item.field] = '';
      // }
    }
    return data;
  }, {});
}

// 是否展示查看点位列表table
export const isShowDatasetTable = ref<boolean>(false);
// 是否展示transform列表table
export const isShowResultTable = ref<boolean>(false);
// 获取数据点位
export const datasetTableData = ref();

export function recoverWriteConfig(writeConfig: any, parserGlobalData: any) {
  if (!writeConfig || !parserGlobalData) {
    return;
  }

  const keys = Object.getOwnPropertyNames(parserGlobalData);
  keys.forEach(key => {
    if (key === 'variable_not_exist_in_table_name_template' || key === 'table_name_contains_illegal_char') {
      if (parserGlobalData[key]['replace_to'] !== undefined) {
        writeConfig[`${key}_unit`] = 'replace_to';
        writeConfig[key] = parserGlobalData[key]['replace_to'];
      } else {
        writeConfig[`${key}_unit`] = parserGlobalData[key];
      }
    } else if (typeof parserGlobalData[key] === 'object') {
      for (const subKey in parserGlobalData[key]) {
        if (writeConfig[`${key}.${subKey}`] !== undefined) {
          writeConfig[`${key}.${subKey}`] = parserGlobalData[key][subKey];
        }
      }
    } else if (writeConfig[key] !== undefined) {
      writeConfig[key] = parserGlobalData[key];
    }
  });
}

export function recoverFromData(dstype: string, dataDisplay: Recordable, rdata: Recordable, parentKey?: string) {
  // console.log('recoverFromData', dataDisplay);
  // console.log('data', rdata);
  const keys = Object.getOwnPropertyNames(dataDisplay);

  keys.forEach(key => {
    if (typeof dataDisplay[key] === 'object') {
      // 对象或数组类型
      recoverFromData(dstype, dataDisplay[key], rdata, key);
    } else if (rdata[key] !== undefined) {
      // 有值的情况下，需要恢复
      if (dstype === 'opentsdb' && key === 'metrics') {
        if (rdata[key].length > 0) {
          dataDisplay[key] = rdata[key].split(',');
        } else {
          dataDisplay[key] = [];
        }
      } else if (key === 'port' && typeof rdata[key] === 'number') {
        dataDisplay[key] = String(rdata[key]);
      } else if (typeof dataDisplay[key] === 'boolean') {
        if (rdata[key] === 'true') {
          dataDisplay[key] = true;
        } else if (rdata[key] === 'false') {
          dataDisplay[key] = false;
        } else {
          dataDisplay[key] = rdata[key];
        }
      } else {
        dataDisplay[key] = rdata[key];
      }
    } else if (rdata[`${parentKey}.${key}`] !== undefined) {
      // 有值的情况下，需要恢复
      dataDisplay[key] = rdata[`${parentKey}.${key}`];
    }
  });
}

export function formatFromData(from: Recordable) {
  const { agent, type, data } = from;
  const resultFrom = {
    agent,
    type,
    data: {}
  };
  mergeToFromData(data, resultFrom.data);
  return resultFrom;
}

// 将配置数据合入为 from.data 参数，也就是3.3.6.0版本之前的 dsn 字符串的 对象表达形式
function mergeToFromData(data: Recordable, fromData: Recordable, fullNameMap: any = {}, parentKey?: string) {
  if (!data || typeof data !== 'object') {
    return;
  }

  if (data['only-choose-one$'] && typeof data[data['only-choose-one$']] === 'object') {
    mergeToFromData(data[data['only-choose-one$']], fromData, fullNameMap, data['only-choose-one$']);
    return;
  }

  const keys = Object.getOwnPropertyNames(data);

  keys.forEach(key => {
    if (!parentKey && (key === 'parser' || key === 'write_config' || !data[key])) {
      // 不需要根节点的 parser 数据，如果根节点没有配置，则也不需要
      return;
    }

    if (typeof data[key] === 'object') {
      // 对象或数组类型
      if (data[key]?.length !== undefined) {
        fromData[key] = data[key].join(',');
      } else {
        mergeToFromData(data[key], fromData, fullNameMap, key);
      }
    } else {
      // 普通类型
      if (fromData[key] !== undefined) {
        const fullName = fullNameMap[key];
        fromData[fullName] = fromData[key];
        delete fromData[key];
        fromData[`${parentKey}.${key}`] = data[key];
      } else {
        fromData[key] = data[key];
        fullNameMap[key] = `${parentKey}.${key}`;
      }
    }
  });
}

// 获取嵌套的值
export const getNestedValue = (obj: Recordable, str: string) => {
  const path = str.split('/');
  return path.reduce((acc, key) => {
    return acc && acc[key] !== undefined ? acc[key] : null;
  }, obj);
};

export function getDataRange(datatype: string) {
  switch (datatype) {
    case 'TINYINT':
      return [-128, 127, 4];
    case 'TINYINT UNSIGNED':
      return [0, 255, 3];
    case 'SMALLINT':
      return [-32768, 32767, 6];
    case 'SMALLINT UNSIGNED':
      return [0, 65535, 5];
    case 'INT':
      return [-2147483648, 2147483647, 11];
    case 'INT UNSIGNED':
      return [0, 4294967295, 10];
    case 'BIGINT':
      return [-9223372036854775808n, 9223372036854775807n, 20];
    case 'BIGINT UNSIGNED':
      return [0, 18446744073709551615n, 20];
    case 'FLOAT':
      return [-3.4e38, 3.4e38, 38];
    case 'DOUBLE':
      return [-1.7e308, 1.7e308, 308];
  }
  return null;
}

export function getWriteConfigData(data: Recordable) {
  const writeConfigData = cloneDeep(data[writeConfigField]);

  const global: Recordable = {
    cache: {},
    archive: {}
  };

  const valueMap = ['variable_not_exist_in_table_name_template_unit', 'table_name_contains_illegal_char_unit'];

  for (const [key, value] of Object.entries(writeConfigData)) {
    const cacheKey = key.replace(/^cache\./, '');
    const archiveKey = key.replace(/^archive\./, '');

    if (key.startsWith('cache')) {
      global.cache[cacheKey] = value;
    } else if (key.startsWith('archive')) {
      global.archive[archiveKey] = value;
    } else if (valueMap.includes(key)) {
      const noTypeKey = key.replace(/_unit$/, '');
      global[noTypeKey] = value === 'replace_to' ? { replace_to: writeConfigData[noTypeKey] } : value;
    } else {
      global[key] = value;
    }
  }

  // 搬迁属性，将高级选项中的写入并发数迁移至写入配置的全局配置
  const advanceOptions = data['advanced_options'];
  if (advanceOptions && advanceOptions['written_concurrent']) {
    global['written_concurrent'] = advanceOptions['written_concurrent'];
  }

  return global;
}

export function getCSVOptions(data: Recordable) {
  const queryArr: string[] = [];
  getGroupsQuery(data[groupsFieldBeforeConnection], queryArr);
  getGroupsQuery(data[groupsFieldAfterConnection], queryArr);
  return queryArr;
}

// 以下函数 for test
import { hasOwnProperty } from 'utils/validate';

const ReplacePoint = '~';
export const ComposeParams = [
  'timeout',
  'schema-polling-interval',
  'unit',
  'retro',
  'excursion',
  'interval',
  'MaxBackfillRangeDays',
  'timeWindow',
  'retrieveInterval',
  'tolerance',
  'delay',
  'local_threshold'
];

const valueField = 'dea7d812-3c76-40a5-bb8a-1048945f79cb';
export const optionsField = 'connection_options';
const groupsFieldBeforeConnection = 'groups_before';
const groupsFieldAfterConnection = 'groups_after';
export const datasetsField = 'datasets';
const writeConfigField = 'write_config';

export const ProtocolPrefix = NoNeedAgentType.concat(['influxdb', 'opentsdb']);

function getOriginField(field: string) {
  return field.replace(new RegExp(ReplacePoint, 'g'), '.');
}

function checkValue(value: string | any[] | null | undefined) {
  if (value === undefined || value === null || value === '' || value === 'undefined') return false;
  if (Array.isArray(value)) {
    if (!value.length) return false;
  }
  return true;
}

function getGroupsQuery(groups: any, query: any[]) {
  groups = cloneDeep(groups);
  if (!groups) return query;
  for (const key in groups) {
    if (typeof groups[key] == 'object') {
      if (hasOwnProperty(groups[key], valueField) && !groups[key][valueField]) continue;
      for (const k in groups[key]) {
        if (!checkValue(groups[key][k])) continue;
        if (k == valueField) {
          continue;
        } else {
          const field = getOriginField(k);
          if (ComposeParams.includes(k)) {
            const unit_value = checkValue(getQueryParamValue(groups[key][k + '_unit']))
              ? getQueryParamValue(groups[key][k + '_unit'])
              : '';
            query.push(field + '=' + getQueryParamValue(groups[key][k]) + unit_value);
          } else if (/_unit$/.test(k)) {
            delete groups[key][k + '_unit'];
          } else {
            query.push(field + '=' + getQueryParamValue(groups[key][k]));
          }
        }
      }
    }
  }
}

function getQueryParamValue(value: any) {
  let result = value;
  try {
    if (value && typeof value == 'object') {
      if (value instanceof Array) {
        result = value.toString();
      } else {
        result = JSON.stringify(value);
      }
    } else {
      result = encodeURIComponentECO(value);
    }
  } catch (error) {
    console.log(error, 'value:' + value);
  }
  return result;
}

function encodeURIComponentECO(str: string) {
  return encodeURIComponent(str).replace(/[.!'()*]/g, function (c) {
    return '%' + c.charCodeAt(0).toString(16);
  });
}

export function getAdvancedHealth(advanced: Recordable) {
  if (!advanced || advanced['health_check_window_in_second_value'] === undefined) return null;
  const health = {
    health_check_window_in_second: advanced['health_check_window_in_second_value'],
    busy_threshold: advanced['busy_threshold_value'] / 100,
    max_queue_length: advanced['max_queue_length'],
    max_errors_in_window: advanced['max_errors_in_window']
  };
  return health;
}

export const dataInMockData = [
  {
    taskid: 1,
    id: 1,
    name: 'td3-demo',
    localname: 'td3',
    localtype: 'TDengine Data Subscription',
    target: 'targetDatabse',
    created_at: '2024-03-27T10:34:15.994Z',
    finished_at: '2024-03-27T21:20:51.681Z',
    status: 'completed',
    completed: true,
    taskActivities: [
      {
        level: 'info',
        at: '2024-03-27T21:20:51.681Z',
        activity: '',
        context: ''
      }
    ]
  }
];
export const agentMockData = [
  {
    id: 1,
    name: 'test',
    status: 'created',
    created_at: '2024-03-27T21:20:51.681Z',
    agentActivities: [
      {
        level: 'info',
        at: '2024-03-27T21:20:51.681Z',
        activity: '',
        context: ''
      }
    ]
  }
];
