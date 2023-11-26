import { uuid, hasOwn } from '@/utils/util';
import { cloneDeep } from 'lodash';
import { isObject, isArray } from '@/utils/validate';
import { StaticTemplatePath, IsAliyun } from '@/const';
// import { downloadFile } from '@/api/dataSource';
// import { downloadFileBlob } from '@/utils/file';
import { Loading } from 'element-ui';
import { parsinginZone } from "@/utils/index";
import i18n from '@/lang';

const lang = IsAliyun ? 'zh' : 'en';
const templateUrlMap = {
  // opcua: StaticTemplatePath + `opc-${lang}.csv`,
  // opcda: StaticTemplatePath + `opc-${lang}.csv`,
  // point_file: StaticTemplatePath + `pi-points.csv`,
  // template_for_pi_point_file: StaticTemplatePath + 'pi-points.csv',
  // template_for_af_element_file: StaticTemplatePath + 'elementTemplate.csv'
};
const ReplacePoint = '~';
const InfoParams = ['security_policy', 'security_mode'];
export const TimeFormats = ['beginDateTime', 'endDateTime'];
export const PayConnectorList = ['pi', 'opcua', 'opcda', 'pibackfill'];
// // 无法使用symbol作为key，因为会被for in 和 object.keys过滤掉
const valueField = uuid();
const optionsField = uuid();
const groupsField = uuid();
const piOptionShowValue = 'PI Data Archive and Asset Framework (AF) Server';
const authenticationField = uuid();
const connectivityCheckField = uuid();
const datasetsField = uuid();
let currentType = '';
const DefaultParserValue = {
  parse: {
    payload: {
      json: [],
      keep: true
    }
  },
  model: {
    name: '',
    using: '',
    columns: ['ts'],
    tags: []
  }
};
export const DefaultOpcTableValue = {
  stable_prefix: '',
  column_configs: [
    {
      column_name: 'received_time',
      column_type: 'timestamp',
      column_alias: 'received_time',
      is_primary_key: true
    },
    {
      column_name: 'original_time',
      column_type: 'timestamp',
      column_alias: 'original_time',
      is_primary_key: false
    },
    {
      column_name: 'value',
      column_alias: 'value',
      is_primary_key: false
    },
    {
      column_name: 'quality',
      column_type: 'int',
      column_alias: 'quality',
      is_primary_key: false
    }
  ]
};

// 根据返回的数据源参数定义生成对应的表单配置
export function getFormConfigByDataSource(dataSource, parserValue) {
  return dataSource.reduce((formConfig, item) => {
    const { id, name, type, strict, description, protocol, authentication, groups, options, datasets, parser, params } = item;
    const paramsConfig = [
      {
        label: i18n.t('dataIn.connectionConfiguration'),
        field: optionsField,
        children: []
      }
    ];
    const config = {
      name,
      id,
      type,
      description,
      strict,
      config: paramsConfig
    };
    currentType = id;
    let connectivityCheck = id != 'taos'
    // handleParams(params, paramsConfig);
    // handleProtocol(protocol, paramsConfig);
    handleOptions(options, paramsConfig);
    handleAuthentication(authentication, paramsConfig);
    handleConnectivityCheck(connectivityCheck,paramsConfig)
    // handleDatasets(datasets, paramsConfig);
    handleGroups(groups, paramsConfig);
    handleParser(parser, paramsConfig, parserValue);
    // 先处理protocol
    formConfig[id] = config;
    return formConfig;
  }, {});
}

function handleConnectivityCheck(connectivityCheck, paramsConfig) {
  if (!connectivityCheck) return;
  const children = [];
  paramsConfig.push({
    label: undefined,
    description: undefined,
    field: connectivityCheckField,
    type: 'collapse',
    valueField,
    defaultValue: undefined,
    multiple: false,
    children
  });
}

// 处理protocol
/**
 * {
    "display": "Protocol",
    "description": "Choose a protocol scheme for websocket connection, leave it empty for native connection",
    "choices": [
        {
            "name": "--",
            "display": "Native",
            "description": "Use libtaos client library for connection"
        },
        {
            "name": "ws",
            "display": "WS",
            "description": "Use WebSocket with HTTP connection."
        },
        {
            "name": "wss",
            "display": "WSS",
            "description": "Use WebSocket with HTTPS connection."
        }
    ]
}
 */
// function handleProtocol(protocol, paramsConfig) {
//   if (!protocol) return;
//   const { display, description, choices, value = '' } = protocol;
//   paramsConfig[0].children.push({
//     label: display,
//     description,
//     field: 'protocol',
//     type: 'select',
//     display_order: 0,
//     defaultValue: value,
//     if: currentData => {
//       if (!currentData.system_configuration) return true;
//       return currentData.system_configuration == piOptionShowValue;
//     },
//     required: true, // just required for cloud
//     options: choices.map(item => ({
//       label: item.display,
//       value: item.name,
//       description: item.description
//     }))
//   });
// }
// 处理authentication
/**
 * {
    "display": "Authentication",
    "description": "Use username/password plain authentication or with **token**.",
    "value": "plain",
    "alternatives": [
        {
            "name": "plain",
            "display": "Plain",
            "description": "Use username and password.",
            "username": {
                "display": "Username",
                "description": "TDengine username. The default is root.",
                "placeholder": "root"
            },
            "password": {
                "display": "Password",
                "description": "TDengine password. The default is taosdata.",
                "placeholder": "taosdata"
            }
        },
        {
            "name": "token",
            "display": "Token",
            "description": "Use token in parameters.",
            "params": [
                {
                    "name": "token",
                    "display": "Token",
                    "description": "Cloud token or custom token."
                }
            ]
        }
    ]
}
 */
function handleAuthentication(authentication, paramsConfig) {
  if (!authentication) return;
  const { display, description, value, alternatives } = authentication;
  if (!alternatives || !alternatives.length) return;
  const children = [];
  paramsConfig.push({
    label: display,
    description,
    field: authenticationField,
    type: 'tabs',
    valueField,
    defaultValue: value,
    multiple: false,
    children
  });
  alternatives.forEach(item => {
    const paramsChildren = [];
    const tabConfig = {
      label: item.display,
      name: item.name,
      field: handleField(item.name),
      children: paramsChildren
    };
    if (item.params) {
      item.params.forEach(param => {
        const { display, description, name, value: defaultValue, required = false } = param;
        const config = {
          label: display,
          description,
          required,
          field: name,
          defaultValue: defaultValue ?? ''
        };
        handleHintType(config, param.hint);
        handleInfoParams(config);
        paramsChildren.push(config);
      });
    } else {
      const keys = Object.keys(item).filter(predicate => isObject(item[predicate]));
      keys.forEach(key => {
        const { display, description, value: defaultValue, required } = item[key];
        const config = {
          label: display,
          description,
          required,
          field: key,
          defaultValue: defaultValue ?? ''
        };
        handleHintType(config, item[key]);
        handleInfoParams(config);
        paramsChildren.push(config);
      });
    }
    children.push(tabConfig);
  });
}

// 根据字段判断表单项类型
// export function handleFormItemType(config, data) {
//   if (data.choices) {
//     config.type = 'select';
//     config.options = data.choices.map(item =>
//       typeof item === 'string'
//         ? {
//             label: item,
//             value: item
//           }
//         : {
//             label: item.display,
//             value: item.name,
//             description: item.description
//           }
//     );
//   } else {
//     config.type = 'input';
//   }
// }

// 处理options
/**
 * {
    "host": {
        "display": "Host",
        "description": "TDengine fqdn. Leave it empty if use server localhost(relative to taosX server).",
        "placeholder": "server"
    },
    "port": {
        "display": "Port",
        "description": "TDengine connection port, leave it empty if use default port.",
        "placeholder": "auto"
    },
    "subject": {
        "required": true,
        "display": "Topics",
        "description": "Database name, database.table name or topic name is all available.",
        "placeholder": "Example: db1,db1.stb1,topic1"
    }
}
 */
function handleOptions(options, paramsConfig) {
  if (!options) return;
  const children = paramsConfig[0]?.children ?? [];
  const keys = Object.keys(options);
  keys.forEach(key => {
    const { display, description, placeholder, required, value } = options[key];
    if (!display) return;
    const config = {
      label: display,
      description,
      field: key,
      placeholder,
      required,
      defaultValue: value ?? '',
      if: currentData => {
        if (!currentData.system_configuration || key == 'host') return true;
        return currentData.system_configuration == piOptionShowValue;
      }
    };
    if (key == 'host') {
      config.display_order = 1;
    }
    handleHintType(config, options[key]);
    children.push(config);
  });
  formSort(children);
}

// 处理parser
/**
 * {
    "display": "MQTT Payload Parser",
    "required": true,
    "description": "MQTT will report exactly four fields of data for each data stream:<br>\n\n- **ts**: the collect timestamp.<br>\n- **topic**: the topic name to subscribe.<br>\n- **qos**: the QoS of the message, usually 0, 1, 2.<br>\n- **payload**: the data payload of the message.<br>\n\ntaosX could parse the payload with JSON extractor and let users to specify the<br>\ndata model in the database, for example, the table name pattern and stable name<br>\npattern, field names as tags or field names as columns.\n",
    "fields": [
        {
            "name": "ts",
            "description": "Timestamp."
        },
        {
            "name": "topic",
            "description": "Topic name."
        },
        {
            "name": "qos",
            "description": "QoS, one of 0/1/2."
        },
        {
            "name": "payload",
            "description": "Payload"
        }
    ]
}
 */

function handleParser(parser, paramsConfig, value = cloneDeep(DefaultParserValue)) {
  if (!parser) return;
  const { display, description, fields } = parser;
  paramsConfig.push({
    label: display,
    description,
    field: 'parser',
    type: 'parser',
    fields: fields.filter(item => item.name != 'payload'),
    defaultValue: value,
    children: []
  });
}

/**
 * parser data
 * {
  "model": {
    "columns": [
      "id",
      "current",
      "voltage",
      "phase",
      "ts"
    ],
    "name": "d{id}",
    "tags": [
      "groupid",
      "location"
    ],
    "using": "xwang_meters"
  },
  "parse": {
    "payload": {
      "json": [
        {
          "alias": "id",
          "cast": "int",
          "name": "id"
        },
        {
          "alias": "voltage",
          "cast": "int",
          "name": "voltage"
        },
        {
          "alias": "phase",
          "cast": "float",
          "name": "phase"
        },
        {
          "alias": "groupid",
          "cast": "int",
          "name": "groupid"
        },
        {
          "alias": "location",
          "cast": "varchar",
          "name": "location"
        },
        {
          "alias": "current",
          "cast": "float",
          "name": "current"
        }
      ],
      "keep": true
    }
  }
}
 */

// 处理datasets 生成标签页
/**
{
    "name": "Data Sets",
    "description": "PI System template and points",
    "categories": [
        {
            "category": "PointList",
            "display": "Point List",
            "description": "# Title\nThe point names exists in pi system.\n",
            "target": {
                "name": "PointList",
                "description": "Pi System Point List",
                "required": false,
                "multiple": true,
                "editable": true,
                "selectable": true
            }
        },
        {
            "category": "TemplateForPIPoint",
            "display": "Template For PI Point",
            "description": "# Title\nThe template names exists in pi system.\n",
            "target": {
                "name": "TemplateForPIPoint",
                "description": "Database datasets.",
                "required": false,
                "multiple": true,
                "editable": true,
                "selectable": true
            }
        },
        {
            "category": "TemplateForAFElement",
            "display": "Template For AF Element",
            "description": "# Title\nThe template names exists in pi system.\n",
            "target": {
                "name": "TemplateForAFElement",
                "description": "Database datasets.",
                "required": false,
                "multiple": true,
                "editable": true,
                "selectable": true
            }
        }
    ]
}
 **/
// function handleDatasets(datasets, paramsConfig) {
//   if (!datasets) return;
//   const { name, description, categories, params, value: tabValue } = datasets;
//   const datasetsConfig = {
//     label: name,
//     description,
//     field: datasetsField,
//     type: 'tabs',
//     multiple: currentType == 'pi',
//     name: 'datasets',
//     children: params
//       ? params?.map((item, index) => {
//           const { display, description, name, value, required = false } = item;
//           const config = {
//             label: display,
//             labelShow: false,
//             labelWidth: '0px',
//             description,
//             required,
//             field: name,
//             type: 'dataset',
//             defaultValue: value ?? '',
//             disabled: (_, originalData) => {
//               const optionData = originalData[optionsField];
//               if (!optionData.system_configuration) return false;
//               return optionData.system_configuration != piOptionShowValue && !!index;
//             }
//           };
//           if (templateUrlMap[name]) {
//             config.templateUrl = templateUrlMap[name];
//           }
//           handleInfoParams(config);
//           return config;
//         })
//       : categories.map((item, index) => {
//           const { category, display, description: desc, target, params: categoryParams } = item;
//           return {
//             label: display,
//             name: category,
//             labelShow: false,
//             labelWidth: '0px',
//             category,
//             radio: !!index,
//             description: desc,
//             field: handleField(target.name),
//             type: 'dataset',
//             templateUrl: templateUrlMap[currentType],
//             placeholder: target.description,
//             required: target.required,
//             multiple: target.multiple,
//             editable: target.editable,
//             selectable: target.selectable,
//             children: categoryParams
//               ? categoryParams.map(categoryParam => {
//                   const config = {
//                     ...categoryParam,
//                     label: categoryParam.display,
//                     field: categoryParam.name,
//                     defaultValue: categoryParam.value ?? ''
//                   };
//                   handleHintType(config, categoryParam.hint);
//                   return config;
//                 })
//               : undefined,
//             defaultValue: isArray(target?.value) ? target.value.join(',') : '',
//             disabled: (currentData, topData, currentDefinition) => {
//               if (currentDefinition?.id?.startsWith('opc')) {
//                 return !topData?.[optionsField]?.endpoint;
//               } else {
//                 return false;
//               }
//             }
//           };
//         })
//   };
//   if (categories) {
//     datasetsConfig.valueField = valueField;
//     datasetsConfig.defaultValue = tabValue ?? categories[0].category;
//   }
//   paramsConfig.push(datasetsConfig);
// }

// 处理groups
/**
 * [
    {
        "name": "Subscribe Options",
        "display_order": 2,
        "description": "Options for TMQ subscription.",
        "params": [
            {
                "name": "auto.offset.reset",
                "display": "Start From",
                "description": "Data offset to start subscribing.\n\n- *earliest*: Subscribe from begin.\n- *latest*: Subscribe from latest data.",
                "hint": {
                    "type": "str",
                    "choices": [
                        "earliest",
                        "latest"
                    ]
                }
            },
            {
                "name": "timeout",
                "display": "Timeout",
                "hint": "timeout",
                "description": "A timeout for polling data from the topic.\n\nThe input value should be one of:\n\n- `never`: means waiting for valid message without timeout.\n- A duration string like `5s`, `1m` etc.\n",
                "placeholder": "5s"
            }
        ]
    }
]
 */
function handleGroups(groups, paramsConfig) {
  if (!groups) return;
  groups = groups.sort((a, b) => a.display_order - b.display_order);
  const children = [];
  paramsConfig.push({
    label: 'Groups',
    field: groupsField,
    hide: true,
    children
  });
  groups.forEach(group => {
    const { name, description: d1, params, collapsible = false, collapsed = true, short_description: d2 } = group;
    const paramChildren = [];
    const config = { label: name, field: uuid(), description: d2 ?? d1, children: paramChildren };
    if (collapsible) {
      config.type = 'switch';
      config.defaultValue = collapsed;
      config.valueField = valueField;
      config.hasValue = true;
    }
    children.push(config);
    params.forEach(param => {
      const { display, description, short_description, name, hint, placeholder = '', required = false, value, conflicts_with } = param;
      const paramConfig = {
        label: display,
        description: short_description ?? description,
        field: handleField(name),
        if: collapsible ? data => data[valueField] : true,
        placeholder,
        defaultValue: value,
        required
      };
      handleHintType(paramConfig, hint, value);
      if (isArray(conflicts_with)) {
        paramConfig.disabled = currentData => {
          const conflict = conflicts_with.find(item => currentData?.[item.name] == item.value);
          if (conflict && conflict?.when == currentData[paramConfig.field]) {
            currentData[paramConfig.field] = '';
          }
          return !!conflict;
        };
      }
      // 特殊处理 influxdb 的 bucket
      if (currentType == 'influxdb' && paramConfig.field == 'bucket') {
        paramConfig.type = 'bucket';
      }
      // 特殊处理 historian 的 mode
      if (currentType == 'historian' && paramConfig.field == 'mode') {
        paramConfig.type = 'mode';
      }
      if (paramConfig.type == 'select') {
        paramConfig.meta = {
          allowCreate: true,
          filterable: true
        };
      }
      // TODO: 临时解决
      if (paramConfig.type == 'file') {
        paramConfig.templateUrl = templateUrlMap[currentType] ?? '';
      }
      // 针对opc的opc_table_config特殊处理
      if (name == 'opc_table_config') {
        paramConfig.type = 'opcTable';
        config.name = 'opc_table_configs';
        paramConfig.label = '';
        if (value) {
          paramConfig.defaultValue = JSON.parse(value);
        } else {
          paramConfig.defaultValue = cloneDeep(DefaultOpcTableValue);
        }
      }
      if (paramConfig.type == 'time') {
        paramConfig.valueFormat = 'yyyy-MM-dd HH:mm:ss';
        paramConfig.dateType = 'datetime';
      }
      // add the info
      handleInfoParams(paramConfig);
      paramChildren.push(paramConfig);
    });
  });
}

/**
 * [
    {
        "name": "system_configuration",
        "display": "PI 系统配置",
        "display_order": 0,
        "hint": {
            "type": "str",
            "choices": [
                "PI Data Archive and Asset Framework (AF) Server",
                "PI Data Archive Only"
            ]
        },
        "value": "PI Data Archive and Asset Framework (AF) Server"
    },
    {
        "name": "PISystemName",
        "display": "AF Server 名称",
        "display_order": 3,
        "hint": {
            "type": "str"
        },
        "description": "PI 系统(AF Server) 名称 (hostname).",
        "required": true,
        "placeholder": "pi-af-server-name"
    }
]
 */

// function handleParams(params, paramsConfig) {
//   if (!params) return;
//   params = params.sort((a, b) => a.display_order - b.display_order);
//   const children = paramsConfig[0].children;
//   params.forEach((param, index) => {
//     const { name, display, description, required, placeholder, value, hint } = param;
//     const config = { label: display, field: handleField(name), description, required, placeholder, defaultValue: value, display_order: index };
//     if (name != 'system_configuration') {
//       config.if = currentData => {
//         if (!currentData.system_configuration) return true;
//         return currentData.system_configuration == piOptionShowValue;
//       };
//     }
//     handleHintType(config, hint);
//     children.push(config);
//   });
// }

//根据hint判断表单项类型
export function handleHintType(config, hint) {
  let type = hint;
  if (hint?.type) {
    type = hint.type;
  }
  if (isArray(hint)) {
    type = 'pibackfillTime';
  }
  switch (type) {
    case 'integer':
      config.type = 'number';
      config.min = hint?.min ?? -Infinity;
      config.max = hint?.max ?? Infinity;
      break;
    case 'bool':
      config.type = 'switch';
      config.defaultValue = config.defaultValue == 'true';
      break;
    case 'time':
      config.type = 'time';
      break;
    case 'timeout':
      config.type = 'input';
      break;
    case 'file':
      config.type = 'file';
      break;
    case 'pibackfillTime':
      config.type = 'pibackfillTime';
      config.options = hint;
      if (!config.defaultValue) {
        config.defaultValue = hint.find(item => item.selected)?.value;
      }
      break;
    default:
      if (hint?.choices) {
        config.type = 'select';
        config.options = hint.choices.map(item => ({
          label: item,
          value: item
        }));
      } else if (config.field == 'password') {
        config.type = 'password';
      } else {
        config.type = 'input';
      }
      break;
  }
}

function handleInfoParams(config) {
  if (config?.field && InfoParams.includes(config.field)) {
    config.info = true;
  }
}
// 生成表单初始化数据
/**
 * 1. {
 *   无存储的原始数据无法进行编辑修改
 *   data:存储提交时生成的数据结构,
 *   config?:表单配置（可选）
 *   targetDB:目标数据库
 *   dns:根据数据生成的dns
 * }
 * 2. 生成的数据完全扁平化，不需要对应的层级结构，问题：字段冲突覆盖
 */
export function generateFormInitData(paramsConfig) {
  return paramsConfig.reduce((data, item) => {
    const value = item.type == 'number' ? item.defaultValue : item.defaultValue ?? '';

    if (item.children && item.children.length) {
      data[item.field] = generateFormInitData(item.children);
      if (item.valueField) {
        data[item.field][item.valueField] = value;
      }
    } else {
      data[item.field] = value;
    }
    return data;
  }, {});
}
export const NoNeedAgentType = ['tmq', 'taos'];
// tmq和taos需要再协议前面加上+
export const ProtocolPrefix = NoNeedAgentType.concat(['influxdb']);

export function getDsnData(data, definition) {
  let dsn = handleProtocolData(data[optionsField].protocol, definition);
  let queryArr = [];
  dsn += getAuthentications(data[authenticationField], queryArr);
  dsn += getOptionData(data[optionsField], queryArr, definition);
  getGroupsQuery(data[groupsField], queryArr);
  getDatasetsQuery(data[datasetsField], data, queryArr);
  if (queryArr.length) {
    if (dsn.includes('?')) {
      if (!dsn.endsWith('?')) {
        dsn += '&';
      }
    } else {
      dsn += '?';
    }
    dsn += queryArr.join('&');
  }
  return dsn;
}
function handleProtocolData(protocol, definition) {
  const { id } = definition;
  if (id === 'tmq') {
    return '';
  }
  let dsn = '';
  if (protocol && protocol != '--') {
    if (ProtocolPrefix.includes(id?.toLowerCase())) {
      dsn += '+';
    }
    dsn += protocol;
  }
  return dsn + '://';
}

// export function getDataSetDsn(data, definition) {
//   let dsn = handleProtocolData(data[optionsField].protocol, definition);
//   let queryArr = [];
//   dsn += getAuthentications(data[authenticationField], queryArr);
//   dsn += getOptionData(data[optionsField], queryArr, definition);
//   if (queryArr.length) {
//     dsn += '?' + queryArr.join('&');
//   }
//   return dsn;
// }

function getGroupsQuery(groups, query) {
  if (!groups) return query;
  for (let key in groups) {
    if (typeof groups[key] == 'object') {
      if (hasOwn(groups[key], valueField) && !groups[key][valueField]) continue;
      for (let k in groups[key]) {
        if (!checkValue(groups[key][k])) continue;
        if (k == valueField) {
          continue;
        } else {
          const field = getOriginField(k);
          if (TimeFormats.includes(k)) {
            let value = parsinginZone(groups[key][k])
            groups[key][k] = value
          }
          query.push(field + '=' + getQueryParamValue(groups[key][k]));
        }
      }
    }
  }
}

function getDatasetsQuery(datasets, allData, query) {
  if (!datasets) return query;
  const onlyPoint = allData[optionsField].system_configuration != piOptionShowValue;
  const tabValue = datasets[valueField];
  if (tabValue) {
    if (typeof datasets[tabValue] == 'object') {
      for (let k in datasets[tabValue]) {
        if (!checkValue(datasets[tabValue][k])) continue;
        const field = getOriginField(k);
        query.push(field + '=' + getQueryParamValue(datasets[tabValue][k]));
      }
    } else {
      query.push(tabValue + '=' + getQueryParamValue(datasets[tabValue]));
    }
  } else {
    for (let k in datasets) {
      if (onlyPoint && k != 'point_file') continue;
      if (!checkValue(datasets[k])) continue;
      const field = getOriginField(k);
      query.push(field + '=' + getQueryParamValue(datasets[k]));
    }
  }
}

// 获取authentications
export function getAuthentications(authentication, params) {
  if (!authentication) return '';
  const currentData = authentication[handleField(authentication[valueField])];
  const dataFields = Object.keys(currentData);
  switch (authentication[valueField]) {
    case 'plain':
      if (!currentData.username) {
        return '';
      }
      return getQueryParamValue(currentData.username) + ':' + getQueryParamValue(currentData.password) + '@';
    default:
      params.push(...dataFields.map(item => getOriginField(item) + '=' + getQueryParamValue(currentData[item])));
      break;
  }
  return '';
}

function getOptionData(data, queryArr, definition) {
  if (!data || !definition) return '';
  let result = '';
  let { subject, host, port, endpoint, system_configuration, PISystemName } = data;
  if (PISystemName) {
    queryArr.push('PISystemName=' + PISystemName);
  }
  if (endpoint === undefined) {
    result += host.replace(/\w*:\/\//, '');
    if (system_configuration && system_configuration != piOptionShowValue) return result;
    if (port) {
      result += ':' + port;
    }
    if (subject) {
      result += '/' + subject;
    }
  } else {
    result += endpoint;
  }
  return result;
}

function handleField(field) {
  return field.replace(/\./g, ReplacePoint);
}
function getOriginField(field) {
  return field.replace(new RegExp(ReplacePoint, 'g'), '.');
}

function checkValue(value) {
  if (value === undefined || value === null || value === '') return false;
  if (Array.isArray(value)) {
    if (!value.length) return false;
  }
  return true;
}

function getQueryParamValue(value) {
  let result = value;
  try {
    if (value && typeof value == 'object') {
      result = JSON.stringify(value);
    } else {
      result = encodeURIComponentECO(value);
    }
  } catch (error) {
    console.log(error, 'value:' + value);
  }
  return result;
}

function encodeURIComponentECO(str) {
  return encodeURIComponent(str).replace(/[.!'()*]/g, function (c) {
    return '%' + c.charCodeAt(0).toString(16);
  });
}

function formSort(data) {
  return data.sort((a, b) => {
    const aOrder = a.display_order ?? 1 << 10;
    const bOrder = b.display_order ?? 1 << 10;
    return aOrder - bOrder;
  });
}

// export async function handleDownload(filePath, fileName) {
//   const loading = Loading.service({
//     lock: true,
//     text: 'Loading',
//     spinner: 'el-icon-loading',
//     background: 'rgba(0, 0, 0, 0.7)'
//   });
//   if (filePath && filePath.startsWith('@')) {
//     filePath = filePath.substr(1);
//   }
//   let res = await downloadFile(filePath);
//   downloadFileBlob(res, fileName);
//   loading.close();
// }


// 获取 groups 扁平化对象，好用于获取值
export function getGroupsObj(data) {
  let groups = data[groupsField]
  if (!groups) return {};
  for (let key in groups) {
    if (typeof groups[key] == 'object') {
      if (hasOwn(groups[key], valueField) && !groups[key][valueField]) continue;
      for (let k in groups[key]) {
        if (k == valueField) {
          continue;
        } else {
          return {...groups[key]}
        }
      }
    }
  }
}

export function getFieldClassMarkName(field) {
  return field.replace(/[^\w-]/g, '-');
}