import { uuid, hasOwn } from '@/utils/util';
import { cloneDeep } from 'lodash';
import { isObject, isArray } from '@/utils/validate';
import { StaticTemplatePath } from '@/const';
import { Loading } from 'element-ui';
import { parsinginZone, decrypt, formatTime } from "@/utils/index";
import i18n from '@/lang';
import store from '@/store/modules/app';
import { cs } from 'date-fns/locale';

const lang = i18n.locale.includes('zh') ? 'zh' : 'en';
const templateUrlMap = {
  opcua: `template-${lang}.csv`,
  opcda: `template-${lang}.csv`,
  point_file: `/Points.csv`,
  template_for_pi_point_file: '/ElementTemplates.csv',
  template_for_af_element_file: '/ElementTemplates.csv'
};
const DownloadUrl =  process.env.VUE_APP_X_API + `/download?file_path=`
const ReplacePoint = '~';
const InfoParams = ['security_policy', 'security_mode'];
const Info2Params = ['point_file','template_for_pi_point_file', 'template_for_af_element_file','csv_config_file'];
export const TimeFormats = ['beginDateTime', 'endDateTime', 'start', 'end', 'beginTime', 'endTime', 'BackfillStartTime', 'BackfillEndTime'];
export const PayConnectorList = ['pi', 'opcua', 'opcda', 'pibackfill'];
export const ComposeParams = ['timeout','schema-polling-interval','unit','retro','excursion','interval','MaxBackfillRangeDays','timeWindow','retrieveInterval','tolerance', 'delay', 'local_threshold'];
const SelectAllPoints = 'child_table_expression'
// // 无法使用symbol作为key，因为会被for in 和 object.keys过滤掉
const valueField = uuid();
export const optionsField = uuid();
const groupsFieldBeforeConnection = uuid();
const groupsFieldAfterConnection = uuid();
const advancedField = uuid();
const piOptionShowValue = 'PI Data Archive and Asset Framework (AF) Server';
const historianLiveTable = 'Runtime.dbo.Live'
const historianSynchronizeMode = 'synchronize'
const opcuaSecuritymodeValue = 'None'
const opcGroupShowValue = 'csv_config_file'
const piAdvancedShowValue = 'multi-column'
const authenticationField = uuid();
export const datasetsField = uuid();
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

export function getDataRange(datatype) {
  switch (datatype) {
    case 'TINYINT':
      return [-128, 127, 4]
    case 'TINYINT UNSIGNED':
      return [0, 255, 3]
    case 'SMALLINT':
      return [-32768, 32767, 6]
    case 'SMALLINT UNSIGNED':
      return [0, 65535, 5]
    case 'INT':
      return [-2147483648, 2147483647, 11]
    case 'INT UNSIGNED':
      return [0, 4294967295, 10]
    case 'BIGINT':
      return [-9223372036854775808n, 9223372036854775807n, 20]
    case 'BIGINT UNSIGNED':
      return [0, 18446744073709551615n, 20]
    case 'FLOAT':
      return [-3.4E38, 3.4E38, 38]
    case 'DOUBLE':
      return [-1.7E308, 1.7E308, 308]
  }
  return null;
}

// 根据返回的数据源参数定义生成对应的表单配置
export function getFormConfigByDataSource(dataSource, parserValue) {
  return dataSource.reduce((formConfig, item) => {
    const { id, name, type, strict, description, protocol, authentication, groups, options, datasets, parser, params, advanced } = item;
  
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
      config: paramsConfig,
      parser
    };
    currentType = id;

    handleParams(params, paramsConfig);
    handleProtocol(protocol, paramsConfig);
    handleOptions(options, paramsConfig, id);
    handleAuthentication(authentication, paramsConfig);
    
    handleGroups(groups, paramsConfig, true, id);
    if (id != 'csv') {
      handleConnectivityCheck(paramsConfig)
    }
    handleDatasets(datasets, paramsConfig);
    handleGroups(groups, paramsConfig, false, id);
    handleParser(parser, paramsConfig, parserValue,id);
    handleCsvData(id,paramsConfig);
    handleAdvanced(advanced, paramsConfig, id)
    // 先处理protocol
    if(id=='csv'){
      config.parser=parserValue
      let index=paramsConfig.findIndex((item)=>{
        return ['连接配置','Connection Configuration'].includes(item.label)
      })
      paramsConfig.splice(index,1)
    }
    formConfig[id] = config;
   
    return formConfig;
  }, {});
}

function handleConnectivityCheck(paramsConfig) {
  paramsConfig.push({
    field: 'checkConnectivity',
    type: 'checkConnectivity',
    children: []
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
function handleProtocol(protocol, paramsConfig) {
  if (!protocol) return;
  const { display, description, choices, value = '' } = protocol;
  paramsConfig[0].children.push({
    label: display,
    description,
    field: 'protocol',
    type: 'select',
    display_order: 0,
    defaultValue: value,
    if: currentData => {
      if (!currentData.system_configuration) return true;
      return currentData.system_configuration == piOptionShowValue;
    },
    required: true, // just required for cloud
    options: choices.map(item => ({
      label: item.display,
      value: item.name,
      description: item.description
    }))
  });
}
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
      item.params.forEach((param, index) => {
        const { display, description, name, value: defaultValue, required = false, placeholder } = param;
        const config = {
          label: display,
          description,
          placeholder,
          required,
          field: name,
          defaultValue: defaultValue ?? '',
          accept: '.pem,.der,.cert,.key,.crt',
        };
        if (name == 'orgId') {
          config.pattern = /^[0-9a-fA-F]+$/
          config.patternMsg = i18n.t('dataIn.orgIdTip')
        }
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
function handleOptions(options, paramsConfig, id) {
  if (!options) return;
  if (id === 'kafka') {
    paramsConfig[0].type = 'grouping'
    paramsConfig[0].children = options.params.map((param, index) => {
      const keys = Object.keys(param);
      let uid = uuid()
      keys.forEach(key => {
        const { display, description, placeholder, required, value, pattern, patternMsg } = param[key]
        param[key] = {
          label: display,
          description,
          field: key + '_' + uid,
          placeholder,
          required: !index ? required: false,
          pattern: pattern || null,
          patternMsg,
          defaultValue: value ?? '',
          value,
        }
        handleHintType(param[key], param[key]?.hint);
      })
      return param
    })
  } else {
    const children = paramsConfig[0]?.children ?? [];
    const keys = Object.keys(options);
    keys.forEach(key => {
      const { display, description, placeholder, required, value, pattern, patternMsg } = options[key];
      if (!display) return;
      const config = {
        label: display,
        description,
        field: key,
        placeholder,
        // required,
        pattern: pattern || null,
        patternMsg,
        defaultValue: value ?? '',
        if: currentData => {
          if (!currentData.system_configuration || key == 'host') return true;
          return currentData.system_configuration == piOptionShowValue;
        },
        required: (currentData) => {
          if (id?.startsWith('opcua')) {
            return ['private_key','security_policy','certificate'].includes(key)
              ? checkValue(currentData.security_mode) && 
                currentData.security_mode !== opcuaSecuritymodeValue 
              : required
          } else {
            return required
          }
        },
         disabled: (currentData,b,c,isEdit) => {
          if (id?.startsWith('opcua')) {
            // 特殊处理 opcua 安全策略
            if ( currentData.security_mode == opcuaSecuritymodeValue) {
              currentData.security_policy = '';
            }
            return currentData.security_mode == opcuaSecuritymodeValue &&
              ['security_policy'].includes(key)
          } 
          if (id?.startsWith('mqtt')) {
            return ['host','port'].includes(key) && isEdit ? isEdit : false;
          }
          return false
        },
      };
      if (key == 'host') {
        config.display_order = 1;
      }
      handleHintType(config, options[key]?.hint);
      children.push(config);
    });
    formSort(children);
  }
}
// 处理 csv parser
function handleCsvData(id, paramsConfig) {
  if (id != 'csv') return;
  paramsConfig.push({
    label: '',
    field: 'csvData',
    type: 'csvData',
    children: []
  });
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

function handleParser(parser, paramsConfig, value = cloneDeep(DefaultParserValue),id) {
  if (!parser) return;
  const { display, description, fields } = parser;
  paramsConfig.push({
    label: display,
    description:['mqtt','kafka','mongodb'].includes(id)?'':description,
    field: 'parser',
    type: 'parser',
    fields: fields,//fields.filter(item => item.name != 'payload'),
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
function handleDatasets(datasets, paramsConfig) {
  if (!datasets) return;
  let { name, description, categories, params, value: tabValue } = datasets;
  const datasetsConfig = {
    label: name,
    description,
    field: datasetsField,
    type: 'tabs',
    multiple: false,
    name: 'datasets',
    children: params
      ? params?.map((item, index) => {
          const { display, description, name, value, required = false } = item;
          const config = {
            label: display,
            labelShow: false,
            labelWidth: '0px',
            description,
            required: true,
            field: name,
            name,
            type: 'dataset',
            accept: '.csv',
            defaultValue: value ?? '',
            disabled: (_, originalData) => {
              const optionData = originalData[optionsField];
              if (!optionData.system_configuration) return false;
              return optionData.system_configuration != piOptionShowValue && !!index;
            }
          };
          if (templateUrlMap[name]) {
            config.templateUrl = templateUrlMap[name];
          }
          handleInfoParams(config);
          return config;
        })
      : categories.map((item, index) => {
          const { category, display, short_description, description: desc, target, params: categoryParams } = item;
          const paramConfig = {
            label: display,
            name: category,
            labelShow: false,
            labelWidth: '0px',
            category,
            radio: !!index,
            description: desc,
            short_description,
            field: handleField(target.name),
            type: 'dataset',
            accept: '.csv',
            templateUrl: templateUrlMap[currentType],
            placeholder: target.description,
            required: target.required,
            multiple: target.multiple,
            editable: target.editable,
            selectable: target.selectable,
            children: categoryParams
              ? categoryParams.map(categoryParam => {
                  const config = {
                    ...categoryParam,
                    label: categoryParam.display,
                    field: categoryParam.name,
                    defaultValue: categoryParam?.multiple ? categoryParam?.value?.split(',') : categoryParam.value ?? '' ,
                    multiple: categoryParam.multiple ?? false
                  };

                  handleHintType(config, categoryParam.hint);
                  // 特殊处理 opc 的点位过滤
                  if (currentType.startsWith('opc')) {

                    if (config.field == 'pattern') {
                      config.type = 'pattern';
                    }
                    if (config.field == 'namespaces') {
                      config.options = (that) => {
                        const { namespaces = [] } = that.$store.state.app.connectivityCheckResult
                        let list = []
                        namespaces.map((item,index) => {
                          if (index > 0) {
                            list.push({ label: item, value: `${index}`}) 
                          }
                        })
                        return list
                      }
                    }
                  } else if (currentType == 'pi' || currentType == 'pibackfill') {
                    if (config.field == 'filter_value') {
                      config.options = (that) => {
                        let activeTabValues = getActiveTabValueObject(that.sourceParent.sourceForm.data);
                        if (that.sourceParent.sourceForm.data[optionsField]['system_configuration'].indexOf('AF') < 0) {
                          activeTabValues['filter_value_type'] = 'point'
                          return [{label: 'point name',value: 'point'}]
                        } else {
                          activeTabValues['filter_value_type'] = 'template'
                          return [{label: 'template',value: 'template'}]
                        }
                      }
                    }
                  }
                  return config;
                })
              : undefined,
            defaultValue: isArray(target?.value) ? target.value.join(',') : '',
            disabled: (currentData, topData, currentDefinition) => {
              if (currentDefinition?.id?.startsWith('opc')) {
                return !topData?.[optionsField]?.endpoint;
              } else {
                return false;
              }
            }
          };
          handleInfoParams(paramConfig);
          return paramConfig;
        })
  };
  if (categories) {
    tabValue = tabValue === SelectAllPoints ? 'select_all_points' : tabValue
    datasetsConfig.valueField = valueField;
    datasetsConfig.defaultValue = tabValue ?? categories[0].category;
  }
  if (params) {
    datasetsConfig.valueField = valueField;
    datasetsConfig.defaultValue = tabValue ?? params[0].name
  }
  paramsConfig.push(datasetsConfig);
}

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
function handleGroups(groups, paramsConfig, beforeConnectionCheck, id) {
  if (!groups) return;
  groups = groups.sort((a, b) => a.display_order - b.display_order);
  const children = [];
  paramsConfig.push({
    label: 'Groups-' + (beforeConnectionCheck ? 'before' : 'after'),
    field: beforeConnectionCheck ? groupsFieldBeforeConnection : groupsFieldAfterConnection,
    hide: true,
    children
  });
  groups.forEach(group => {
    if ((beforeConnectionCheck && !group.connection_option) || (!beforeConnectionCheck && group.connection_option)) {
      return;
    }
    const { name, description: d1, params, collapsible = false, collapsed = true, short_description: d2, display} = group;
    const paramChildren = [];
    const config = { label: id == 'taos' ? display: name, field: id == 'taos' ? name : uuid(), description: d1 ?? d2, children: paramChildren, hide: false };
    if (collapsible) {
      config.type = 'switch';
      config.defaultValue = collapsed;
      config.valueField = valueField;
      config.hasValue = true;
    }
    children.push(config);
    params.forEach(param => {
      const { display, description, short_description, name, hint, placeholder = '', required = false, value, multiple, pattern, patternMsg, grid_two = false, type_value, edit_disabled, validator } = param;
      const paramConfig = {
        label: display,
        description: description ?? short_description,
        field: handleField(name),
        // if: collapsible ? data => data[valueField] : true,
        if: (currentData, originalData) => {
          const datasetsData = originalData[datasetsField];
          if (collapsible) {
            if (id === 'kafka' && group.display_order == 1) {
              return (currentData.sasl_mechanism == "GSSAPI" )
                ? currentData[valueField] && !['sasl_username','sasl_password'].includes(name) 
                : currentData[valueField] && ['sasl_mechanism','sasl_username','sasl_password'].includes(name)
            } else {
              return currentData[valueField];
            }
          } 
          // if (!currentData.table) return true;
          if (id == 'taos') {
            const migrateOptionsFiled = 'migrate_options';
            const { mode, schema } = originalData[groupsFieldAfterConnection][migrateOptionsFiled]
            if (schema == 'only') {
              config.hide = ['realtime_settings','range'].includes(config.field)
              return !['start','end','unit','retro','interval','excursion'].includes(name)
            }
            if (mode == 'realtime') {
              config.hide = ['range'].includes(config.field)
              return !['start','end','unit'].includes(name)
            } 
            if (mode == 'history') {
              config.hide = ['realtime_settings'].includes(config.field)
              return !['retro','interval','excursion'].includes(name)
            }
            config.hide = false;
          }
          if (id.startsWith('opc')) {
            if (datasetsData && datasetsData[valueField] === opcGroupShowValue) {
              if (currentData.collect_mode == "subscribe") {
                // 只显示采集模式 
                return ['collect_mode'].includes(name)
              } else {
                return !['update_interval','update_mode'].includes(name)
              }
            } else {
              if (currentData.collect_mode == "subscribe") {
                return !['interval','request_timeout'].includes(name)
              } else {
                return true
              }
            }
          } else if (currentData.mode == historianSynchronizeMode) {
            if (currentData.table == historianLiveTable) {
              return ['mode','table','tags','retrieveInterval'].includes(name)
            } else {
              return !['endDateTime'].includes(name)
            }
          } else if (currentData.trust_cert){
            return !['trust_cert_ca'].includes(name)
          } else {
            return !['table','retrieveInterval','tolerance'].includes(name)
          }
        },
        placeholder,
        defaultValue: multiple ? value?.split(',') : value,
        required: (currentData,b,c,isEdit) => {
          if (id?.startsWith('kafka') || id?.startsWith('mqtt')) {
            return ['client_id','group'].includes(name) && 
              isEdit ? !isEdit: required;
          } 
          return required;
        },
        disabled: (currentData,b,c,isEdit) => {
          return isEdit ? edit_disabled : false
        },
        multiple,
        pattern: pattern || null,
        patternMsg,
        grid_two,
        type_value,
        validator
      };
      handleHintType(paramConfig, hint, value);
      // 2024-05-17，pibackfill remove the special rule
      // if (isArray(conflicts_with)) {
      //   paramConfig.disabled = currentData => {
      //     const conflict = conflicts_with.find(item => currentData?.[item.name] == item.value);
      //     if (conflict && conflict?.when == currentData[paramConfig.field]) {
      //       currentData[paramConfig.field] = '';
      //     }
      //     return !!conflict;
      //   };
      // }

      // postgres/mysql/sql server 的 sql 在编辑状态下不能修改
      if ((currentType == 'postgres' || currentType == 'mysql' || currentType == 'oracle' || currentType == 'mssql' || currentType == 'mongodb') && paramConfig.field == 'sql') {
        paramConfig.disabled = (a,b,c,isEdit) => {
          return isEdit;
        };
      }

      // 特殊处理 influxdb 的 bucket
      if ((currentType == 'influxdb' && paramConfig.field == 'bucket') || (currentType == 'opentsdb' && paramConfig.field == 'metrics')) {
        paramConfig.type = 'bucket';
      }
      // 特殊处理 historian 的 mode
      if ((currentType == 'avevaHistorian' || currentType == 'mysql' || currentType == 'postgres') && paramConfig.field == 'mode') {
        paramConfig.type = 'mode';
      }
      if ((currentType == 'mqtt' && paramConfig.field == 'client_id') || (currentType == 'kafka' &&(paramConfig.field == 'client_id' || paramConfig.field == 'group' ))) {
        paramConfig.type = 'customId'
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
        if (currentType == 'mssql') {
          paramConfig.accept = '.pem'
        }
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

function handleParams(params, paramsConfig) {
  if (!params) return;
  params = params.sort((a, b) => a.display_order - b.display_order);
  const children = paramsConfig[0].children;
  params.forEach((param, index) => {
    const { name, display, description, required, placeholder, value, hint } = param;
    const config = { label: display, field: handleField(name), description, required, placeholder, defaultValue: value, display_order: index };
    if (name != 'system_configuration') {
      config.if = currentData => {
        if (!currentData.system_configuration) return true;
        return currentData.system_configuration == piOptionShowValue;
      };
    }
    handleHintType(config, hint);
    children.push(config);
  });
}

/*
"advanced": {
    "name": "Advanced Options",
    "description": "Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n",
    "collapsible": true,
    "params": [
      {
        "name": "concurrency",
        "display": "Read Concurrency",
        "hint": {
            "type": "integer",
            "min": 1,
            "max": 1000
        },
        "description": "The number of concurrent queries when reading data from the data source.\n",
        "value": "1",
        "hidden": true
      },
      {
          "name": "log_level",
          "display": "Log Level",
          "hint": {
            "type": "str",
            "choices": [
                "error",
                "warn",
                "info",
                "debug",
                "trace"
            ]
          },
          "description": "Adjust the log level of the data source as required. This parameter does not always take effect.",
          "value": "info"
      }
    ]
  }
*/ 
function handleAdvanced(advanced, paramsConfig, id) {
  if (!advanced) return;
  const { params, collapsible, collapsed = true, name, description } = advanced
  const children = [];
  const config = {
    label: name,
    field: advancedField,
    description,
    type: 'advanced',
    defaultValue: collapsible,
    collapsible: collapsible ? 'one' : '',
    children
  }
 
  paramsConfig.push(config);
  
  params.forEach(group => {
    const { name, value, display, hint, hidden = false, required = false, placeholder, description: d1, short_description: d2 } = group;
    const paramChildren = [];
    const config = { 
      label: display, 
      field: handleField(name), 
      description: d1 ?? d2, 
      defaultValue: value,
      // if: !hidden,
      placeholder,
      required,
      if: (currentData, originalData) => {
        if (id == 'pi') {
          const datasetsData = originalData[datasetsField];
          if (piAdvancedShowValue == datasetsData[valueField]) return true
          return ['batch_size', 'batch_timeout', 'log_level'].includes(name) 
        }
        return !hidden
      }
    };
    handleHintType(config, hint, value);
    children.push(config);
  });
}

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
    // case 'timeout':
    //   config.type = 'input';
    //   break;
    case 'file':
      config.type = 'file';
      break;
    case 'password':
      config.type = 'password';
      break;
    case 'host':
      config.type = 'grouping';
      break;
    case 'customId':
      config.type = 'customId';
      break;
    case 'pibackfillTime':
      config.type = 'pibackfillTime';
      config.options = hint;
      if (!config.defaultValue) {
        config.defaultValue = hint.find(item => item.selected)?.value;
      }
      break;
    case 'compose':
      config.type = 'compose';
      if (hint?.choices) {
        config.options = hint.choices.filter(item => item != '--NONE--').map(item => ({
          label: item,
          value: item
        }));
      }
      break;
    case 'duration':
    case 'timeout':
      config.type = 'composeAppend';
      if (hint?.choices) {
        config.options = hint.choices;
      }
      config.min = hint?.min ?? -Infinity;
      config.max = hint?.max ?? Infinity;
      break;

    default:
      if (hint?.choices) {
        config.type = 'select';
        // 过滤 --NONE-- 
        config.options = hint.choices.filter(item => item != '--NONE--').map(item => ({
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
  // 目前是 dataset upload desc 直接展示
  if (config?.field && Info2Params.includes(config.field)) {
    config.info2 = true;
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
    const value = item.type == 'number' ? Number(item.defaultValue) : item.defaultValue ?? '';

    if (item.children && item.children.length) {
      data[item.field] = generateFormInitData(item.children);
      if (item.valueField) {
        data[item.field][item.valueField] = value;
      }
      if (item.type === 'grouping') {
        item.children.forEach(child => {
          data[item.field][child.host.field]= child.host.defaultValue ?? ''
          data[item.field][child.port.field]= child.port.defaultValue ?? ''
        }) 
      }

    } else {
      data[item.field] = value;
      if (item.type === 'compose' && item.hint?.choices) {
        data[item.field + '_type'] = item.type_value || "";
      }
      if (item.type === 'composeAppend') {
        data[item.field + '_type'] = item.type_value || "";
        data[item.field] = value ? value?.match(/\d+/)[0] : undefined;
      }
    }
    return data;
  }, {});
}
export const NoNeedAgentType = ['tmq', 'taos', 'csv'];
// tmq和taos需要再协议前面加上+
export const ProtocolPrefix = NoNeedAgentType.concat(['influxdb', 'opentsdb']);

export function getActiveTabValueObject(data) {
  const activeTab = data[datasetsField][valueField];
  return data[datasetsField][activeTab];
}

export function getActiveTabKey(data) {
  return data[datasetsField][valueField];
}

export function getOptionsValue(data) {
  return data[optionsField];
}

export function getCSVOptions(data, definition) {
  let queryArr = [];
  getGroupsQuery(data[groupsFieldBeforeConnection], queryArr, definition);
  getGroupsQuery(data[groupsFieldAfterConnection], queryArr, definition);
  return queryArr;
}

export function getDsnData(data, definition) {
  let dsn = handleProtocolData(data[optionsField]?.protocol, definition);
  let queryArr = [];
  dsn += getAuthentications(data[authenticationField], queryArr, definition);
  dsn += getOptionData(data[optionsField], queryArr, definition);
  getGroupsQuery(data[groupsFieldBeforeConnection], queryArr, definition);
  getGroupsQuery(data[groupsFieldAfterConnection], queryArr, definition);
  getDatasetsQuery(data[datasetsField], data, queryArr);
  getAdvancedQuery(data[advancedField],queryArr)
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
  if (definition.id == 'pi' || definition.id == 'pibackfill') {
    dsn += '&model=' + getActiveTabKey(data);
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
  if(id=='csv'){
    let csvFileConfig = store.state.csvFileListener || {file_or_dir: "1", keep_processed_files: true}
    if (csvFileConfig.file_or_dir === '1') {
      return dsn + store.state.csvfiles.map(item => item.response[0]).join(',') 
                + '?file_or_dir=' + csvFileConfig.file_or_dir 
                + '&keep_processed_files=' + csvFileConfig.keep_processed_files
    } else {
      return dsn + csvFileConfig.fileurl
                + '?file_or_dir=' + csvFileConfig.file_or_dir 
                + '&file_pattern=' + csvFileConfig.file_pattern
                + '&new_file_notify=' + csvFileConfig.new_file_notify
                + '&notify_interval=' + csvFileConfig.notify_interval + 's'   // 固定单位为秒
                + '&sort=' + csvFileConfig.sort 
    }
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

function getGroupsQuery(groups, query, definition) {
  groups = cloneDeep(groups)
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
            let value = groups[key][k]
            if (typeof groups[key][k] == 'object') {
              value = formatTime(groups[key][k])
            }
            query.push(field + '=' + getQueryParamValue(value));
          } else if (ComposeParams.includes(k)) {
            let type_value = checkValue(getQueryParamValue(groups[key][k+'_type'])) ? getQueryParamValue(groups[key][k+'_type']) : ''
            query.push(field + '=' + getQueryParamValue(groups[key][k]) + type_value);
          } else if (/_type$/.test(k)) {
            delete groups[key][k+'_type'];
          } else {
            // todo 临时解决 mongoDB 增加 tls 参数
            if (definition.id == 'mongodb') {
              query.push('tls' + '=' + checkValue(getQueryParamValue(groups[key]['cert_key_file_path'])));
            }
            query.push(field + '=' + getQueryParamValue(groups[key][k]));
          }
        }
      }
    }
  }
}

function getAdvancedQuery(advanced, query) {
  if (!advanced) return query;
  for (let key in advanced) {
    if (!checkValue(advanced[key])) continue;
    if (key == valueField) {
      continue;
    } else {
      const field = getOriginField(key);
      query.push(field + '=' + getQueryParamValue(advanced[key]));
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
      query.push(tabValue + '=' + true);
    } else {
      if (!checkValue(datasets[tabValue])) return; 
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
export function getAuthentications(authentication, params, definition) {
  if (!authentication) return '';
  const currentData = authentication[handleField(authentication[valueField])];
  const dataFields = Object.keys(currentData);
  const { id } = definition;
  switch (authentication[valueField]) {
    case 'plain':
      if (id === 'mongodb') {
        if (currentData.mechanism) {
          params.push('mechanism=' + currentData.mechanism)
        }
        if (currentData.source) {
          params.push('source=' + currentData.source)
        }
      }
      if (!currentData.username) {
        return '';
      }
      return getQueryParamValue(currentData.username) + ':' + getQueryParamValue(currentData.password) + '@';
    default:
      params.push(...dataFields.map(item => {
        if (!checkValue(currentData[item])) return
        return getOriginField(item) + '=' + getQueryParamValue(currentData[item])
      }));
      break;
  }
  return '';
}

function getOptionData(data, queryArr, definition) {
  if (!data || !definition) return '';
  let result = '';
  let { subject, host, port, endpoint, system_configuration, PISystemName, security_mode, security_policy, certificate, private_key, connect_timeout, direct_connection, repl_set_name, local_threshold, local_threshold_type, load_balanced } = data;
  let { id } = definition;
  if (PISystemName) {
    queryArr.push('PISystemName=' + PISystemName);
  }
  if (system_configuration) {
    queryArr.push('system_configuration=' + system_configuration)
  }
  if (security_mode) {
    queryArr.push('security_mode=' + security_mode)
  } 
  if (security_policy) {
    queryArr.push('security_policy=' + security_policy)
  }
  if (certificate) {
    queryArr.push('certificate=' + certificate)
  }
  if (private_key) {
    queryArr.push('private_key=' + private_key)
  }
  if (connect_timeout) {
    queryArr.push('connect_timeout=' + connect_timeout)
  }
  // if (load_balanced) {
  //   queryArr.push('load_balanced=' + load_balanced)
  // }
  // if (!direct_connection) {
  //   queryArr.push('direct_connection=' + direct_connection)
  //   if (repl_set_name) {
  //     queryArr.push('repl_set_name=' + repl_set_name)
  //   }
  //   if (local_threshold) {
  //     queryArr.push('local_threshold=' + local_threshold + local_threshold_type)
  //   }
  // }
  if (endpoint === undefined&&definition.id!=='csv'&&definition.id!=='kafka') {
    result += host.replace(/\w*:\/\//, '');
    if (system_configuration && system_configuration != piOptionShowValue) return result;
    if (port) {
      result += ':' + port;
    }
    if (subject) {
      result += '/' + subject;
    }
  } else {
    if (id === 'tmq') {
      result += handleEndpoint(endpoint)
    } else {
      if(id=='csv'){
        result+= (Array.isArray(store.state.csvfiles)?store.state.csvfiles[0].response[0]:store.state.csvfiles)
      }else{
        result += endpoint;
      }
    }
  }
  return result;
}

// 处理 tmq endpoint
function handleEndpoint(endpoint) {
  if (!endpoint) return '';
  let url = endpoint.replace(/^(taos|tmq)\+/, "").replace(/^(http|ws):/, "ws:").replace(/^(https|wss):/, "wss:");
  if (url.includes("://")) {
    try {
      let parsed_url = new URL(url);
      if (parsed_url.protocol == "taos:" || parsed_url.protocol == "tmq:") {
        return parsed_url.toString().replace('taos:','tmq:');
      } else {
        return "tmq+" + parsed_url.toString();
      }
    } catch (error) {
      console.log("Invalid URL: ", url, error);
      // not a valid url, use as is.
      return "tmq+" + url;
    }
  } else {
    if (url.includes("6041")) {
      return "tmq+ws://" + url;
    } else {
      return "tmq://" + url;
    }
  }
}

function handleField(field) {
  return field.replace(/\./g, ReplacePoint);
}
function getOriginField(field) {
  return field.replace(new RegExp(ReplacePoint, 'g'), '.');
}

function checkValue(value) {
  if (value === undefined || value === null || value === '' || value === 'undefined') return false;
  if (Array.isArray(value)) {
    if (!value.length) return false;
  }
  return true;
}

function getQueryParamValue(value) {
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

export async function handleDownload(filePath, fileName) {
  let link = document.createElement("a");
  link.download = "file_name";
  link.href = DownloadUrl + filePath;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

// 获取 groups 扁平化对象，好用于获取值
export function getGroupsObj(data) {
  let groups = Object.assign({}, data[groupsFieldBeforeConnection] || {}, data[groupsFieldAfterConnection] || {}); 
  let obj = {}
  if (!groups) return {};
  for (let key in groups) {
    if (typeof groups[key] == 'object') {
      if (hasOwn(groups[key], valueField) && !groups[key][valueField]) continue;
      for (let k in groups[key]) {
        if (k == valueField) {
          continue;
        } else {
          obj = Object.assign({}, obj, groups[key]);
        }
      }
    }
  }
  return obj
}

export function getFieldClassMarkName(field) {
  return field.replace(/[^\w-]/g, '-');
}

export function checkJson (_, value, callback) {
  function isValidJSON(text) {
      try {
        return Object.keys(JSON.parse(text)).length !== 0;
      } catch (error) {
        return false;
      }
  }
  if (value && !isValidJSON(value)) {
    return callback(new Error(i18n.t('dataIn.jsonTip')))
  }
  return callback()
}