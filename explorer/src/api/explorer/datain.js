import { request } from "@/utils/request";
import JSONbig from "json-bigint";
import { getLocalTimezone, deepClone } from "@/utils";
import i18n from '@/lang/index'
import { getDataSource } from "./community";

let language = i18n.locale.includes('zh') ? 'zh' : 'en'
export function getTask(id, type) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks?lang=${i18n.locale}&detail=true&labels=type::${type},cluster-id::${id}`,
        method: "get"
    });
}

export function getRunningTask() {
    let id = localStorage.getItem("local_clusterID")
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks?lang=${i18n.locale}&detail=true&labels=type::datain,cluster-id::${id}&in_scheduler=true`,
        method: "get"
    });
}


export function getUIData() {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in?lang=${i18n.locale}`,
        method: 'get'
    })
}

export function generatePIDefaultConfigFile(dsn, taskId, agentId) {
    let url = `/ds/in/download/pi_default_config?from=${encodeURIComponent(dsn)}`;
    if (taskId) {
        url += `&task_id=${taskId}`;
    }
    if (agentId) {
        url += `&via=${agentId}`;
    }
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url,
        method: 'get'
    })
}

export function AddSource(data) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: '/tasks',
        method: 'post',
        headers: {
            "Content-Type": "application/json"
        },
        data
    })
}

export function EditSource(data, id) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}`,
        method: 'patch',
        headers: {
            "Content-Type": "application/json"
        },
        data
    })
}
//获取ua的nodes或者da的tags
export function getUaAndDaData(data) {
    let language = i18n.locale.includes('zh') ? 'zh' : 'en'
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/sets`,
        method: 'post',
        headers: {
            "Content-Type": "application/json"
        },
        data: {
            ...data,
            lang: language
        }
    })
}

function loadTaskDetail(id) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}?detail=true&lang=${i18n.locale}`,
        method: 'get',
        headers: {
            "Content-Type": "application/json"
        }

    })
}

function mergeTaskDetailOptions(cfgOptions, data, from) {
    if (data.id === 'kafka') {
        const startIndex = from.indexOf('kafka://') + 'kafka://'.length;
        const endIndex = from.indexOf('?');
        const hostsAndPorts = from.substring(startIndex, endIndex);
        const endpoints = hostsAndPorts.split(',')
        
        while (cfgOptions.params.length < endpoints.length) {
            let item = deepClone(cfgOptions.params[0])
            cfgOptions.params.push({...item}); 
        }
        for (let i = 0; i < endpoints.length; i++) {
            cfgOptions.params[i].host.value = endpoints[i].split(':')[0]
            cfgOptions.params[i].port.value = endpoints[i].split(':')[1]
        }
    } else {
        for (let key in cfgOptions) {
            if (data[key]) {
                cfgOptions[key].value = data[key].toString();
            } else if (data.params[key]) {
                cfgOptions[key].value = data.params[key];
            }
        }
    }

    if (cfgOptions.endpoint && !cfgOptions.endpoint.value) {
        cfgOptions.endpoint.value = `${data.host}`;
        if (data.port) {
            cfgOptions.endpoint.value += `:${data.port}`
        }
        if (data.subject) {
            cfgOptions.endpoint.value += `/${data.subject}`;
        }
        if (data.id === 'tmq') {
            if (data.username && data.password) {
                cfgOptions.endpoint.value = `tmq${data.protocol ? '+' + data.protocol: ''}://${data.username}:${data.password}@${cfgOptions.endpoint.value}`;
            } else {
                cfgOptions.endpoint.value = `tmq${data.protocol ? '+' + data.protocol: ''}://${cfgOptions.endpoint.value}`;
            }
        }
    }
}

function mergeTaskDetailParams(cfgParams, dataParams) {
    let haveAvailable = false;
    for (let i = 0; i < cfgParams.length; i++) {
        let key = cfgParams[i].name;
        if (dataParams[key]) {
            cfgParams[i].value = dataParams[key];
            haveAvailable = true;
            if (cfgParams[i].hint?.type === 'compose') {
                cfgParams[i].type_value = dataParams[key + '_type'];
            }
            if (cfgParams[i].hint?.type === 'timeout' || cfgParams[i].hint?.type === 'duration') {
                // dataParams[key + '_type'] 不存在，则是纯字符串版本
                cfgParams[i].type_value = dataParams[key] 
                    ? dataParams[key].match(/[a-zA-Z]+$/) 
                        ? dataParams[key].match(/[a-zA-Z]+$/)[0] 
                        : cfgParams[i].type_value
                    : '';
                cfgParams[i].value = dataParams[key] ? dataParams[key].match(/\d+/)[0] : '';
            }
        }
    }
    return haveAvailable;
}

function haveAuthentication(auth_alternatives, auth_type) {
    for (let i = 0; i < auth_alternatives.length; i++) {
        if (auth_alternatives[i].name === auth_type) {
            return true;
        }
    }
    return false;
}

function mergeAuthentication(cfgAuth, data) {
    if (cfgAuth.alternatives.length > 1) {
        // 多种认证方式时，需要判断用的是那种认证类型
        if (data.username && data.password && haveAuthentication(cfgAuth.alternatives, 'plain')) {
            cfgAuth.value = 'plain';
        } else if (data.params.auth_certificate && haveAuthentication(cfgAuth.alternatives, 'certificates')) {
            cfgAuth.value = 'certificates';
        } else if (data.params.version && data.params.version.split('.')[0] == '1' && haveAuthentication(cfgAuth.alternatives, '1.x')) {
            cfgAuth.value = '1.x'
        }
    }

    for (let i = 0; i < cfgAuth.alternatives.length; i++) {
        let authentication = cfgAuth.alternatives[i];
        if (authentication.name === cfgAuth.value) {
            for (let key in authentication) {
                if (key === 'params' && authentication[key].length > 0) {
                    for (let j = 0; j < authentication[key].length; j++) {
                        let param = authentication[key][j];
                        if (data.params[param.name]) {
                            param.value = data.params[param.name];
                        } else if (data[param.name]) {
                            // username&password
                            param.value = data[param.name];
                        }
                    }
                } else if (authentication[key].display && data[key]) {
                    authentication[key].value = data[key];
                }
            }
        }
    }
}

// 前端组装数据，不使用后端的 from_detail
export async function refreshTask(id) {
    let taskDetail = await loadTaskDetail(id)
    let dsType = taskDetail.from_expand.id;

    let dsConfig = getDataSource(i18n.locale, dsType);
    const data = taskDetail.from_expand;
    const from = taskDetail.from
    
    mergeTaskDetailOptions(dsConfig.options, data, from);
    if (dsConfig.advanced && dsConfig.advanced.params) {
        mergeTaskDetailParams(dsConfig.advanced.params, data.params);
    }
    if (dsConfig.params) {
        mergeTaskDetailParams(dsConfig.params, data.params);
    }
    if (dsConfig.authentication) {
        mergeAuthentication(dsConfig.authentication, data);
    }
    
    for (let i = 0; i < dsConfig.groups.length; i++) {
        let haveAvailable = mergeTaskDetailParams(dsConfig.groups[i].params, data.params);
        if (haveAvailable && dsConfig.groups[i].collapsed === false) {
            dsConfig.groups[i].collapsed = true;
        }
    }
    if (dsConfig.datasets) {
        const categories = dsConfig.datasets.categories;
        for (let i = 0; i < categories.length; i++) {
            if (data.params[categories[i].category]) {
                dsConfig.datasets.value = categories[i].category
                if (categories[i].params) {
                    mergeTaskDetailParams(categories[i].params, data.params);
                }
                if (categories[i].target && categories[i].target.name === categories[i].category) {
                    if (categories[i].target.multiple) {
                        categories[i].target.value = [data.params[categories[i].category]];
                    } else {
                        categories[i].target.value = data.params[categories[i].category];
                    }
                }
                break;
            }
        }
    }
    taskDetail.from_detail = dsConfig;
    return taskDetail;
}

export function uploadFile(file) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/upload`,
        method: 'post',
        headers: {
            "Content-Type": "multipart/form-data"
        }

    })
}

export function getCSVColumns(path, type, other) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/filemeta?file_path=${path}&file_type=${type}${other ? '&' + other : ''}`,
        method: 'get',
        headers: {
            "Content-Type": "multipart/form-data"
        }
    })
}

export function getAgentActivities(agentId) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/agents/${agentId}/activities`,
        method: 'get'
    })
}

export function getTaskActivities(taskId) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${taskId}/activities`,
        method: 'get'
    })
}

export function getMetrics(taskId) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${taskId}/metrics`,
        method: 'get'
    })
}

export function validateTask(data) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: '/ds/in/validate',
        method: 'post',
        data
    })
}

export function getFileStream(filepath) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/download?file_path=${filepath}`,
        method: 'get',
        responseType: 'blob'
    })
}

export function downlaodAllNodes(data, agentid) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/download/all_data_sets?from=${data}` + (agentid ? `&via=${agentid}` : ''),
        method: 'get',
        responseType: 'blob',
    })
}

export function checkParseData(data) {
    let mutateRules = data.parser?.mutate;
    if (!mutateRules) {
        return;
    }

    // 检查 extract 规则
    for (let i = 0; i < mutateRules.length; i++) {
        if (mutateRules[i].extract) {
            let extract = mutateRules[i].extract;
            if ("" in extract) {
                return "datasource.transformer.extractrule.nofield";
            }
            for (let key in extract) {
                if ("" in extract[key]) {
                    return "datasource.transformer.extractrule.norule";
                }
            }
        }
    }
}

export function getParser(data, messagebox) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/transform/sample/flat?tz=${getLocalTimezone()}`,
        method: 'post',
        transformResponse: [function (data) {
            try {
              return JSONbig.parse(data);
            } catch (error) {
              return data;
            }
          }],
        data
    })
}

// export function listParserPlugins() {
//     return [{
//         "id": "hebeipower.dll",
//         "name": "hebeipower",
//         "version": "0.1.0"
//     }];
// }

export function listParserPlugins() {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/transform/parser/plugins`,
        method: 'get',
    })
}

export function getMetricsDesc(data) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/metrics/description?lang=${i18n.locale}`,
        method: 'get',
    })
}

export function getSampleDataMsgbody(datatype,data,agentid,timeout){
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/sample?dsn=${datatype}${data}` + (agentid ? `&via=${agentid}` : '') + (timeout ? `&timeout=${timeout}` : ''),
        method: 'get',
        transformResponse: [function (data) {
            try {
              return JSONbig.parse(data);
            } catch (error) {
              return data;
            }
          }],
    })
}

// opc：提交数据点位模版文件下载请求，获取 ticket
export function getTicket(data, agentid, category) {
    let language = i18n.locale.includes('zh') ? 'zh' : 'en'
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/point/file/download/task?from=${encodeURIComponent(data)}&lang=${language}&categories=${category}` + (agentid ? `&via=${agentid}` : ''),
        method: 'get'
    })
}

// opc：检查数据点位模版文件是否准备好
export function checkReadyFile(ticket) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/point/file/are/you/ready?ticket=${ticket}`,
        method: 'get'
    })
}

// opc：下载数据点位模版csv文件
export function downlaodOpcPointFile(ticket) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/point/file/async?ticket=${ticket}`,
        method: 'get',
        responseType: 'blob',
    })
}

/**
 * opc：分页获取数据点位
 * @param {*} ticket 
 * @param {*} page 
 * @param {*} pageSize 
 */
export function getDatasets(ticket,page,pageSize) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/point/data/page?ticket=${ticket}&page=${page}&page_size=${pageSize}`,
        method: 'get'
    })
}

// 下载 csv 空模版
export function getCsvEmptyTemplate(driver) {
    let language = i18n.locale.includes('zh') ? 'zh' : 'en'
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/point/file/template?driver=${driver}&lang=${language}`,
        method: 'get',
        responseType: 'blob',
    })
}

// 获取表同步进度
export function getTableProgress(id,params) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}/table_progress?${params}`,
        method: 'get',
        transformResponse: [function (data) {
        try {
            return JSONbig.parse(data);
        } catch (error) {
            return data;
        }
        }]
    })
}

//  获取 vgroup 消费进度
export function getVgroupProgress(id) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}/vgroup_progress`,
        method: 'get',
    })
}

//  获取 csv 文件处理进度
export function getCSVProgress(id) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/${id}/csv_files`,
        method: 'get',
    })
}

// 校验 opc 点位合法性
export function validOpcFile(dsn) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/point/file/is_valid?dsn=${encodeURIComponent(dsn)}`,
        methods: 'get'
    })
}

// 批量启动任务
export function batchStartTask(data) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/start`,
        method: "post",
        data
    });
}
// 批量停止任务
export function batchStopTask(data) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/stop`,
        method: "post",
        data
    });
}
// 批量删除任务
export function batchDelTask(data) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/tasks/delete`,
        method: "post",
        data
    });
}
// 增加点位
export function addOpcPoint(data) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/opc/csv/points`,
        method: "post",
        data
    });
}

// 查看点位配置csv Header 
export function getOpcCsvHeader(taskId) {
    return request({
        baseURL: process.env.VUE_APP_X_API,
        url: `/ds/in/opc/csv/points/header?task_id=${taskId}`,
        method: "get",
    });
}