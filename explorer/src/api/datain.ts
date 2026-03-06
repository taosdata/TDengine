import { request } from '@/utils/request.ts';
import JSONbig from 'json-bigint';
import { getLocalTimezone, getLocalLang, getUser } from '@/utils';
import pathDetector from '@/utils/pathDetector';

const language = getLocalLang();
export function getTask(type: string) {
  const user = getUser();
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks?lang=${language}&detail=true&labels=type::${type},user::${user}`,
    method: 'get'
  });
}

export function getRunningTask() {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks?lang=${language}&detail=true&labels=type::datain&in_scheduler=true`,
    method: 'get'
  });
}

export function getUIData() {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in?lang=${language}`,
    method: 'get'
  });
}

export function generatePIDefaultConfigFile(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/download/pi_default_config`,
    method: 'post',
    data
  });
}

export function AddSource(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: '/tasks',
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    data
  });
}

export function EditSource(data: Record<string, any>, id: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${id}`,
    method: 'patch',
    headers: {
      'Content-Type': 'application/json'
    },
    data
  });
}
//获取ua的nodes或者da的tags
export function getUaAndDaData(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/sets`,
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    data: {
      ...data,
      lang: language
    }
  });
}

export function loadTaskDetail(id: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${id}?detail=true&lang=${language}`,
    method: 'get',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}
export async function refreshTask(id: string | number) {
  const taskDetail = await loadTaskDetail(id);
  taskDetail.from = taskDetail.from_json;
  return taskDetail;
}

export function skip2Latest(id: string | number, _recovery: boolean) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/kafka/${id}/seek_to_end`,
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export function uploadFile() {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/upload`,
    method: 'post',
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  });
}

export function getCSVColumns(path: string, type: string, other?: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/filemeta?file_path=${path}&file_type=${type}${other ? '&' + other : ''}`,
    method: 'get',
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  });
}

export function getAgentActivities(agentId: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/agents/${agentId}/activities`,
    method: 'get'
  });
}

export function getTaskActivities(taskId: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${taskId}/activities`,
    method: 'get'
  });
}

export function getMetrics(taskId: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${taskId}/metrics`,
    method: 'get'
  });
}

export function validateTask(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: '/ds/in/validate',
    method: 'post',
    data
  }).then(data => {
    if (!data.code) {
      return data;
    }
    return {
      valid: false,
      support: false,
      data_source: 'unknown',
      message: data.message
    };
  });
}

export function getFileStream(filepath: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/download?file_path=${filepath}`,
    method: 'get',
    responseType: 'blob'
  });
}

export function downloadAllNodes(data: string, agentid?: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/download/all_data_sets?from=${data}` + (agentid ? `&via=${agentid}` : ''),
    method: 'get',
    responseType: 'blob'
  });
}

export function checkParseData(data: any) {
  const mutateRules = data.parser?.mutate;
  if (!mutateRules) {
    return;
  }

  // 检查 extract 规则
  for (let i = 0; i < mutateRules.length; i++) {
    if (mutateRules[i].extract) {
      const extract = mutateRules[i].extract;
      if ('' in extract) {
        return 'datasource.transformer.extractrule.nofield';
      }
      for (const key in extract) {
        if ('' in extract[key]) {
          return 'datasource.transformer.extractrule.norule';
        }
      }
    }
  }
}

export function getParser(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/transform/sample/flat?tz=${getLocalTimezone()}`,
    method: 'post',
    transformResponse: [
      function (data: any) {
        try {
          return JSONbig.parse(data);
        } catch (error) {
          return data;
        }
      }
    ],
    data
  });
}
export function getMetricsDesc() {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/metrics/description?lang=${language}`,
    method: 'get'
  });
}

export function getSampleDataMsgbody(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/sample`,
    method: 'post',
    data,
    transformResponse: [
      function (data: any) {
        try {
          return JSONbig.parse(data);
        } catch (error) {
          return data;
        }
      }
    ]
  });
}

export function listParserPlugins() {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/transform/parser/plugins`,
    method: 'get'
  });
}

// 用模版的方式创建超级表的预览api
export function getStableParser(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/transform/sample/flat/s_model/preview?tz=${getLocalTimezone()}`,
    method: 'post',
    transformResponse: [
      function (data: any) {
        try {
          return JSONbig.parse(data);
        } catch (error) {
          return data;
        }
      }
    ],
    data
  });
}

// opc：提交数据点位模版文件下载请求，获取 ticket
export function getTicket(data: Recordable) {
  // If DSN is pspace, inject mode=points for download task
  try {
    const from = (data as any)?.from_json;
    if (from && (from.type === 'pspace' || from.driver === 'pspace')) {
      if (!from.params || typeof from.params !== 'object') {
        from.params = {};
      }
      // Only set when not already provided by caller
      if (!('pspace_mode' in from.params) && !('mode' in from.params)) {
        from.params.pspace_mode = 'points';
      }
    }
  } catch {
    // noop: best-effort enrichment
  }
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/point/file/download/task`,
    method: 'post',
    data
  });
}

// opc：检查数据点位模版文件是否准备好
export function checkReadyFile(ticket: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/point/file/are/you/ready?ticket=${ticket}`,
    method: 'get'
  });
}

// opc：下载数据点位模版csv文件
export function downloadOpcPointFile(ticket: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/point/file/async?ticket=${ticket}`,
    method: 'get',
    responseType: 'blob'
  });
}

// 获取 point 类型数据的选项
// 例如：KingHistorian 要显示 Tag 的过滤条件：测点组、测点、标签等
// 例如：pSpace 要显示 Root Nodes 的下拉列表
export function getPointOptions(data: Record<string, any>) {
  // Support two call styles:
  // 1) { kind, q, dsn } -> map to sets-like payload for fetching option lists
  // 2) sets-like payload already provided (from_json, categories, pattern, ...)
  let payload: Record<string, any>;

  if (data && (data.from_json || data.categories || data.pattern || data.offset !== undefined)) {
    // Assume caller already formed the payload in the same shape as /ds/in/sets
    payload = { lang: language, ...data };
    // Inject pspace_mode=nodes for pspace DSN when not explicitly set
    const from = (payload as any)?.from_json;
    if (from && (from.type === 'pspace' || from.driver === 'pspace')) {
      if (!from.params || typeof from.params !== 'object') {
        from.params = {};
      }
      if (!('pspace_mode' in from.params) && !('mode' in from.params)) {
        from.params.pspace_mode = 'nodes';
      }
    }
  } else {
    const { kind, q, dsn } = data || {};
    const via = dsn?.agent ?? undefined;
    // Prepare DSN and inject pspace_mode=nodes if driver==pspace
    const dsnObj = dsn ? { ...dsn } : undefined;
    if (dsnObj && (dsnObj.type === 'pspace' || dsnObj.driver === 'pspace')) {
      if (!dsnObj.params || typeof dsnObj.params !== 'object') {
        dsnObj.params = {};
      }
      if (!('pspace_mode' in dsnObj.params) && !('mode' in dsnObj.params)) {
        dsnObj.params.pspace_mode = 'nodes';
      }
    }
    payload = {
      from_json: dsnObj,
      categories: kind ? [kind] : [],
      pattern: q ?? '',
      offset: 0,
      limit: 300,
      lang: language
    };
    if (via !== undefined && via !== null && via !== '') {
      payload.via = via;
    }
  }

  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/point/options`,
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    data: payload
  });
}

/**
 * opc：分页获取数据点位
 * @param {*} ticket
 * @param {*} page
 * @param {*} pageSize
 */
export function getDatasets(ticket: string, page: number, pageSize: number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/point/data/page?ticket=${ticket}&page=${page}&page_size=${pageSize}`,
    method: 'get'
  });
}

// 下载 csv 空模版
export function getCsvEmptyTemplate(driver: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/point/file/template?driver=${driver}&lang=${language}`,
    method: 'get',
    responseType: 'blob'
  });
}

// 获取表同步进度
export function getTableProgress(id: string | number, params: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${id}/table_progress?${params}`,
    method: 'get',
    transformResponse: [
      function (data) {
        try {
          return JSONbig.parse(data);
        } catch (error) {
          return data;
        }
      }
    ]
  });
}

//  获取 vgroup 消费进度
export function getVgroupProgress(id: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${id}/vgroup_progress`,
    method: 'get'
  });
}
// 校验 opc 点位合法性
export function validOpcFile(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/point/file/is_valid`,
    data,
    method: 'post'
  });
}

// 批量启动任务
export function batchStartTask(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/start`,
    method: 'post',
    data
  });
}
// 批量停止任务
export function batchStopTask(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/stop`,
    method: 'post',
    data
  });
}
// 批量删除任务
export function batchDelTask(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/delete`,
    method: 'post',
    data
  });
}

export function batchExportTask(ids: number[]) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/export?ids=${ids.join(',')}`,
    method: 'get',
    responseType: 'blob'
  });
}

export function importTask(data: Recordable) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/import`,
    method: 'post',
    data
  });
}

// 增加点位
export function addOpcPoint(data: Record<string, any>) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/opc/csv/points`,
    method: 'post',
    data
  });
}

// 查看点位配置csv Header
export function getOpcCsvHeader(taskId: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/ds/in/opc/csv/points/header?task_id=${taskId}`,
    method: 'get'
  });
}
