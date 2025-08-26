import { request } from '@/utils/request.ts';
import JSONbig from 'json-bigint';
import { getLocalTimezone, getLocalLang } from '@/utils';

const language = getLocalLang();
export function getTask(type: string) {
  const id = localStorage.getItem('local_clusterID');
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks?lang=${language}&detail=true&labels=type::${type},cluster-id::${id}`,
    method: 'get'
  });
}

export function getRunningTask() {
  const id = localStorage.getItem('local_clusterID');
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks?lang=${language}&detail=true&labels=type::datain,cluster-id::${id}&in_scheduler=true`,
    method: 'get'
  });
}

export function getUIData() {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in?lang=${language}`,
    method: 'get'
  });
}

export function generatePIDefaultConfigFile(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/download/pi_default_config`,
    method: 'post',
    data
  });
}

export function AddSource(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
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
    baseURL: import.meta.env.VITE_APP_X_API,
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
    baseURL: import.meta.env.VITE_APP_X_API,
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
    baseURL: import.meta.env.VITE_APP_X_API,
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

export function skip2Latest(id: string | number, recovery: boolean) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/kafka/${id}/seek_to_end`,
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

export function uploadFile() {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/upload`,
    method: 'post',
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  });
}

export function getCSVColumns(path: string, type: string, other?: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/filemeta?file_path=${path}&file_type=${type}${other ? '&' + other : ''}`,
    method: 'get',
    headers: {
      'Content-Type': 'multipart/form-data'
    }
  });
}

export function getAgentActivities(agentId: string | number) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/agents/${agentId}/activities`,
    method: 'get'
  });
}

export function getTaskActivities(taskId: string | number) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/${taskId}/activities`,
    method: 'get'
  });
}

export function getMetrics(taskId: string | number) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/${taskId}/metrics`,
    method: 'get'
  });
}

export function validateTask(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: '/ds/in/validate',
    method: 'post',
    data
  }).then(data => {
    if (data.code === 0) {
      return data;
    }
    return {
      valid: false,
      support: false,
      data_source: 'unknown',
      message: data.desc
    }
  });
}

export function getFileStream(filepath: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/download?file_path=${filepath}`,
    method: 'get',
    responseType: 'blob'
  });
}

export function downlaodAllNodes(data: string, agentid?: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
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
    baseURL: import.meta.env.VITE_APP_X_API,
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
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/metrics/description?lang=${language}`,
    method: 'get'
  });
}

export function getSampleDataMsgbody(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
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
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/transform/parser/plugins`,
    method: 'get'
  });
}

// 用模版的方式创建超级表的预览api
export function getStabelParser(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
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
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/point/file/download/task`,
    method: 'post',
    data
  });
}

// opc：检查数据点位模版文件是否准备好
export function checkReadyFile(ticket: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/point/file/are/you/ready?ticket=${ticket}`,
    method: 'get'
  });
}

// opc：下载数据点位模版csv文件
export function downlaodOpcPointFile(ticket: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/point/file/async?ticket=${ticket}`,
    method: 'get',
    responseType: 'blob'
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
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/point/data/page?ticket=${ticket}&page=${page}&page_size=${pageSize}`,
    method: 'get'
  });
}

// 下载 csv 空模版
export function getCsvEmptyTemplate(driver: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/point/file/template?driver=${driver}&lang=${language}`,
    method: 'get',
    responseType: 'blob'
  });
}

// 获取表同步进度
export function getTableProgress(id: string | number, params: string) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
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
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/${id}/vgroup_progress`,
    method: 'get'
  });
}
// 校验 opc 点位合法性
export function validOpcFile(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/point/file/is_valid`,
    data,
    method: 'post'
  });
}

// 批量启动任务
export function batchStartTask(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/start`,
    method: 'post',
    data
  });
}
// 批量停止任务
export function batchStopTask(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/stop`,
    method: 'post',
    data
  });
}
// 批量删除任务
export function batchDelTask(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/delete`,
    method: 'post',
    data
  });
}


export function batchExportTask(ids: number[]) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/export?ids=${ids.join(',')}`,
    method: 'get',
    responseType: 'blob'
  });
}

export function importTask(data: Recordable) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/tasks/import`,
    method: 'post',
    data
  });
}

// 增加点位
export function addOpcPoint(data: Record<string, any>) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/opc/csv/points`,
    method: 'post',
    data
  });
}

// 查看点位配置csv Header
export function getOpcCsvHeader(taskId: string | number) {
  return request({
    baseURL: import.meta.env.VITE_APP_X_API,
    url: `/ds/in/opc/csv/points/header?task_id=${taskId}`,
    method: 'get'
  });
}
