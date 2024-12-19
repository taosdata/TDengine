import {request} from "@/utils/request";
import i18n from '@/lang/index'
import {decrypt} from '@/utils';

let language = i18n.locale.includes('zh') ? 'zh' : 'en'

//获取backup列表
export function getBackupList(id, type) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/tasks?lang=${language}&detail=true&labels=type::${type},cluster-id::${id}`,
    method: "get"
  });
}

export function getBackupHistory(id) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/backup/${id}/points`,
    method: "get"
  });
}

export function restoreBackups(restoreData) {
  const username = localStorage.getItem("username") || ''
  const decryptPwd = decrypt(localStorage.getItem("pwd")) || '';

  let base_url = localStorage.getItem("base_url")
  let splitArr = base_url.split('//')
  let to = `tmq+${splitArr[0]}//${username}:${encodeURIComponent(decryptPwd)}@${splitArr[1]}/${restoreData.database}`;
  let from = `local:${restoreData.backupDirectory}?task_id=${restoreData.point.task_id}&topic=${restoreData.point.topic}&from=${restoreData.from}&to=${restoreData.to}&db_name=${restoreData.point.db_name}&db_sql=${restoreData.point.db_sql}`;
  if (restoreData.point.stable_name) {
    from += `&stable_name=${restoreData.point.stable_name}&stable_sql=${restoreData.point.stable_sql}`;
  }

  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/tasks`,
    method: "post",
    data: {
      "labels": ["type::restore", `cluster-id::${localStorage.getItem("local_clusterID")}`],
      "trigger": {"schedule": "oneshot", "resume": "never"},
      from,
      to,
    }
  });
}

//添加backup
export function addBackupData(data) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    headers: {
      "Content-Type": "application/json"
    },
    url: `/tasks?lang=${language}`,
    method: "post",
    data
  });
}

//编辑backup
export function editBackup(id, data) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/tasks/${id}`,
    method: "patch",
    data
  });
}

//删除backup
export function deleteBackup(id) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    url: `/tasks/${id}`,
    method: "delete"
  });
}

//恢复backup
export function restorBackupData(clusterID, data) {
  return request({
    baseURL: process.env.VUE_APP_X_API,
    headers: {
      "Content-Type": "application/json"
    },
    url: `/tasks?lang=${language}&labels=type::restore,cluster-id::${clusterID}`,
    method: "post",
    data
  });
}
