import { request } from '@/utils/request.ts';
import { getLocalLang, decrypt } from '@/utils';
import pathDetector from '@/utils/pathDetector';

const language = getLocalLang();
//获取backup列表
export function getBackupList(id: string | number, type: string) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks?lang=${language}&detail=true&labels=type::${type},cluster-id::${id}`,
    method: 'get'
  });
}

export function getBackupHistory(id) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/backup/${id}/points`,
    method: 'get'
  });
}

//添加backup
// export function addBackupData(clusterID: string, data: Recordable) {
//   return request({
//     baseURL: pathDetector.getXApiBasePath(),
//     headers: {
//       'Content-Type': 'application/json'
//     },
//     url: `/tasks?lang=${language}&labels=type::backup,cluster-id::${clusterID}`,
//     method: 'post',
//     data
//   });
// }

//编辑backup
export function editBackup(id: string | number, data: Recordable) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${id}`,
    method: 'patch',
    data
  });
}

//删除backup
export function deleteBackup(id: string | number) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks/${id}`,
    method: 'delete'
  });
}

export function restoreBackups(restoreData: any) {
  const username = localStorage.getItem('username') || '';
  const decryptPwd = decrypt(localStorage.getItem('pwd') || 'taosdata') || '';

  const base_url = localStorage.getItem('base_url') || '';
  const splitArr = base_url.split('//');
  const to = `tmq+${splitArr[0]}//${username}:${encodeURIComponent(decryptPwd)}@${splitArr[1]}/${restoreData.database}`;
  let from = `local:${restoreData.backupDirectory}?${restoreData.s3Config}&task_id=${restoreData.point.task_id}&topic=${restoreData.point.topic}&from=${restoreData.from}&to=${restoreData.to}&db_name=${restoreData.point.db_name}&db_sql=${restoreData.point.db_sql}`;
  if (restoreData.point.stable_name) {
    from += `&stable_name=${restoreData.point.stable_name}&stable_sql=${restoreData.point.stable_sql}`;
  }

  return request({
    baseURL: pathDetector.getXApiBasePath(),
    url: `/tasks`,
    method: 'post',
    data: {
      labels: ['type::restore', `cluster-id::${localStorage.getItem('local_clusterID')}`],
      trigger: { schedule: 'oneshot', resume: 'never' },
      from,
      to
    }
  });
}

//恢复backup
export function restoreBackupData(clusterID: string, data: Recordable) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    headers: {
      'Content-Type': 'application/json'
    },
    url: `/tasks?lang=${language}&labels=type::restore,cluster-id::${clusterID}`,
    method: 'post',
    data
  });
}

//添加backup
export function addBackupData(data) {
  return request({
    baseURL: pathDetector.getXApiBasePath(),
    headers: {
      'Content-Type': 'application/json'
    },
    url: `/tasks?lang=${language}`,
    method: 'post',
    data
  }).then(res => {
    if (res.code === 65535 && res.message) {
      return Promise.reject(res.message);
    } else {
      return res;
    }
  });
}
