import { ref } from 'vue';
import { defineStore } from 'pinia';
import { getBackupList, getBackupHistory } from '@/api/backup';
import { parsinginZone } from '@/utils/index';
import { getDBListReq } from '@/api/database';

// 兼容旧版本创建的备份计划，旧版本中 compression_level 设置为 none
const parseBackup = (data: any) => {
  const targetData: any = {};
  targetData.id = data.id;
  targetData['database'] = data.from.split('/').at(-1);
  const params_start = targetData['database'].indexOf('?');
  if (params_start > 0) {
    targetData['database'] = targetData['database'].substring(0, params_start);
  }

  targetData.status = data.status;
  targetData.last_modified_at = data.last_modified_at;
  targetData.reason = data.reason;

  targetData.stable = data.from_expand.params?.stable || '';
  targetData.upcoming = data.trigger.upcoming;
  targetData.running = data.status !== 'stopped';

  targetData.interval = data.trigger.interval;
  targetData.max_size = data.to_expand.params?.max_size || '1GB';

  targetData.directory = data.to_expand.path;
  targetData.max_retry = data.from_expand.params?.max_retry || 3;
  const retry_interval_part = data.from_expand.params?.retry_interval.match(/^(\d+)s$/) || ['5', 's'];
  if (retry_interval_part && retry_interval_part.length === 2) {
    targetData.retry_interval = retry_interval_part[1];
  }
  targetData.backup_max_size = targetData.max_size;
  targetData.compression_level = data.to_expand.params?.compression_level || 'none';
  targetData.created_at = parsinginZone(data.created_at);
  targetData.s3_enable = data.to_expand.params?.s3_enable === "true";
  if (targetData.s3_enable) {
    targetData.s3_endpoint = data.to_expand.params?.s3_endpoint;
    targetData.s3_bucket = data.to_expand.params?.s3_bucket;
    targetData.s3_access_key_id = data.to_expand.params?.s3_access_key_id;
    targetData.s3_secret_access_key = data.to_expand.params?.s3_secret_access_key;
    targetData.s3_region = data.to_expand.params?.s3_region;
    targetData.s3_object_prefix = data.to_expand.params?.s3_object_prefix;
    targetData.backup_retention_size = parseInt(data.to_expand.params?.backup_retention_size || '10');
    const backup_retention_period_part = data.to_expand.params?.backup_retention_period?.match(/^(\d+)([hd])$/) || ['', 'd'];
    if (backup_retention_period_part && backup_retention_period_part.length === 3) {
      targetData.backup_retention_period_value = backup_retention_period_part[1];
      targetData.backup_retention_period_unit = backup_retention_period_part[2];
    }
  }
  return targetData;
};

const parseRestore = data => {
  const targetData: any = {};
  targetData.id = data.id;
  targetData.from_path = data.from_expand.path;
  targetData.from_point_start = data.from_expand.params.from;
  targetData.from_point_end = data.from_expand.params.to;
  targetData.to_database = data.to_expand.subject;
  targetData.status = data.status;
  targetData.last_modified_at = data.last_modified_at;
  targetData.reason = data.reason;
  targetData.created_at = parsinginZone(data.created_at);
  return targetData;
};

export const useBackupStore = defineStore('backup', () => {
  // state
  const backupPlanList = ref<any>([]);
  const restoreList = ref<any>([]);
  const backupPlanLoading = ref(false);

  const historyPlanId = ref('');
  const historyList = ref<any>([]);

  const dbList = ref<any>([]);

  const getBackupPlanList = async () => {
    try {
      backupPlanLoading.value = true;
      const res = await getBackupList('backup');
      backupPlanList.value = res.map(item => parseBackup(item));
      // console.log('topicList', JSON.stringify(topicList));
      backupPlanLoading.value = false;
    } catch (error) {
      return Promise.reject(error);
    }
  };

  const getRestoreList = async () => {
    try {
      backupPlanLoading.value = true;
      const res = await getBackupList('restore');
      restoreList.value = res.map((data: any) => parseRestore(data));
      backupPlanLoading.value = false;
    } catch (error) {
      return Promise.reject(error);
    }
  };

  const initDatabaseList = async () => {
    dbList.value = await getDBListReq();
  };

  const getHistoryList = async (id: string) => {
    const res = await getBackupHistory(id);
    console.log('getHistoryList', res);
    if (res && res.code > 0) {
      throw new Error(res.message);
    }
    historyList.value = res;
  };

  const setHistoryPlanId = (id: string) => {
    historyPlanId.value = id;
  };

  return {
    backupPlanList,
    restoreList,
    dbList,
    backupPlanLoading,
    historyPlanId,
    historyList,
    getBackupPlanList,
    getRestoreList,
    setHistoryPlanId,
    getHistoryList,
    initDatabaseList
  };
});
