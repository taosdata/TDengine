/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_TSTATUS_H
#define TDENGINE_TSTATUS_H

#ifdef __cplusplus
extern "C" {
#endif

enum _TSDB_VG_STATUS {
  TSDB_VG_STATUS_READY,
  TSDB_VG_STATUS_IN_PROGRESS,
  TSDB_VG_STATUS_COMMITLOG_INIT_FAILED,
  TSDB_VG_STATUS_INIT_FAILED,
  TSDB_VG_STATUS_FULL
};

enum _TSDB_DB_STATUS {
  TSDB_DB_STATUS_READY,
  TSDB_DB_STATUS_DROPPING,
  TSDB_DB_STATUS_DROP_FROM_SDB
};

enum _TSDB_VN_STATUS {
  TSDB_VN_STATUS_OFFLINE,
  TSDB_VN_STATUS_CREATING,
  TSDB_VN_STATUS_UNSYNCED,
  TSDB_VN_STATUS_SLAVE,
  TSDB_VN_STATUS_MASTER,
  TSDB_VN_STATUS_CLOSING,
  TSDB_VN_STATUS_DELETING,
};

enum _TSDB_VN_SYNC_STATUS {
  TSDB_VN_SYNC_STATUS_INIT,
  TSDB_VN_SYNC_STATUS_SYNCING,
  TSDB_VN_SYNC_STATUS_SYNC_CACHE,
  TSDB_VN_SYNC_STATUS_SYNC_FILE
};

enum _TSDB_VN_DROP_STATUS {
  TSDB_VN_DROP_STATUS_READY,
  TSDB_VN_DROP_STATUS_DROPPING
};

enum _TSDB_DN_STATUS {
  TSDB_DN_STATUS_OFFLINE,
  TSDB_DN_STATUS_READY
};

enum _TSDB_DN_LB_STATUS {
  TSDB_DN_LB_STATUS_BALANCED,
  TSDB_DN_LB_STATUS_BALANCING,
  TSDB_DN_LB_STATUS_OFFLINE_REMOVING,
  TSDB_DN_LB_STATE_SHELL_REMOVING
};

enum _TSDB_VG_LB_STATUS {
  TSDB_VG_LB_STATUS_READY,
  TSDB_VG_LB_STATUS_UPDATE
};

enum _TSDB_VN_STREAM_STATUS {
  TSDB_VN_STREAM_STATUS_STOP,
  TSDB_VN_STREAM_STATUS_START
};

const char* taosGetVgroupStatusStr(int vgroupStatus);
const char* taosGetDbStatusStr(int dbStatus);
const char* taosGetVnodeStatusStr(int vnodeStatus);
const char* taosGetVnodeSyncStatusStr(int vnodeSyncStatus);
const char* taosGetVnodeDropStatusStr(int dropping);
const char* taosGetDnodeStatusStr(int dnodeStatus);
const char* taosGetDnodeLbStatusStr(int dnodeBalanceStatus);
const char* taosGetVgroupLbStatusStr(int vglbStatus);
const char* taosGetVnodeStreamStatusStr(int vnodeStreamStatus);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSTATUS_H
