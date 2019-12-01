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

#include "taosmsg.h"
#include "tsdb.h"
#include "tstatus.h"

const char* taosGetVgroupStatusStr(int vgroupStatus) {
  switch (vgroupStatus) {
    case TSDB_VG_STATUS_READY:                 return "ready";
    case TSDB_VG_STATUS_IN_PROGRESS:           return "inprogress";
    case TSDB_VG_STATUS_COMMITLOG_INIT_FAILED: return "commitlog_init_failed";
    case TSDB_VG_STATUS_INIT_FAILED:           return "init_failed";
    case TSDB_VG_STATUS_FULL:                  return "full";
    default:                                   return "undefined";
  }
}

const char* taosGetDbStatusStr(int dbStatus) {
  switch (dbStatus) {
    case TSDB_DB_STATUS_READY:         return "ready";
    case TSDB_DB_STATUS_DROPPING:      return "dropping";
    case TSDB_DB_STATUS_DROP_FROM_SDB: return "drop_from_sdb";
    default:                           return "undefined";
  }
}

const char* taosGetVnodeStatusStr(int vnodeStatus) {
  switch (vnodeStatus) {
    case TSDB_VN_STATUS_OFFLINE:  return "offline";
    case TSDB_VN_STATUS_CREATING: return "creating";
    case TSDB_VN_STATUS_UNSYNCED: return "unsynced";
    case TSDB_VN_STATUS_SLAVE:    return "slave";
    case TSDB_VN_STATUS_MASTER:   return "master";
    case TSDB_VN_STATUS_CLOSING:  return "closing";
    case TSDB_VN_STATUS_DELETING: return "deleting";
    default:                      return "undefined";
  }
}

const char* taosGetVnodeSyncStatusStr(int vnodeSyncStatus) {
  switch (vnodeSyncStatus) {
    case TSDB_VN_SYNC_STATUS_INIT:       return "init";
    case TSDB_VN_SYNC_STATUS_SYNCING:    return "syncing";
    case TSDB_VN_SYNC_STATUS_SYNC_CACHE: return "sync_cache";
    case TSDB_VN_SYNC_STATUS_SYNC_FILE:  return "sync_file";
    default:                             return "undefined";
  }
}

const char* taosGetVnodeDropStatusStr(int dropping) {
  switch (dropping) {
    case TSDB_VN_DROP_STATUS_READY:     return "ready";
    case TSDB_VN_DROP_STATUS_DROPPING:  return "dropping";
    default:                            return "undefined";
  }
}

const char* taosGetDnodeStatusStr(int dnodeStatus) {
  switch (dnodeStatus) {
    case TSDB_DN_STATUS_OFFLINE: return "offline";
    case TSDB_DN_STATUS_READY:   return "ready";
    default:                     return "undefined";
  }
}

const char* taosGetDnodeLbStatusStr(int dnodeBalanceStatus) {
  switch (dnodeBalanceStatus) {
    case TSDB_DN_LB_STATUS_BALANCED:         return "balanced";
    case TSDB_DN_LB_STATUS_BALANCING:        return "balancing";
    case TSDB_DN_LB_STATUS_OFFLINE_REMOVING: return "offline removing";
    case TSDB_DN_LB_STATE_SHELL_REMOVING:    return "removing";
    default:                                 return "undefined";
  }
}

const char* taosGetVgroupLbStatusStr(int vglbStatus) {
  switch (vglbStatus) {
    case TSDB_VG_LB_STATUS_READY:   return "ready";
    case TSDB_VG_LB_STATUS_UPDATE:  return "updating";
    default:                        return "undefined";
  }
}

const  char* taosGetVnodeStreamStatusStr(int vnodeStreamStatus) {
  switch (vnodeStreamStatus) {
    case TSDB_VN_STREAM_STATUS_START: return "start";
    case TSDB_VN_STREAM_STATUS_STOP:  return "stop";
    default:                          return "undefined";
  }
}
