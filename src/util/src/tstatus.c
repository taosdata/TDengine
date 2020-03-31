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
#include "tstatus.h"

char* taosGetVgroupStatusStr(int32_t vgroupStatus) {
  switch (vgroupStatus) {
    case TSDB_VG_STATUS_READY:                 return (char*)tstrerror(vgroupStatus);
    case TSDB_VG_STATUS_IN_PROGRESS:           return (char*)tstrerror(vgroupStatus);
    case TSDB_VG_STATUS_NO_DISK_PERMISSIONS:   return (char*)tstrerror(vgroupStatus);
    case TSDB_VG_STATUS_SERVER_NO_PACE:        return (char*)tstrerror(vgroupStatus);
    case TSDB_VG_STATUS_SERV_OUT_OF_MEMORY:    return (char*)tstrerror(vgroupStatus);
    case TSDB_VG_STATUS_INIT_FAILED:           return (char*)tstrerror(vgroupStatus);
    case TSDB_VG_STATUS_FULL:                  return (char*)tstrerror(vgroupStatus);
    default:                                   return "undefined";
  }
}

char* taosGetDbStatusStr(int32_t dbStatus) {
  switch (dbStatus) {
    case TSDB_DB_STATUS_READY:         return "ready";
    case TSDB_DB_STATUS_DROPPING:      return "dropping";
    case TSDB_DB_STATUS_DROP_FROM_SDB: return "drop_from_sdb";
    default:                           return "undefined";
  }
}

char* taosGetVnodeStatusStr(int32_t vnodeStatus) {
  switch (vnodeStatus) {
    case TSDB_VN_STATUS_NOT_READY:return "not_ready";
    case TSDB_VN_STATUS_UNSYNCED: return "unsynced";
    case TSDB_VN_STATUS_SLAVE:    return "slave";
    case TSDB_VN_STATUS_MASTER:   return "master";
    case TSDB_VN_STATUS_CREATING: return "creating";
    case TSDB_VN_STATUS_CLOSING:  return "closing";
    case TSDB_VN_STATUS_DELETING: return "deleting";
    default:                      return "undefined";
  }
}

char* taosGetVnodeSyncStatusStr(int32_t vnodeSyncStatus) {
  switch (vnodeSyncStatus) {
    case TSDB_VN_SYNC_STATUS_INIT:       return "ready";
    case TSDB_VN_SYNC_STATUS_SYNCING:    return "syncing";
    case TSDB_VN_SYNC_STATUS_SYNC_CACHE: return "sync_cache";
    case TSDB_VN_SYNC_STATUS_SYNC_FILE:  return "sync_file";
    default:                             return "undefined";
  }
}

char* taosGetVnodeDropStatusStr(int32_t dropping) {
  switch (dropping) {
    case TSDB_VN_DROP_STATUS_READY:     return "ready";
    case TSDB_VN_DROP_STATUS_DROPPING:  return "dropping";
    default:                            return "undefined";
  }
}

char* taosGetDnodeStatusStr(int32_t dnodeStatus) {
  switch (dnodeStatus) {
    case TSDB_DN_STATUS_OFFLINE: return "offline";
    case TSDB_DN_STATUS_READY:   return "ready";
    default:                     return "undefined";
  }
}

char* taosGetDnodeLbStatusStr(int32_t dnodeBalanceStatus) {
  switch (dnodeBalanceStatus) {
    case TSDB_DN_LB_STATUS_BALANCED:         return "balanced";
    case TSDB_DN_LB_STATUS_BALANCING:        return "balancing";
    case TSDB_DN_LB_STATUS_OFFLINE_REMOVING: return "offline removing";
    case TSDB_DN_LB_STATE_SHELL_REMOVING:    return "removing";
    default:                                 return "undefined";
  }
}

char* taosGetVgroupLbStatusStr(int32_t vglbStatus) {
  switch (vglbStatus) {
    case TSDB_VG_LB_STATUS_READY:   return "ready";
    case TSDB_VG_LB_STATUS_UPDATE:  return "updating";
    default:                        return "undefined";
  }
}

char* taosGetVnodeStreamStatusStr(int32_t vnodeStreamStatus) {
  switch (vnodeStreamStatus) {
    case TSDB_VN_STREAM_STATUS_START: return "start";
    case TSDB_VN_STREAM_STATUS_STOP:  return "stop";
    default:                          return "undefined";
  }
}

char* taosGetTableStatusStr(int32_t tableStatus) {
  switch(tableStatus) {
    case TSDB_METER_STATE_INSERTING:return "inserting";
    case TSDB_METER_STATE_IMPORTING:return "importing";
    case TSDB_METER_STATE_UPDATING: return "updating";
    case TSDB_METER_STATE_DROPPING: return "deleting";
    case TSDB_METER_STATE_DROPPED:  return "dropped";
    case TSDB_METER_STATE_READY:    return "ready";
    default:return "undefined";
  }
}

char *taosGetShowTypeStr(int32_t showType) {
  switch (showType) {
    case TSDB_MGMT_TABLE_ACCT:    return "show accounts";
    case TSDB_MGMT_TABLE_USER:    return "show users";
    case TSDB_MGMT_TABLE_DB:      return "show databases";
    case TSDB_MGMT_TABLE_TABLE:   return "show tables";
    case TSDB_MGMT_TABLE_DNODE:   return "show dnodes";
    case TSDB_MGMT_TABLE_MNODE:   return "show mnodes";
    case TSDB_MGMT_TABLE_VGROUP:  return "show vgroups";
    case TSDB_MGMT_TABLE_METRIC:  return "show stables";
    case TSDB_MGMT_TABLE_MODULE:  return "show modules";
    case TSDB_MGMT_TABLE_QUERIES: return "show queries";
    case TSDB_MGMT_TABLE_STREAMS: return "show streams";
    case TSDB_MGMT_TABLE_CONFIGS: return "show configs";
    case TSDB_MGMT_TABLE_CONNS:   return "show connections";
    case TSDB_MGMT_TABLE_SCORES:  return "show scores";
    case TSDB_MGMT_TABLE_GRANTS:  return "show grants";
    case TSDB_MGMT_TABLE_VNODES:  return "show vnodes";
    default:                      return "undefined";
  }
}
