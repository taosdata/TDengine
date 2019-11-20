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

const char* taosGetVnodeStatusStr(int vnodeStatus) {
  switch (vnodeStatus) {
    case TSDB_VNODE_STATUS_OFFLINE:return "offline";
    case TSDB_VNODE_STATUS_CREATING: return "creating";
    case TSDB_VNODE_STATUS_UNSYNCED: return "unsynced";
    case TSDB_VNODE_STATUS_SLAVE: return "slave";
    case TSDB_VNODE_STATUS_MASTER: return "master";
    case TSDB_VNODE_STATUS_CLOSING: return "closing";
    case TSDB_VNODE_STATUS_DELETING: return "deleting";
    default: return "undefined";
  }
}

const char* taosGetDnodeStatusStr(int dnodeStatus) {
  switch (dnodeStatus) {
    case TSDB_DNODE_STATUS_OFFLINE: return "offline";
    case TSDB_DNODE_STATUS_READY: return "ready";
    default: return "undefined";
  }
}

const char* taosGetDnodeBalanceStateStr(int dnodeBalanceStatus) {
  switch (dnodeBalanceStatus) {
    case LB_DNODE_STATE_BALANCED: return "balanced";
    case LB_DNODE_STATE_BALANCING: return "balancing";
    case LB_DNODE_STATE_OFFLINE_REMOVING: return "offline removing";
    case LB_DNODE_STATE_SHELL_REMOVING: return "removing";
    default: return "undefined";
  }
}

const char* taosGetVnodeSyncStatusStr(int vnodeSyncStatus) {
  switch (vnodeSyncStatus) {
    case STDB_SSTATUS_INIT: return "init";
    case TSDB_SSTATUS_SYNCING: return "syncing";
    case TSDB_SSTATUS_SYNC_CACHE: return "sync_cache";
    case TSDB_SSTATUS_SYNC_FILE: return "sync_file";
    default: return "undefined";
  }
}

const char* taosGetVnodeDropStatusStr(int dropping) {
  switch (dropping) {
    case 0: return "ready";
    case 1: return "dropping";
    default: return "undefined";
  }
}