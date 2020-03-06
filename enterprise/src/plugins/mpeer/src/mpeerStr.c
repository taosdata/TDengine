/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies.
 *  No part of this file may be reproduced, stored, transmitted,
 *  disclosed or used in any form or by any means other than as
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/
#define _DEFAULT_SOURCE
#include "sdb.h"
#include "mpeerStr.h"

const char *mpeerGetSdbRoleStr(int32_t role) {
  switch (role) {
    case SDB_ROLE_UNAPPROVED: return "unapproved";
    case SDB_ROLE_UNDECIDED:  return "undecided";
    case SDB_ROLE_MASTER:     return "master";
    case SDB_ROLE_SLAVE:      return "slave";
    default:                  return "undefined";
  }
}

const char *mpeerGetSdbStatusStr(int32_t status) {
  switch (status) {
    case SDB_STATUS_OFFLINE:  return "offline";
    case SDB_STATUS_UNSYNCED: return "unsynced";
    case SDB_STATUS_SYNCING:  return "syncing";
    case SDB_STATUS_SERVING:  return "serving";
    case SDB_STATUS_DELETED:  return "deleted";
    default:                  return "undefined";
  }
}

const char *mpeerGetSdbTableName(int32_t table) {
  switch (table) {
    case 0:  return "account";
    case 1:  return "user";
    case 2:  return "dnodes";
    case 3:  return "db";
    case 4:  return "vgroups";
    case 5:  return "meters";
    case 6:  return "mnode";
    default: return "undefined";
  }
}

const char *mpeerGetSdbOperName(int32_t oper) {
  switch (oper) {
    case SDB_TYPE_INSERT:              return "insert";
    case SDB_TYPE_DELETE:              return "delete";
    case SDB_TYPE_UPDATE:              return "update";
    case SDB_TYPE_DECODE:              return "decode";
    case SDB_TYPE_ENCODE:              return "encode";
    case SDB_TYPE_BEFORE_BATCH_UPDATE: return "before_batch_update";
    case SDB_TYPE_BATCH_UPDATE:        return "batch_update";
    case SDB_TYPE_AFTER_BATCH_UPDATE:  return "after_batch_update";
    case SDB_TYPE_RESET:               return "reset";
    case SDB_TYPE_DESTROY:             return "destroy";
    case SDB_MAX_ACTION_TYPES:         return "invalid";
    default:                           return "undefined";
  }
}