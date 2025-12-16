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

#define _DEFAULT_SOURCE
#include "tpriv.h"
#include "tmsg.h"
#include "tutil.h"

static TdThreadOnce privInit = PTHREAD_ONCE_INIT;

static const char* privObjTypeNames[] = {
    "CLUSTER", "NODE", "DATABASE", "TABLE", "FUNCTION", "INDEX", "VIEW",  "USER",
    "ROLE",    "RSMA", "TSMA",     "TOPIC", "STREAM",   "MOUNT", "AUDIT", "TOKEN",
};

/**
 * N.B. increase the macro PRIV_INFO_TABLE_VERSION for any update of privInfoTable
 */

#define SYS_ADMIN_BASIC_ROLES (ROLE_SYSDBA | ROLE_SYSSEC | ROLE_SYSAUDIT)
#define SYS_ADMIN_INFO1_ROLES (SYS_ADMIN_BASIC_ROLES | ROLE_SYSINFO_1)
#define SYS_ADMIN_INFO_ROLES  (SYS_ADMIN_BASIC_ROLES | ROLE_SYSINFO_0 | ROLE_SYSINFO_1)
#define SYS_ADMIN_ALL_ROLES   (SYS_ADMIN_BASIC_ROLES | ROLE_SYSAUDIT_LOG | ROLE_SYSINFO_0 | ROLE_SYSINFO_1)

static SPrivInfo privInfoTable[] = {
    // ==================== system privileges ====================
    // Database Management
    {PRIV_DB_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "CREATE DATABASE"},
    {PRIV_DB_DROP_OWNED, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "DROP OWNED DATABASE"},
    {PRIV_VG_BALANCE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "BALANCE VGROUP"},
    {PRIV_VG_BALANCE_LEADER, PRIV_CATEGORY_OBJECT, 0, 0, ROLE_SYSDBA, "BALANCE VGROUP LEADER"},
    {PRIV_VG_MERGE, PRIV_CATEGORY_OBJECT, 0, 0, ROLE_SYSDBA, "MERGE VGROUP"},
    {PRIV_VG_REDISTRIBUTE, PRIV_CATEGORY_OBJECT, 0, 0, ROLE_SYSDBA, "REDISTRIBUTE VGROUP"},
    {PRIV_VG_SPLIT, PRIV_CATEGORY_OBJECT, 0, 0, ROLE_SYSDBA, "SPLIT VGROUP"},

    // Function Privileges
    {PRIV_FUNC_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "CREATE FUNCTION"},
    {PRIV_FUNC_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "DROP FUNCTION"},
    {PRIV_FUNC_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW FUNCTIONS"},

    // Mount Privileges
    {PRIV_MOUNT_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "CREATE MOUNT"},
    {PRIV_MOUNT_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "DROP MOUNT"},
    {PRIV_MOUNT_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW MOUNTS"},

    // User Management
    {PRIV_USER_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "CREATE USER"},
    {PRIV_USER_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "DROP USER"},
    {PRIV_USER_SET_SECURITY, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "SET USER SECURITY INFO"},
    {PRIV_USER_SET_AUDIT, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSAUDIT, "SET USER AUDIT INFO"},
    {PRIV_USER_SET_BASIC, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "SET USER BASIC INFO"},
    {PRIV_USER_UNLOCK, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "UNLOCK USER"},
    {PRIV_USER_LOCK, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "LOCK USER"},
    {PRIV_USER_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW USERS"},

    // Role Management
    {PRIV_ROLE_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "CREATE ROLE"},
    {PRIV_ROLE_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "DROP ROLE"},
    {PRIV_ROLE_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW ROLES"},

    // Token Privileges
    {PRIV_TOKEN_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "CREATE TOKEN"},
    {PRIV_TOKEN_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "DROP TOKEN"},
    {PRIV_TOKEN_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "ALTER TOKEN"},
    {PRIV_TOKEN_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_BASIC_ROLES, "SHOW TOKENS"},

    // Node Management
    {PRIV_NODE_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "CREATE NODE"},
    {PRIV_NODE_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "DROP NODE"},
    {PRIV_NODES_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW NODES"},

    // System Variables
    {PRIV_VAR_SECURITY_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "ALTER SECURITY VARIABLE"},
    {PRIV_VAR_AUDIT_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSAUDIT, "ALTER AUDIT VARIABLE"},
    {PRIV_VAR_SYSTEM_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "ALTER SYSTEM VARIABLE"},
    {PRIV_VAR_DEBUG_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "ALTER DEBUG VARIABLE"},
    {PRIV_VAR_SECURITY_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW SECURITY VARIABLE"},
    {PRIV_VAR_AUDIT_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW AUDIT VARIABLE"},
    {PRIV_VAR_SYSTEM_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW SYSTEM VARIABLE"},
    {PRIV_VAR_DEBUG_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW DEBUG VARIABLE"},

    // Key/Password Management
    {PRIV_KEY_UPDATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "UPDATE KEY"},
    {PRIV_TOTP_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "CREATE TOTP"},
    {PRIV_TOTP_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "DROP TOTP"},
    {PRIV_TOTP_UPDATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "UPDATE TOTP"},
    {PRIV_PASS_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "ALTER PASS"},
    {PRIV_PASS_ALTER_SELF, PRIV_CATEGORY_SYSTEM, 0, 0, 0, "ALTER SELF PASS"},

    // Grant/Revoke Privileges
    {PRIV_GRANT_PRIVILEGE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "GRANT PRIVILEGE"},
    {PRIV_REVOKE_PRIVILEGE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "REVOKE PRIVILEGE"},
    {PRIV_SHOW_PRIVILEGES, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW PRIVILEGES"},
    {PRIV_GRANT_SYSDBA, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "GRANT SYSDBA PRIVILEGE"},
    {PRIV_REVOKE_SYSDBA, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "REVOKE SYSDBA PRIVILEGE"},
    {PRIV_GRANT_SYSSEC, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "GRANT SYSSEC PRIVILEGE"},
    {PRIV_REVOKE_SYSSEC, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSSEC, "REVOKE SYSSEC PRIVILEGE"},
    {PRIV_GRANT_SYSAUDIT, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSAUDIT, "GRANT SYSAUDIT PRIVILEGE"},
    {PRIV_REVOKE_SYSAUDIT, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSAUDIT, "REVOKE SYSAUDIT PRIVILEGE"},

    // Audit Management
    {PRIV_AUDIT_DB_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "CREATE AUDIT DATABASE"},
    {PRIV_AUDIT_DB_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "DROP AUDIT DATABASE"},
    {PRIV_AUDIT_DB_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "ALTER AUDIT DATABASE"},
    {PRIV_AUDIT_DB_USE, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSAUDIT | ROLE_SYSAUDIT_LOG, "USE AUDIT DATABASE"},

    // System Administration
    {PRIV_TRANS_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW TRANS"},
    {PRIV_TRANS_KILL, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "KILL TRANS"},
    {PRIV_CONNECTION_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW CONNECTIONS"},
    {PRIV_CONNECTION_KILL, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "KILL CONNECTION"},
    {PRIV_QUERY_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW QUERIES"},
    {PRIV_QUERY_KILL, PRIV_CATEGORY_SYSTEM, 0, 0, ROLE_SYSDBA, "KILL QUERY"},
    {PRIV_GRANTS_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW GRANTS"},
    {PRIV_CLUSTER_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW CLUSTER"},
    {PRIV_APPS_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW APPS"},

    // ==================== object privileges ====================
    // Database Privileges
    {PRIV_DB_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "ALTER DATABASE"},
    {PRIV_DB_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "DROP DATABASE"},
    {PRIV_DB_USE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_BASIC_ROLES, "USE DATABASE"},
    {PRIV_DB_FLUSH, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "FLUSH DATABASE"},
    {PRIV_DB_COMPACT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "COMPACT DATABASE"},
    {PRIV_DB_TRIM, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "TRIM DATABASE"},
    {PRIV_DB_ROLLUP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "ROLLUP DATABASE"},
    {PRIV_DB_SCAN, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "SCAN DATABASE"},
    {PRIV_DB_SSMIGRATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA, "SSMIGRATE DATABASE"},
    {PRIV_SHOW_DATABASES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO_ROLES, "SHOW DATABASES"},
    {PRIV_SHOW_VNODES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW VNODES"},
    {PRIV_SHOW_VGROUPS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW VGROUPS"},
    {PRIV_SHOW_COMPACTS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW COMPACTS"},
    {PRIV_SHOW_RETENTIONS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW RETENTIONS"},
    {PRIV_SHOW_SCANS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW SCANS"},
    {PRIV_SHOW_SSMIGRATES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW SSMIGRATES"},

    // Table Privileges
    {PRIV_TBL_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, ROLE_SYSDBA | ROLE_SYSAUDIT_LOG, "CREATE TABLE"},
    {PRIV_TBL_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "DROP TABLE"},
    {PRIV_TBL_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "ALTER TABLE"},
    {PRIV_TBL_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, SYS_ADMIN_INFO_ROLES, "SHOW TABLES"},
    {PRIV_TBL_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, SYS_ADMIN_INFO1_ROLES, "SHOW CREATE TABLE"},
    {PRIV_TBL_SELECT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "SELECT TABLE"},
    {PRIV_TBL_INSERT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "INSERT TABLE"},
    {PRIV_TBL_UPDATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "UPDATE TABLE"},
    {PRIV_TBL_DELETE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "DELETE TABLE"},

    // Index Privileges
    {PRIV_IDX_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "CREATE INDEX"},
    {PRIV_IDX_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_INDEX, 1, ROLE_SYSDBA, "DROP INDEX"},
    {PRIV_IDX_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_INDEX, 1, SYS_ADMIN_INFO_ROLES, "SHOW INDEXES"},

    // RSMA Privileges
    {PRIV_RSMA_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "CREATE RSMA"},
    {PRIV_RSMA_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, ROLE_SYSDBA, "DROP RSMA"},
    {PRIV_RSMA_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, ROLE_SYSDBA, "ALTER RSMA"},
    {PRIV_RSMA_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, SYS_ADMIN_INFO_ROLES, "SHOW RSMAS"},
    {PRIV_RSMA_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE RSMA"},

    // TSMA Privileges
    {PRIV_TSMA_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "CREATE TSMA"},
    {PRIV_TSMA_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, 1, ROLE_SYSDBA, "DROP TSMA"},
    {PRIV_TSMA_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, 1, SYS_ADMIN_INFO_ROLES, "SHOW TSMAS"},

    // View Privileges
    {PRIV_VIEW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "CREATE VIEW"},
    {PRIV_VIEW_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, ROLE_SYSDBA, "DROP VIEW"},
    {PRIV_VIEW_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, SYS_ADMIN_INFO_ROLES, "SHOW VIEWS"},
    {PRIV_VIEW_READ, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, ROLE_SYSDBA, "READ VIEW"},

    // Topic Privileges
    {PRIV_TOPIC_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "CREATE TOPIC"},
    {PRIV_TOPIC_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, ROLE_SYSDBA, "DROP TOPIC"},
    {PRIV_TOPIC_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, SYS_ADMIN_INFO_ROLES, "SHOW TOPICS"},
    {PRIV_CONSUMER_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, SYS_ADMIN_INFO_ROLES, "SHOW CONSUMERS"},
    {PRIV_SUBSCRIPTION_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, SYS_ADMIN_INFO_ROLES, "SHOW SUBSCRIPTIONS"},

    // Stream Privileges
    {PRIV_STREAM_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, 1, ROLE_SYSDBA, "CREATE STREAM"},
    {PRIV_STREAM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, ROLE_SYSDBA, "DROP STREAM"},
    {PRIV_STREAM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, SYS_ADMIN_INFO_ROLES, "SHOW STREAMS"},
    {PRIV_STREAM_START, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, ROLE_SYSDBA, "START STREAM"},
    {PRIV_STREAM_STOP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, ROLE_SYSDBA, "STOP STREAM"},
    {PRIV_STREAM_RECALC, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, ROLE_SYSDBA, "RECALC STREAM"},

    // ==================== legacy privileges ====================
    {PRIV_TYPE_ALL, PRIV_CATEGORY_LEGACY, 0, 0, 0, "ALL PRIVILEGES"},
    {PRIV_TYPE_READ, PRIV_CATEGORY_LEGACY, 0, 0, 0, "READ PRIVILEGE"},
    {PRIV_TYPE_WRITE, PRIV_CATEGORY_LEGACY, 0, 0, 0, "WRITE PRIVILEGE"},
    {PRIV_TYPE_SUBSCRIBE, PRIV_CATEGORY_LEGACY, 0, 0, 0, "SUBSCRIBE PRIVILEGE"},
    {PRIV_TYPE_ALTER, PRIV_CATEGORY_LEGACY, 0, 0, 0, "ALTER PRIVILEGE"},
};

static SPrivInfo* privLookup[MAX_PRIV_TYPE + 1] = {0};

static void initPrivLookup(void) {
  for (size_t i = 0; i < sizeof(privInfoTable) / sizeof(privInfoTable[0]); ++i) {
    if (privInfoTable[i].privType <= MAX_PRIV_TYPE) {
      privLookup[privInfoTable[i].privType] = &privInfoTable[i];
    }
  }
}

int32_t checkPrivConflicts(const SPrivSet* privSet, EPrivCategory* pCategory, EPrivObjType* pObjType,
                           uint8_t* pObjLevel, EPrivType* conflict0, EPrivType* conflict1) {
  (void)taosThreadOnce(&privInit, initPrivLookup);

  if (!privSet) return TSDB_CODE_PAR_INTERNAL_ERROR;

  int32_t          code = 0;
  const SPrivInfo* privInfo = NULL;

  bool hasSystemPriv = false;
  bool hasObjectPriv = false;
  bool hasLegacyPriv = false;

  EPrivType    lastPriv = PRIV_TYPE_UNKNOWN;
  EPrivObjType objectType = PRIV_OBJ_UNKNOWN;
  uint8_t      objectLevel = -1;

  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    uint64_t chunk = privSet->set[i];
    if (chunk == 0) continue;

    while (chunk != 0) {
      int32_t   bitPos = BUILDIN_CTZL(chunk);
      EPrivType privType = (i << 6) + bitPos;
      chunk &= ~(1ULL << bitPos);

      privInfo = privLookup[privType];
      if (privInfo == NULL) continue;

      switch (privInfo->category) {
        case PRIV_CATEGORY_SYSTEM: {
          if (hasObjectPriv || hasLegacyPriv) {
            code = 1;
            goto _exit;
          }
          hasSystemPriv = true;
          lastPriv = privInfo->privType;
          break;
        }
        case PRIV_CATEGORY_OBJECT: {
          if (hasSystemPriv || hasLegacyPriv) {
            code = 1;
            goto _exit;
          }
          if (objectType == PRIV_OBJ_UNKNOWN) {
            objectType = privInfo->objType;
            objectLevel = privInfo->objLevel;
          } else if (objectType != privInfo->objType) {
            code = 2;
            goto _exit;
          } else if (objectLevel != privInfo->objLevel) {
            code = 3;
            goto _exit;
          }
          hasObjectPriv = true;
          lastPriv = privInfo->privType;
          break;
        }
        case PRIV_CATEGORY_LEGACY: {
          if (hasSystemPriv || hasObjectPriv) {
            code = 1;
            goto _exit;
          }
          if (objectType == PRIV_OBJ_UNKNOWN) {
            objectType = privInfo->objType;
            objectLevel = privInfo->objLevel;
          } else if (objectType != privInfo->objType) {
            code = 2;
            goto _exit;
          } else if (objectLevel != privInfo->objLevel) {
            code = 3;
            goto _exit;
          }
          hasLegacyPriv = true;
          lastPriv = privInfo->privType;
          break;
        }
        default:
          break;
      }
    }
  }

_exit:
  if (code != 0) {
    if (conflict0) *conflict0 = lastPriv;
    if (conflict1 && privInfo) *conflict1 = privInfo->privType;
    return code;
  }

  if (pCategory) {
    *pCategory = hasSystemPriv   ? PRIV_CATEGORY_SYSTEM
                 : hasObjectPriv ? PRIV_CATEGORY_OBJECT
                 : hasLegacyPriv ? PRIV_CATEGORY_LEGACY
                                 : PRIV_CATEGORY_UNKNOWN;
  }
  if (pObjType) {
    *pObjType = objectType;
  }
  if (pObjLevel) {
    *pObjLevel = objectLevel;
  }

  return 0;
}

void privIterInit(SPrivIter* pIter, SPrivSet* privSet) {
  (void)taosThreadOnce(&privInit, initPrivLookup);
  pIter->privSet = privSet;
  pIter->groupIndex = 0;
  pIter->curPriv = privSet->set[0];
}

bool privIterNext(SPrivIter* iter, SPrivInfo** ppPrivInfo) {
_loop:
  while (iter->curPriv == 0) {
    if (++iter->groupIndex >= PRIV_GROUP_CNT) {
      return false;
    }
    iter->curPriv = iter->privSet->set[iter->groupIndex];
  }
  int32_t   bitPos = BUILDIN_CTZL(iter->curPriv);
  EPrivType privType = (iter->groupIndex << 6) + bitPos;
  iter->curPriv &= ~(1ULL << bitPos);
  if (ppPrivInfo) {
    *ppPrivInfo = privLookup[privType];
    if (!(*ppPrivInfo)) goto _loop;
  }
  return true;
}

void privInfoIterInit(SPrivInfoIter* pIter) {
  pIter->privInfo = privInfoTable;
  pIter->privInfoCnt = sizeof(privInfoTable) / sizeof(privInfoTable[0]);
  pIter->index = 0;
}

bool privInfoIterNext(SPrivInfoIter* iter, SPrivInfo** ppPrivInfo) {
  if (iter->index < iter->privInfoCnt) {
    if (ppPrivInfo) {
      *ppPrivInfo = &iter->privInfo[iter->index++];
    } else {
      iter->index++;
    }
    return true;
  }

  return false;
}
// objType.1.db.tb or objType.1.db
int32_t privObjKeyF(SPrivInfo* pPrivInfo, const char* fname, const char* tb, char* buf, int32_t bufLen) {
  return (pPrivInfo->objLevel == 0)
             ? snprintf(buf, bufLen, "%d.%s", pPrivInfo->objType, fname ? fname : "")
             : snprintf(buf, bufLen, "%d.%s.%s", pPrivInfo->objType, fname ? fname : "", tb ? tb : "");
}

int32_t privObjKey(SPrivInfo* pPrivInfo, int32_t acctId, const char* name, const char* tb, char* buf, int32_t bufLen) {
  return (pPrivInfo->objLevel == 0)
             ? snprintf(buf, bufLen, "%d.%d.%s", pPrivInfo->objType, acctId, name ? name : "")
             : snprintf(buf, bufLen, "%d.%d.%s.%s", pPrivInfo->objType, acctId, name ? name : "", tb ? tb : "");
}

// objType.1.db or objType.1.db.tb
int32_t privObjKeyParse(const char* str, EPrivObjType* pObjType, char* db, int32_t dbLen, char* tb, int32_t tbLen) {
  char* p = strchr(str, '.');
  if (!p) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  *pObjType = taosStr2Int32(str, NULL, 10);
  if (errno == ERANGE || *pObjType < PRIV_OBJ_CLUSTER || *pObjType >= PRIV_OBJ_MAX) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  char* pNext = strchr(p + 1, '.');
  if (!pNext) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  char* qNext = strchr(pNext + 1, '.');
  if (qNext) {
    size_t dbLength = qNext - (p + 1);
    if (dbLength >= (size_t)dbLen) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    strncpy(db, p + 1, dbLength);
    db[dbLength] = '\0';
    strncpy(tb, qNext + 1, tbLen);
  } else {
    strcpy(db, p + 1);
    tb[0] = '\0';
  }
  return TSDB_CODE_SUCCESS;
}

int32_t privTblKey(const char* db, const char* tb, char* buf, int32_t bufLen) {
  return snprintf(buf, bufLen, "%s.%s", db ? db : "", tb ? tb : "");
}

const char* privObjTypeName(EPrivObjType objType) {
  if (objType < PRIV_OBJ_CLUSTER || objType >= PRIV_OBJ_MAX) {
    return "UNKNOWN";
  }
  return privObjTypeNames[objType];
}

int32_t getSysRoleType(const char* roleName) {
  if (strcmp(roleName, TSDB_ROLE_SYSDBA) == 0) {
    return ROLE_SYSDBA;
  } else if (strcmp(roleName, TSDB_ROLE_SYSSEC) == 0) {
    return ROLE_SYSSEC;
  } else if (strcmp(roleName, TSDB_ROLE_SYSAUDIT) == 0) {
    return ROLE_SYSAUDIT;
  } else if (strcmp(roleName, TSDB_ROLE_SYSAUDIT_LOG) == 0) {
    return ROLE_SYSAUDIT_LOG;
  } else if (strcmp(roleName, TSDB_ROLE_SYSINFO_0) == 0) {
    return ROLE_SYSINFO_0;
  } else if (strcmp(roleName, TSDB_ROLE_SYSINFO_1) == 0) {
    return ROLE_SYSINFO_1;
  }
  return 0;
}

bool isPrivInheritName(const char* name) {
  if (name && (name[0] == 'S' || name[0] == 's')) {
    if (taosStrncasecmp(name, TSDB_ROLE_SYSDBA, 7) == 0 || taosStrncasecmp(name, TSDB_ROLE_SYSSEC, 7) == 0 ||
        taosStrncasecmp(name, TSDB_ROLE_SYSAUDIT, 9) == 0 || taosStrncasecmp(name, TSDB_ROLE_SYSAUDIT_LOG, 13) == 0 ||
        taosStrncasecmp(name, TSDB_ROLE_SYSINFO_0, 10) == 0 || taosStrncasecmp(name, TSDB_ROLE_SYSINFO_1, 10) == 0) {
      return true;
    }
  }
  return false;
}

int32_t privTblPolicyCopy(SPrivTblPolicy* dest, SPrivTblPolicy* src) {
  dest->policyId = src->policyId;
  dest->span[0] = src->span[0];
  dest->span[1] = src->span[1];
  if (src->tagCond) {
    if (!(dest->tagCond = taosStrdup(src->tagCond))) {
      return terrno;
    }
  } else {
    dest->tagCond = NULL;
  }
  dest->taghash = src->taghash;
  dest->colHash = src->colHash;
  if (src->cols) {
    dest->cols = taosArrayInit_s(sizeof(SColNameFlag), taosArrayGetSize(src->cols));
    if (dest->cols == NULL) {
      return terrno;
    }

    for (int32_t i = 0; i < TARRAY_SIZE(src->cols); ++i) {
      SColNameFlag* pSrcCol = (SColNameFlag*)TARRAY_GET_ELEM(src->cols, i);
      SColNameFlag* destCol = (SColNameFlag*)TARRAY_GET_ELEM(dest->cols, i);
      destCol->colId = pSrcCol->colId;
      (void)snprintf(destCol->colName, TSDB_COL_NAME_LEN, "%s", pSrcCol->colName);
    }
  } else {
    dest->cols = NULL;
  }
  return 0;
}

int32_t privTblPoliciesAdd(SPrivTblPolicies* dest, SPrivTblPolicies* src, bool deepCopy) {
  int32_t code = 0, lino = 0;
  dest->nPolicies += src->nPolicies;
  for (int32_t i = 0; i < PRIV_TBL_POLICY_MAX; ++i) {
    if (taosArrayGetSize(src->policy[i]) > 0) {
      if (deepCopy) {
        if (!dest->policy[i]) {
          dest->policy[i] = taosArrayInit(TARRAY_SIZE(src->policy[i]), sizeof(SPrivTblPolicy));
          if (!dest->policy[i]) {
            TAOS_CHECK_EXIT(terrno);
          }
        }
        for (int32_t j = 0; j < TARRAY_SIZE(src->policy[i]); ++j) {
          SPrivTblPolicy* pSrcPolicy = (SPrivTblPolicy*)TARRAY_GET_ELEM(src->policy[i], j);
          SPrivTblPolicy  destPolicy = {0};
          if ((code = privTblPolicyCopy(&destPolicy, pSrcPolicy))) {
            privTblPolicyFree(&destPolicy);
            TAOS_CHECK_EXIT(code);
          }
          if (!taosArrayPush(dest->policy[i], &destPolicy)) {
            privTblPolicyFree(&destPolicy);
            TAOS_CHECK_EXIT(terrno);
          }
        }
      } else {
        dest->policy[i] = src->policy[i];
      }
    } else {
      dest->policy[i] = NULL;
    }
  }
_exit:
  return code;
}

SPrivInfo* privInfoGet(EPrivType privType) {
  (void)taosThreadOnce(&privInit, initPrivLookup);
  SPrivInfo* result = (privType >= 0 && privType <= MAX_PRIV_TYPE) ? privLookup[privType] : NULL;
  if (!result) terrno = TSDB_CODE_APP_ERROR;
  return result;
}