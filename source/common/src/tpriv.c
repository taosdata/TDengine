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

static TdThreadOnce privInit = PTHREAD_ONCE_INIT;

static SPrivInfo privInfoTable[] = {
    // ==================== system privileges ====================
    // Database Management
    {PRIV_DB_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE DATABASE"},
    {PRIV_VG_BALANCE, PRIV_CATEGORY_SYSTEM, 0, "BALANCE VGROUP"},

    // Function Privileges
    {PRIV_FUNC_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE FUNCTION"},
    {PRIV_FUNC_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP FUNCTION"},
    {PRIV_FUNC_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW FUNCTIONS"},

    // Mount Privileges
    {PRIV_MOUNT_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE MOUNT"},
    {PRIV_MOUNT_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP MOUNT"},
    {PRIV_MOUNT_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW MOUNTS"},

    // User Management
    {PRIV_USER_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE USER"},
    {PRIV_USER_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP USER"},
    {PRIV_USER_SET_SECURITY, PRIV_CATEGORY_SYSTEM, 0, "SET USER SECURITY INFO"},
    {PRIV_USER_SET_AUDIT, PRIV_CATEGORY_SYSTEM, 0, "SET USER AUDIT INFO"},
    {PRIV_USER_SET_BASIC, PRIV_CATEGORY_SYSTEM, 0, "SET USER BASIC INFO"},
    {PRIV_USER_ENABLE, PRIV_CATEGORY_SYSTEM, 0, "ENABLE USER"},
    {PRIV_USER_DISABLE, PRIV_CATEGORY_SYSTEM, 0, "DISABLE USER"},
    {PRIV_USER_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW USERS"},

    // Role Management
    {PRIV_ROLE_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE ROLE"},
    {PRIV_ROLE_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP ROLE"},
    {PRIV_ROLE_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW ROLES"},

    // Token Privileges
    {PRIV_TOKEN_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE TOKEN"},
    {PRIV_TOKEN_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP TOKEN"},
    {PRIV_TOKEN_ALTER, PRIV_CATEGORY_SYSTEM, 0, "ALTER TOKEN"},
    {PRIV_TOKEN_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW TOKENS"},

    // Node Management
    {PRIV_NODE_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE NODE"},
    {PRIV_NODE_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP NODE"},
    {PRIV_NODES_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW NODES"},

    // System Variables
    {PRIV_VAR_SECURITY_ALTER, PRIV_CATEGORY_SYSTEM, 0, "ALTER SECURITY VARIABLE"},
    {PRIV_VAR_AUDIT_ALTER, PRIV_CATEGORY_SYSTEM, 0, "ALTER AUDIT VARIABLE"},
    {PRIV_VAR_SYSTEM_ALTER, PRIV_CATEGORY_SYSTEM, 0, "ALTER SYSTEM VARIABLE"},
    {PRIV_VAR_DEBUG_ALTER, PRIV_CATEGORY_SYSTEM, 0, "ALTER DEBUG VARIABLE"},
    {PRIV_VAR_SECURITY_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW SECURITY VARIABLE"},
    {PRIV_VAR_AUDIT_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW AUDIT VARIABLE"},
    {PRIV_VAR_SYSTEM_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW SYSTEM VARIABLE"},
    {PRIV_VAR_DEBUG_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW DEBUG VARIABLE"},

    // Key/Password Management
    {PRIV_KEY_UPDATE, PRIV_CATEGORY_SYSTEM, 0, "UPDATE KEY"},
    {PRIV_TOTP_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE TOTP"},
    {PRIV_TOTP_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP TOTP"},
    {PRIV_TOTP_UPDATE, PRIV_CATEGORY_SYSTEM, 0, "UPDATE TOTP"},
    {PRIV_PASS_ALTER, PRIV_CATEGORY_SYSTEM, 0, "ALTER PASS"},
    {PRIV_PASS_ALTER_SELF, PRIV_CATEGORY_SYSTEM, 0, "ALTER SELF PASS"},

    // Audit Management
    {PRIV_AUDIT_DB_CREATE, PRIV_CATEGORY_SYSTEM, 0, "CREATE AUDIT DATABASE"},
    {PRIV_AUDIT_DB_DROP, PRIV_CATEGORY_SYSTEM, 0, "DROP AUDIT DATABASE"},
    {PRIV_AUDIT_DB_ALTER, PRIV_CATEGORY_SYSTEM, 0, "ALTER AUDIT DATABASE"},
    {PRIV_AUDIT_DB_READ, PRIV_CATEGORY_SYSTEM, 0, "USE AUDIT DATABASE"},

    // System Administration
    {PRIV_TRANS_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW TRANS"},
    {PRIV_TRANS_KILL, PRIV_CATEGORY_SYSTEM, 0, "KILL TRANS"},
    {PRIV_CONNECTION_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW CONNECTIONS"},
    {PRIV_CONNECTION_KILL, PRIV_CATEGORY_SYSTEM, 0, "KILL CONNECTION"},
    {PRIV_QUERY_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW QUERIES"},
    {PRIV_QUERY_KILL, PRIV_CATEGORY_SYSTEM, 0, "KILL QUERY"},
    {PRIV_GRANTS_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW GRANTS"},
    {PRIV_CLUSTER_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW CLUSTER"},
    {PRIV_APPS_SHOW, PRIV_CATEGORY_SYSTEM, 0, "SHOW APPS"},

    // ==================== object privileges ====================
    // Database Privileges
    {PRIV_DB_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "ALTER DATABASE"},
    {PRIV_DB_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "DROP DATABASE"},
    {PRIV_DB_USE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "USE DATABASE"},
    {PRIV_DB_FLUSH, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "FLUSH DATABASE"},
    {PRIV_DB_COMPACT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "COMPACT DATABASE"},
    {PRIV_DB_TRIM, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "TRIM DATABASE"},
    {PRIV_DB_ROLLUP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "ROLLUP DATABASE"},
    {PRIV_DB_SCAN, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SCAN DATABASE"},
    {PRIV_DB_SSMIGRATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SSMIGRATE DATABASE"},
    {PRIV_VG_BALANCE_LEADER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "BALANCE VGROUP LEADER"},
    {PRIV_VG_MERGER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "MERGE VGROUP"},
    {PRIV_VG_REDISTRIBUTE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "REDISTRIBUTE VGROUP"},
    {PRIV_VG_SPLIT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SPLIT VGROUP"},
    {PRIV_SHOW_DATABASES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SHOW DATABASES"},
    {PRIV_SHOW_VNODES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SHOW VNODES"},
    {PRIV_SHOW_VGROUPS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SHOW VGROUPS"},
    {PRIV_SHOW_COMPACTS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SHOW COMPACTS"},
    {PRIV_SHOW_RETENTIONS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SHOW RETENTIONS"},
    {PRIV_SHOW_SCANS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SHOW SCANS"},
    {PRIV_SHOW_SSMIGRATES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, "SHOW SSMIGRATES"},

    // Table Privileges
    {PRIV_TBL_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "CREATE TABLE"},
    {PRIV_TBL_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "DROP TABLE"},
    {PRIV_TBL_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "ALTER TABLE"},
    {PRIV_TBL_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "SHOW TABLES"},
    {PRIV_TBL_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "SHOW CREATE TABLE"},
    {PRIV_TBL_SELECT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "READ TABLE"},
    {PRIV_TBL_INSERT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "WRITE TABLE"},
    {PRIV_TBL_DELETE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TABLE, "DELETE TABLE"},

    // Index Privileges
    {PRIV_IDX_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_INDEX, "CREATE INDEX"},
    {PRIV_IDX_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_INDEX, "DROP INDEX"},
    {PRIV_IDX_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_INDEX, "SHOW INDEXES"},

    // RSMA Privileges
    {PRIV_RSMA_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, "CREATE RSMA"},
    {PRIV_RSMA_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, "DROP RSMA"},
    {PRIV_RSMA_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, "ALTER RSMA"},
    {PRIV_RSMA_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, "SHOW RSMAS"},
    {PRIV_RSMA_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, "SHOW CREATE RSMA"},

    // TSMA Privileges
    {PRIV_TSMA_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, "CREATE TSMA"},
    {PRIV_TSMA_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, "DROP TSMA"},
    {PRIV_TSMA_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, "SHOW TSMAS"},

    // View Privileges
    {PRIV_VIEW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, "CREATE VIEW"},
    {PRIV_VIEW_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, "DROP VIEW"},
    {PRIV_VIEW_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, "SHOW VIEWS"},
    {PRIV_VIEW_READ, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, "READ VIEW"},

    // Topic Privileges
    {PRIV_TOPIC_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, "CREATE TOPIC"},
    {PRIV_TOPIC_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, "DROP TOPIC"},
    {PRIV_TOPIC_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, "SHOW TOPICS"},
    {PRIV_CONSUMER_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, "SHOW CONSUMERS"},
    {PRIV_SUBSCRIPTION_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, "SHOW SUBSCRIPTIONS"},

    // Stream Privileges
    {PRIV_STREAM_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, "CREATE STREAM"},
    {PRIV_STREAM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, "DROP STREAM"},
    {PRIV_STREAM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, "SHOW STREAMS"},
    {PRIV_STREAM_START, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, "START STREAM"},
    {PRIV_STREAM_STOP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, "STOP STREAM"},
    {PRIV_STREAM_RECALC, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, "RECALC STREAM"},

    // ==================== legacy privileges ====================
    {PRIV_TYPE_ALL, PRIV_CATEGORY_LEGACY, 0, "ALL PRIVILEGES"},
    {PRIV_TYPE_READ, PRIV_CATEGORY_LEGACY, 0, "READ PRIVILEGE"},
    {PRIV_TYPE_WRITE, PRIV_CATEGORY_LEGACY, 0, "WRITE PRIVILEGE"},
    {PRIV_TYPE_SUBSCRIBE, PRIV_CATEGORY_LEGACY, 0, "SUBSCRIBE PRIVILEGE"},
    {PRIV_TYPE_ALTER, PRIV_CATEGORY_LEGACY, 0, "ALTER PRIVILEGE"},
};

static SPrivInfo* privLookup[MAX_PRIV_TYPE + 1] = {0};

static void initPrivLookup(void) {
  for (size_t i = 0; i < sizeof(privInfoTable) / sizeof(privInfoTable[0]); ++i) {
    if (privInfoTable[i].privType <= MAX_PRIV_TYPE) {
      privLookup[privInfoTable[i].privType] = &privInfoTable[i];
    }
  }
}

int32_t checkPrivConflicts(const SPrivSet* privSet, EPrivCategory* pCategory, EPrivObjType* pObjType) {
  if (!privSet) goto _exit;

  (void)taosThreadOnce(&privInit, initPrivLookup);

  bool hasSystemPriv = false;
  bool hasObjectPriv = false;
  bool hasLegacyPriv = false;

  EPrivObjType objectType = PRIV_OBJ_UNKNOWN;

  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    uint64_t chunk = privSet->set[i];
    if (chunk == 0) continue;

    while (chunk != 0) {
      int32_t   bitPos = BUILDIN_CTZL(chunk);
      EPrivType privType = (i << 6) + bitPos;
      chunk &= ~(1ULL << bitPos);

      const SPrivInfo* privInfo = privLookup[privType];
      if (privInfo == NULL) continue;

      switch (privInfo->category) {
        case PRIV_CATEGORY_SYSTEM: {
          if (hasObjectPriv || hasLegacyPriv) {
            return 1;
          }
          hasSystemPriv = true;
          break;
        }
        case PRIV_CATEGORY_OBJECT: {
          if (hasSystemPriv || hasLegacyPriv) {
            return 1;
          }
          hasObjectPriv = true;
          if (objectType == PRIV_OBJ_UNKNOWN) {
            objectType = privInfo->objType;
          } else if (objectType != privInfo->objType) {
            return 2;
          }
          break;
        }
        case PRIV_CATEGORY_LEGACY: {
          if (hasSystemPriv || hasObjectPriv) {
            return 1;
          }
          hasLegacyPriv = true;
          if (objectType == PRIV_OBJ_UNKNOWN) {
            objectType = privInfo->objType;
          } else if (objectType != privInfo->objType) {
            return 2;
          }
          break;
        }
        default:
          break;
      }
    }
  }

_exit:
  if (pCategory) {
    *pCategory = hasSystemPriv   ? PRIV_CATEGORY_SYSTEM
                 : hasObjectPriv ? PRIV_CATEGORY_OBJECT
                 : hasLegacyPriv ? PRIV_CATEGORY_LEGACY
                                 : PRIV_CATEGORY_UNKNOWN;
  }
  if (pObjType) {
    *pObjType = objectType;
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

int32_t privObjKey(EPrivObjType objType, const char* db, const char* tb, char* buf, size_t bufLen) {
  return (objType == PRIV_OBJ_DB) ? snprintf(buf, bufLen, "%d.%s", objType, db ? db : "")
                                  : snprintf(buf, bufLen, "%d.%s.%s", objType, db ? db : "", tb ? tb : "");
}

int32_t privRowKey(ETableType tbType, const char* db, const char* tb, int64_t tsStart, int64_t tsEnd, char* buf,
                   size_t bufLen) {
  return snprintf(buf, bufLen, "%d.%s.%s.%" PRIi64 ".%" PRIi64, tbType, db, tb, tsStart, tsEnd);
}

int32_t privColKey(ETableType tbType, const char* db, const char* tb, const char* col, char* buf, size_t bufLen) {
  return snprintf(buf, bufLen, "%d.%s.%s.%s", tbType, db ? db : "", tb ? tb : "", col ? col : "");
}
